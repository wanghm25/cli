// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package docxparse

import (
	"strings"
	"testing"
)

func TestParseXMLBuildsBlockDistribution(t *testing.T) {
	result, err := Parse(`<title>T</title><p>P</p><ul><li>A</li><li>B</li></ul>`, FormatXML)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if result.XML != `<title>T</title><p>P</p><ul><li>A</li><li>B</li></ul>` {
		t.Fatalf("XML = %q", result.XML)
	}
	if result.Profile.BlockCount != 5 {
		t.Fatalf("block total = %d, want 5", result.Profile.BlockCount)
	}
	shares := map[string]BlockShare{}
	for _, share := range result.Profile.Blocks {
		shares[share.Type] = share
	}
	if got := shares["li"]; got.Count != 2 || got.Ratio != 0.4 {
		t.Fatalf("li share = %+v, want count=2 ratio=0.4", got)
	}
	for _, typ := range []string{"title", "p", "ul"} {
		if got := shares[typ]; got.Count != 1 || got.Ratio != 0.2 {
			t.Errorf("%s share = %+v, want count=1 ratio=0.2", typ, got)
		}
	}
}

func TestParseXMLRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name   string
		source string
	}{
		{name: "unsupported tag", source: `<unknown>x</unknown>`},
		{name: "missing closing tag", source: `<p>one`},
		{name: "invalid nesting", source: `<span>x<table><tr><td>y</td></tr></table></span>`},
		{name: "malformed block id", source: `<block_id="8,9"/>`},
		{name: "unterminated cdata", source: `<code><![CDATA[a < b</code>`},
		{name: "tag spacing", source: `< p>text< / p>`},
		{name: "self closing slash spacing", source: `<p/ >`},
		{name: "unquoted attribute", source: `<p align=center>text</p>`},
		{name: "invalid entity", source: `<p>one &unknown;</p>`},
		{name: "invalid attribute entity", source: `<img href="https://example.com/&unknown;"/>`},
		{name: "missing required ancestor", source: `<td>cell</td>`},
		{name: "missing required attribute", source: `<img/>`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Parse(tt.source, FormatXML); err == nil {
				t.Fatalf("Parse(%q) succeeded, want validation error", tt.source)
			}
		})
	}
}

func TestParseAutoDetectsXMLAndMarkdown(t *testing.T) {
	tests := []struct {
		name   string
		source string
		blocks int
	}{
		{name: "xml", source: `<title>T</title><p>P</p>`, blocks: 2},
		{name: "markdown", source: "# T\n\nP", blocks: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			profile, err := ParseAuto(tt.source)
			if err != nil {
				t.Fatalf("ParseAuto() error = %v", err)
			}
			if profile.BlockCount != tt.blocks {
				t.Fatalf("profile = %+v, want %d blocks", profile, tt.blocks)
			}
		})
	}
}

func TestParseAutoDoesNotTreatMalformedXMLAsMarkdown(t *testing.T) {
	if _, err := ParseAuto(`<p>text`); err == nil {
		t.Fatal("ParseAuto() succeeded, want malformed XML error")
	}
}

func TestParseXMLAcceptsPublicTagAliasesWithoutChangingInput(t *testing.T) {
	source := `<P>one<strong>two</strong><br></P><image href="https://example.com/image.png">`
	result, err := Parse(source, FormatXML)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if result.XML != source {
		t.Fatalf("XML = %q, want original %q", result.XML, source)
	}
	if result.Profile.BlockCount != 2 {
		t.Fatalf("profile = %+v, want p and img blocks", result.Profile)
	}
}

func TestParseXMLAcceptsPublicAttributeAliasesWithoutChangingInput(t *testing.T) {
	source := `<callout color="blue" icon="💡"><p>x</p></callout><at id="ou_legacy"></at><img url="https://example.com/image.png"/>`
	result, err := Parse(source, FormatXML)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if result.XML != source {
		t.Fatalf("XML = %q, want original %q", result.XML, source)
	}
	if result.Profile.BlockCount != 3 {
		t.Fatalf("profile = %+v, want callout, p, and img blocks", result.Profile)
	}
}

func TestParseXMLAcceptsBareAmpersandsInAttributes(t *testing.T) {
	source := `<block_insert><parameter><block_id>-1</block_id><content><img href="https://picsum.photos/320/200?seed=lark-cli&raw=1"/></content></parameter></block_insert>`
	result, err := Parse(source, FormatXML)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if result.XML != source {
		t.Fatalf("XML = %q, want original %q", result.XML, source)
	}
}

func TestNormalizeXMLAttributeAmpersandsPreservesEntityReferences(t *testing.T) {
	source := `https://example.com?a=1&b=2&amp;c=3&#38;d=4&#x26;e=5&unknown;`
	want := `https://example.com?a=1&amp;b=2&amp;c=3&#38;d=4&#x26;e=5&unknown;`
	if got := normalizeXMLAttributeAmpersands(source); got != want {
		t.Fatalf("normalizeXMLAttributeAmpersands() = %q, want %q", got, want)
	}
}

func TestParseXMLPreservesValidCDATA(t *testing.T) {
	source := `<code><![CDATA[a < b && c > d]]></code>`
	result, err := Parse(source, FormatXML)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if result.XML != source {
		t.Fatalf("XML = %q, want original %q", result.XML, source)
	}
}

func TestParseXMLPreservesUTF8BOM(t *testing.T) {
	source := "\uFEFF<p>text</p>"
	result, err := Parse(source, FormatXML)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if result.XML != source {
		t.Fatalf("XML = %q, want original input", result.XML)
	}
}

func TestParseMarkdownConvertsLarkOpenCLIBlocks(t *testing.T) {
	source := "# 标题\n\nHello **world**.\n\n- [x] Done\n- [ ] Todo\n\n" +
		"| A | B |\n| --- | --- |\n| 1 | 2 |\n\n" +
		"```go\nfmt.Println(\"x\")\n```\n\n$E=mc^2$\n"
	result, err := Parse(source, FormatMarkdown)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	for _, fragment := range []string{
		`<h1>标题</h1>`,
		`<p>Hello <b>world</b>.</p>`,
		`<checkbox done="true">Done</checkbox>`,
		`<checkbox done="false">Todo</checkbox>`,
		`<table><thead><tr><th>A</th><th>B</th></tr></thead><tbody><tr><td>1</td><td>2</td></tr></tbody></table>`,
		`<pre lang="go"><code>fmt.Println("x")</code></pre>`,
		`<p><latex>E=mc^2</latex></p>`,
	} {
		if !strings.Contains(result.XML, fragment) {
			t.Errorf("XML missing %q:\n%s", fragment, result.XML)
		}
	}
}

func TestParseMarkdownPreservesLineBreakSemantics(t *testing.T) {
	tests := []struct {
		name   string
		source string
		want   string
	}{
		{
			name:   "soft breaks become spaces",
			source: "**文号：桂汛旱指〔2026〕17号**\n**签发人：XXX**\n**发布日期：2026年7月13日**",
			want:   `<p><b>文号：桂汛旱指〔2026〕17号</b> <b>签发人：XXX</b> <b>发布日期：2026年7月13日</b></p>`,
		},
		{
			name:   "hard breaks remain line breaks",
			source: "**文号：A**  \n**签发人：B**",
			want:   `<p><b>文号：A</b><br/><b>签发人：B</b></p>`,
		},
		{
			name:   "blank lines remain paragraph breaks",
			source: "**文号：A**\n\n**签发人：B**",
			want:   `<p><b>文号：A</b></p><p><b>签发人：B</b></p>`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Parse(tt.source, FormatMarkdown)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if result.XML != tt.want {
				t.Fatalf("XML = %q, want %q", result.XML, tt.want)
			}
		})
	}
}

func TestParseMarkdownContainerKeepsMarkdownChildren(t *testing.T) {
	source := "<callout emoji=\"💡\">\n\n## Note\n\n- item\n\n</callout>\n"
	result, err := Parse(source, FormatMarkdown)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	want := `<callout emoji="💡"><h2>Note</h2><ul><li>item</li></ul></callout>`
	if result.XML != want {
		t.Fatalf("XML = %q, want %q", result.XML, want)
	}
}

func TestParseMarkdownMatchesLarkOpenCLIFixtures(t *testing.T) {
	t.Run("deep nested list", func(t *testing.T) {
		result, err := Parse("1. 第一层\n   - 第二层\n     - 第三层\n       - 第四层\n", FormatMarkdown)
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}
		if strings.Contains(result.XML, "<pre>") || strings.Contains(result.XML, "<code>") || !strings.Contains(result.XML, "第四层") {
			t.Fatalf("nested list converted incorrectly: %s", result.XML)
		}
	})

	t.Run("fenced mermaid", func(t *testing.T) {
		result, err := Parse("```mermaid\nflowchart LR\nA-->B\n```", FormatMarkdown)
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}
		want := `<whiteboard type="mermaid">flowchart LR<br/>A--&gt;B</whiteboard>`
		if result.XML != want {
			t.Fatalf("XML = %q, want %q", result.XML, want)
		}
	})

	t.Run("raw whiteboard source", func(t *testing.T) {
		source := "<whiteboard type=\"mermaid\">\nflowchart LR\n  A --> B\n</whiteboard>"
		result, err := Parse(source, FormatMarkdown)
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}
		want := `<whiteboard type="mermaid">flowchart LR<br/>  A --&gt; B</whiteboard>`
		if result.XML != want {
			t.Fatalf("XML = %q, want %q", result.XML, want)
		}
	})

	t.Run("raw code stays literal", func(t *testing.T) {
		source := "<code lang=\"go\">\nif a < b && c > d {\n  fmt.Println(\"**raw**\")\n}\n</code>"
		result, err := Parse(source, FormatMarkdown)
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}
		want := `<code lang="go">if a &lt; b &amp;&amp; c &gt; d {<br/>  fmt.Println("**raw**")<br/>}</code>`
		if result.XML != want {
			t.Fatalf("XML = %q, want %q", result.XML, want)
		}
	})

	t.Run("underscore tags", func(t *testing.T) {
		result, err := Parse(`text <synced_reference src-block-id="abc" src-token="def"/> more`, FormatMarkdown)
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}
		if !strings.Contains(result.XML, `<synced_reference`) || strings.Contains(result.XML, `&lt;synced_reference`) {
			t.Fatalf("underscore tag was not preserved: %s", result.XML)
		}
	})

	t.Run("canonical user cite", func(t *testing.T) {
		result, err := Parse(`hello <cite type="user" user-id="ou_user"></cite>`, FormatMarkdown)
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}
		for _, want := range []string{`<cite`, `type="user"`, `user-id="ou_user"`} {
			if !strings.Contains(result.XML, want) {
				t.Errorf("XML missing %q: %s", want, result.XML)
			}
		}
	})

	t.Run("public tag alias converts to canonical XML", func(t *testing.T) {
		result, err := Parse(`hello <strong>world</strong>`, FormatMarkdown)
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}
		if result.XML != `<p>hello <b>world</b></p>` {
			t.Fatalf("XML = %q", result.XML)
		}
	})

	t.Run("public cite alias converts attributes", func(t *testing.T) {
		result, err := Parse(`hello <at id="ou_legacy"></at>`, FormatMarkdown)
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}
		if result.XML != `<p>hello <cite type="user" user-id="ou_legacy"></cite></p>` {
			t.Fatalf("XML = %q", result.XML)
		}
	})

	t.Run("markdown backslash escapes", func(t *testing.T) {
		result, err := Parse(`"source\_token": \[abc\] path\\to`, FormatMarkdown)
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}
		for _, want := range []string{`source_token`, `[abc]`, `path\to`} {
			if !strings.Contains(result.XML, want) {
				t.Errorf("XML missing %q: %s", want, result.XML)
			}
		}
	})

	t.Run("adjacent CJK emphasis", func(t *testing.T) {
		source := `***你好。***S 和 ~~再见。~~T。**agent team 做 brownfield 项目，带来的感知会强烈得多**——前提。**这个时刻，才是真正属于 agent team 的"闪光时刻"。**翟霖`
		result, err := Parse(source, FormatMarkdown)
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}
		for _, want := range []string{
			`<em><b>你好。</b></em>S`,
			`<del>再见。</del>T`,
			`<b>agent team 做 brownfield 项目，带来的感知会强烈得多</b>`,
			`<b>这个时刻，才是真正属于 agent team 的"闪光时刻"。</b>翟霖`,
		} {
			if !strings.Contains(result.XML, want) {
				t.Errorf("XML missing %q: %s", want, result.XML)
			}
		}
	})

	t.Run("div parses markdown children", func(t *testing.T) {
		result, err := Parse("<div>\n\n**bold**\n\n</div>", FormatMarkdown)
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}
		if result.XML != `<div><p><b>bold</b></p></div>` {
			t.Fatalf("XML = %q", result.XML)
		}
	})
}

func TestPreprocessCJKAdjacentMarkupUsesRuneOffsetsAfterRawBlock(t *testing.T) {
	tests := []struct {
		name       string
		lineEnding string
		final      string
	}{
		{name: "EOF", lineEnding: "\n"},
		{name: "LF", lineEnding: "\n", final: "\n"},
		{name: "CRLF", lineEnding: "\r\n", final: "\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := "<code>**raw**" + tt.lineEnding + "Ⱥ</code>**你好。**S" + tt.final
			want := "<code>**raw**" + tt.lineEnding + "Ⱥ</code><b>你好。</b>S" + tt.final
			if got := preprocessCJKAdjacentMarkup(source); got != want {
				t.Fatalf("preprocessCJKAdjacentMarkup() = %q, want %q", got, want)
			}
		})
	}
}

func TestTextProfileMatchesLarkOpenCLIContract(t *testing.T) {
	result, err := Parse(`<title>标题</title><p>一个苹果是 an apple。</p>`, FormatXML)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	profile := result.Profile
	if profile.WordCount != 10 || profile.CharCount != 15 {
		t.Fatalf("profile = %+v, want word_count=10 char_count=15", profile)
	}
	if profile.Breakdown.HanChars != 7 || profile.Breakdown.EnglishWords != 2 || profile.Breakdown.ChinesePunctuations != 1 {
		t.Fatalf("breakdown = %+v", profile.Breakdown)
	}
}

func TestTextProfileMatchesAuthoringCounterCases(t *testing.T) {
	tests := []struct {
		name      string
		source    string
		words     int
		chars     int
		blocks    int
		english   int
		numbers   int
		han       int
		listItems int
	}{
		{
			name:   "english number and punctuation",
			source: `<p>Hello world 123.45。</p>`,
			words:  4, chars: 17, blocks: 1, english: 2, numbers: 1,
		},
		{
			name:   "list and checkbox markers",
			source: `<ul><li>甲</li><li>two</li></ul><checkbox done="true">完成</checkbox>`,
			words:  7, chars: 9, blocks: 4, english: 1, han: 3, listItems: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Parse(tt.source, FormatXML)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			profile := result.Profile
			if profile.WordCount != tt.words || profile.CharCount != tt.chars || profile.BlockCount != tt.blocks {
				t.Fatalf("profile = %+v, want words=%d chars=%d blocks=%d", profile, tt.words, tt.chars, tt.blocks)
			}
			if profile.Breakdown.EnglishWords != tt.english || profile.Breakdown.NumberWords != tt.numbers || profile.Breakdown.HanChars != tt.han {
				t.Fatalf("breakdown = %+v", profile.Breakdown)
			}
			if got := blockCountForTest(profile.Blocks, "li"); got != tt.listItems {
				t.Fatalf("li count = %d, want %d", got, tt.listItems)
			}
		})
	}
}

func TestTextProfileUsesVisibleAttributeFallbacks(t *testing.T) {
	result, err := Parse(`<p text="Hello"/><p><span title="world"/></p><img href="https://example.com/image.png" caption="图"/>`, FormatXML)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	profile := result.Profile
	if profile.WordCount != 3 || profile.CharCount != 11 {
		t.Fatalf("profile = %+v, want word_count=3 char_count=11", profile)
	}
	if profile.Breakdown.EnglishWords != 2 || profile.Breakdown.HanChars != 1 {
		t.Fatalf("breakdown = %+v", profile.Breakdown)
	}
}

func TestParseRejectsUnsafeXMLDeclarations(t *testing.T) {
	_, err := Parse(`<!DOCTYPE foo [<!ENTITY x "value">]><p>&x;</p>`, FormatXML)
	if err == nil || !strings.Contains(err.Error(), "DOCTYPE or ENTITY") {
		t.Fatalf("Parse() error = %v, want unsafe declaration rejection", err)
	}
}

func TestParseRejectsInvalidUTF8(t *testing.T) {
	_, err := Parse(string([]byte{'<', 'p', '>', 0xff, '<', '/', 'p', '>'}), FormatXML)
	if err == nil || !strings.Contains(err.Error(), "valid UTF-8") {
		t.Fatalf("Parse() error = %v, want UTF-8 rejection", err)
	}
}

func TestParseRejectsExcessiveNesting(t *testing.T) {
	source := strings.Repeat("<span>", MaxNestingDepth+1)
	_, err := Parse(source, FormatXML)
	if err == nil || !strings.Contains(err.Error(), "nesting exceeds") {
		t.Fatalf("Parse() error = %v, want nesting limit rejection", err)
	}
}

func TestParseXMLRejectsNestedInvalidTagStarts(t *testing.T) {
	if _, err := Parse(`<<<<p>text</p>`, FormatXML); err == nil {
		t.Fatal("Parse() succeeded, want invalid XML token error")
	}
}

func blockCountForTest(blocks []BlockShare, typ string) int {
	for _, block := range blocks {
		if block.Type == typ {
			return block.Count
		}
	}
	return 0
}
