// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package docxparse

import (
	"strings"
	"unicode"
)

// preprocessCJKAdjacentMarkup disambiguates a narrow CommonMark pattern common in
// Chinese prose: emphasis that ends in punctuation and is immediately followed
// by a letter (for example **结论。**下一步). Goldmark correctly follows
// CommonMark's delimiter rules, while LarkOpenCLI accepts this authoring form.
// Rewriting simple CJK delimiter spans to equivalent DocxXML
// before parsing removes the ambiguity while leaving nested Markdown, links,
// code, fenced blocks, and source-bearing XML untouched.
func preprocessCJKAdjacentMarkup(markdown string) string {
	if !strings.Contains(markdown, "**") && !strings.Contains(markdown, "~~") {
		return markdown
	}
	lines := strings.SplitAfter(markdown, "\n")
	var out strings.Builder
	fenceMarker := rune(0)
	fenceLength := 0
	rawSourceTag := ""
	for _, line := range lines {
		trimmed := strings.TrimLeft(line, " \t>")
		if marker, length, ok := markdownFence(trimmed); ok {
			if fenceMarker == 0 {
				fenceMarker, fenceLength = marker, length
			} else if marker == fenceMarker && length >= fenceLength && strings.TrimSpace(runeTail(trimmed, length)) == "" {
				fenceMarker, fenceLength = 0, 0
			}
			out.WriteString(line)
			continue
		}
		if fenceMarker != 0 || leadingIndent(line) >= 4 {
			out.WriteString(line)
			continue
		}
		out.WriteString(rewriteCJKMarkupLine(line, &rawSourceTag))
	}
	return out.String()
}

func markdownFence(line string) (rune, int, bool) {
	runes := []rune(line)
	if len(runes) < 3 || runes[0] != '`' && runes[0] != '~' {
		return 0, 0, false
	}
	marker := runes[0]
	length := 0
	for length < len(runes) && runes[length] == marker {
		length++
	}
	return marker, length, length >= 3
}

func runeTail(value string, start int) string {
	runes := []rune(value)
	if start >= len(runes) {
		return ""
	}
	return string(runes[start:])
}

func leadingIndent(line string) int {
	count := 0
	for _, r := range line {
		switch r {
		case ' ':
			count++
		case '\t':
			count += 4
		default:
			return count
		}
	}
	return count
}

type cjkMarkupRule struct {
	delimiter []rune
	openXML   string
	closeXML  string
}

var cjkMarkupRules = []cjkMarkupRule{
	{delimiter: []rune("***"), openXML: "<em><b>", closeXML: "</b></em>"},
	{delimiter: []rune("~~"), openXML: "<del>", closeXML: "</del>"},
	{delimiter: []rune("**"), openXML: "<b>", closeXML: "</b>"},
}

func rewriteCJKMarkupLine(line string, rawSourceTag *string) string {
	if *rawSourceTag != "" {
		runes := []rune(line)
		closeTag := []rune("</" + *rawSourceTag + ">")
		closeAt := indexRunesFold(runes, 0, closeTag)
		if closeAt < 0 {
			return line
		}
		closeEnd := closeAt + len(closeTag)
		prefix := string(runes[:closeEnd])
		*rawSourceTag = ""
		return prefix + rewriteCJKMarkupLine(string(runes[closeEnd:]), rawSourceTag)
	}

	runes := []rune(line)
	var out strings.Builder
	for i := 0; i < len(runes); {
		if runes[i] == '`' && !runeEscaped(runes, i) {
			if end := codeSpanEnd(runes, i); end > i {
				out.WriteString(string(runes[i:end]))
				i = end
				continue
			}
		}
		if runes[i] == '<' {
			if tag, end, selfClosing, ok := rawTagAt(runes, i); ok {
				out.WriteString(string(runes[i:end]))
				i = end
				if !selfClosing && (tag == "code" || tag == "pre" || tag == "whiteboard") {
					close := []rune("</" + tag + ">")
					if closeAt := indexRunesFold(runes, i, close); closeAt >= 0 {
						closeEnd := closeAt + len(close)
						out.WriteString(string(runes[i:closeEnd]))
						i = closeEnd
					} else {
						out.WriteString(string(runes[i:]))
						*rawSourceTag = tag
						return out.String()
					}
				}
				continue
			}
		}

		rewritten := false
		for _, rule := range cjkMarkupRules {
			if !exactDelimiterAt(runes, i, rule.delimiter) || runeEscaped(runes, i) {
				continue
			}
			closeAt := delimiterCloser(runes, i+len(rule.delimiter), rule.delimiter)
			if closeAt < 0 {
				continue
			}
			content := runes[i+len(rule.delimiter) : closeAt]
			if !shouldRewriteCJKMarkup(content) {
				continue
			}
			out.WriteString(rule.openXML)
			out.WriteString(escapeXMLText(stripBackslashEscapes(string(content))))
			out.WriteString(rule.closeXML)
			i = closeAt + len(rule.delimiter)
			rewritten = true
			break
		}
		if rewritten {
			continue
		}
		out.WriteRune(runes[i])
		i++
	}
	return out.String()
}

func rawTagAt(runes []rune, start int) (tag string, end int, selfClosing, ok bool) {
	if start+1 >= len(runes) || !isASCIILetterRune(runes[start+1]) {
		return "", 0, false, false
	}
	i := start + 1
	for i < len(runes) && (isASCIILetterRune(runes[i]) || isASCIIDigitRune(runes[i]) || runes[i] == '-' || runes[i] == '_') {
		i++
	}
	tag = strings.ToLower(string(runes[start+1 : i]))
	quote := rune(0)
	for ; i < len(runes); i++ {
		if runes[i] == '\'' || runes[i] == '"' {
			if quote == 0 {
				quote = runes[i]
			} else if quote == runes[i] {
				quote = 0
			}
			continue
		}
		if runes[i] == '>' && quote == 0 {
			trimmed := strings.TrimSpace(string(runes[start : i+1]))
			return tag, i + 1, strings.HasSuffix(trimmed, "/>"), true
		}
	}
	return "", 0, false, false
}

func indexRunesFold(haystack []rune, start int, needle []rune) int {
	for i := start; i+len(needle) <= len(haystack); i++ {
		if strings.EqualFold(string(haystack[i:i+len(needle)]), string(needle)) {
			return i
		}
	}
	return -1
}

func codeSpanEnd(runes []rune, open int) int {
	length := 0
	for open+length < len(runes) && runes[open+length] == '`' {
		length++
	}
	for i := open + length; i < len(runes); i++ {
		if runes[i] != '`' || runeEscaped(runes, i) {
			continue
		}
		end := i
		for end < len(runes) && runes[end] == '`' {
			end++
		}
		if end-i == length {
			return end
		}
		i = end - 1
	}
	return -1
}

func exactDelimiterAt(runes []rune, start int, delimiter []rune) bool {
	if start+len(delimiter) > len(runes) {
		return false
	}
	for i, want := range delimiter {
		if runes[start+i] != want {
			return false
		}
	}
	marker := delimiter[0]
	return (start == 0 || runes[start-1] != marker) && (start+len(delimiter) == len(runes) || runes[start+len(delimiter)] != marker)
}

func delimiterCloser(runes []rune, start int, delimiter []rune) int {
	for i := start; i+len(delimiter) <= len(runes); i++ {
		if runes[i] == '\n' {
			return -1
		}
		if exactDelimiterAt(runes, i, delimiter) && !runeEscaped(runes, i) {
			return i
		}
	}
	return -1
}

func shouldRewriteCJKMarkup(content []rune) bool {
	if len(content) == 0 || unicode.IsSpace(content[0]) || unicode.IsSpace(content[len(content)-1]) {
		return false
	}
	for _, r := range content {
		if r == '`' || r == '[' || r == ']' || r == '<' || r == '>' {
			return false
		}
	}
	if !containsCJK(content) {
		return false
	}
	return true
}

func containsCJK(value []rune) bool {
	for _, r := range value {
		if isCJKRune(r) || r > unicode.MaxASCII && (unicode.IsPunct(r) || unicode.IsSymbol(r)) {
			return true
		}
	}
	return false
}

func isCJKRune(r rune) bool {
	return unicode.In(r, unicode.Han, unicode.Hiragana, unicode.Katakana, unicode.Hangul)
}

func runeEscaped(runes []rune, index int) bool {
	count := 0
	for i := index - 1; i >= 0 && runes[i] == '\\'; i-- {
		count++
	}
	return count%2 == 1
}
