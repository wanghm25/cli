// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package docxparse

// Markdown conversion is scoped to the docs +script business domain.

import (
	"fmt"
	"strings"

	"github.com/yuin/goldmark"
	gast "github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	extast "github.com/yuin/goldmark/extension/ast"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/text"
	gmutil "github.com/yuin/goldmark/util"
)

var markdownParser parser.Parser

func init() {
	markdown := goldmark.New(
		goldmark.WithExtensions(
			extension.GFM,
			extension.DefinitionList,
			&mathExtension{},
			&underscoreHTMLExtension{},
		),
		goldmark.WithParserOptions(
			parser.WithBlockParsers(gmutil.Prioritized(&containerBlockParser{}, 90)),
		),
	)
	markdownParser = markdown.Parser()
}

func parseMarkdown(source string) ([]*Node, error) {
	if err := validateSource(source); err != nil {
		return nil, err
	}
	source = strings.TrimPrefix(source, "\uFEFF")
	source = normalizeListIndent(source)
	source = preprocessCJKAdjacentMarkup(source)
	data := []byte(source)
	document := markdownParser.Parse(text.NewReader(data))
	return renderBlockChildren(document, data)
}

func renderBlockChildren(parent gast.Node, source []byte) ([]*Node, error) {
	var out []*Node
	for child := parent.FirstChild(); child != nil; child = child.NextSibling() {
		nodes, err := renderBlockNode(child, source)
		if err != nil {
			return nil, err
		}
		out = append(out, nodes...)
	}
	return out, nil
}

func renderBlockNode(node gast.Node, source []byte) ([]*Node, error) {
	switch node.Kind() {
	case gast.KindParagraph, gast.KindTextBlock:
		children, err := renderInlineChildren(node, source)
		if err != nil {
			return nil, err
		}
		return wrapParagraphChildren(children), nil
	case gast.KindHeading:
		heading := newElement(headingTag(node.(*gast.Heading).Level), nil)
		children, err := renderInlineChildren(node, source)
		if err != nil {
			return nil, err
		}
		for _, child := range children {
			heading.addChild(child)
		}
		return []*Node{heading}, nil
	case gast.KindBlockquote:
		return renderContainer("blockquote", nil, node, source)
	case gast.KindList:
		return renderList(node.(*gast.List), source)
	case gast.KindFencedCodeBlock:
		block := node.(*gast.FencedCodeBlock)
		language := string(block.Language(source))
		content := trimOneTrailingNewline(string(node.Lines().Value(source)))
		lowerLanguage := strings.ToLower(language)
		if content != "" && (lowerLanguage == "mermaid" || lowerLanguage == "plantuml" || lowerLanguage == "svg") {
			whiteboard := newElement("whiteboard", map[string]string{"type": lowerLanguage})
			appendRawTextWithBreaks(whiteboard, content)
			return []*Node{whiteboard}, nil
		}
		attrs := map[string]string(nil)
		if language != "" {
			attrs = map[string]string{"lang": language}
		}
		pre := newElement("pre", attrs)
		code := newElement("code", nil)
		appendRawTextWithBreaks(code, content)
		pre.addChild(code)
		return []*Node{pre}, nil
	case gast.KindCodeBlock:
		pre := newElement("pre", nil)
		code := newElement("code", nil)
		appendRawTextWithBreaks(code, trimOneTrailingNewline(string(node.Lines().Value(source))))
		pre.addChild(code)
		return []*Node{pre}, nil
	case gast.KindThematicBreak:
		return []*Node{newElement("hr", nil)}, nil
	case gast.KindHTMLBlock:
		nodes, err := parseMarkdownHTMLBlock(string(node.Lines().Value(source)))
		if err != nil {
			return nil, err
		}
		stripMarkdownEscapesInNodes(nodes, false, false)
		return nodes, nil
	case kindContainerBlock:
		container := node.(*containerBlock)
		return renderContainer(container.spec.tag, container.attrs, node, source)
	}

	switch node.Kind() {
	case extast.KindTable:
		return renderTable(node, source)
	case extast.KindDefinitionList:
		return renderDefinitionList(node, source)
	}

	value := strings.TrimSpace(extractMarkdownText(node, source))
	if value == "" {
		return nil, nil
	}
	paragraph := newElement("p", nil)
	paragraph.addChild(newText(value))
	return []*Node{paragraph}, nil
}

// parseMarkdownHTMLBlock handles the source-bearing LarkOpenCLI blocks whose
// Markdown bodies are literal text, then delegates every other XML fragment to
// the strict XML parser. Escaping literal code is part of Markdown conversion.
func parseMarkdownHTMLBlock(fragment string) ([]*Node, error) {
	trimmed := strings.TrimSpace(fragment)
	for _, tag := range []string{"code", "whiteboard"} {
		closing := "</" + tag + ">"
		if !strings.HasPrefix(trimmed, "<"+tag) || !strings.HasSuffix(trimmed, closing) {
			continue
		}
		token, contentStart, state := scanXMLToken(trimmed, 0)
		if state != tokenOK || token.closing || token.selfClosing || token.name != tag {
			return nil, fmt.Errorf("invalid Markdown <%s> block", tag)
		}
		contentEnd := len(trimmed) - len(closing)
		if contentStart > contentEnd {
			return nil, fmt.Errorf("invalid Markdown <%s> block", tag)
		}
		attrs := normalizeAttributes(tag, tag, token.attrs)
		block := newElement(tag, attrs)
		appendRawTextWithBreaks(block, strings.Trim(trimmed[contentStart:contentEnd], "\r\n"))
		return []*Node{block}, nil
	}
	return parseXML(fragment)
}

func renderContainer(tag string, attrs map[string]string, node gast.Node, source []byte) ([]*Node, error) {
	attrs = normalizeAttributes(tag, tag, attrs)
	container := newElement(tag, attrs)
	children, err := renderBlockChildren(node, source)
	if err != nil {
		return nil, err
	}
	for _, child := range children {
		container.addChild(child)
	}
	return []*Node{container}, nil
}

func renderList(list *gast.List, source []byte) ([]*Node, error) {
	if isTaskList(list) {
		return renderTaskList(list, source)
	}
	tag := "ul"
	if list.IsOrdered() {
		tag = "ol"
	}
	listNode := newElement(tag, nil)
	for child := list.FirstChild(); child != nil; child = child.NextSibling() {
		if child.Kind() != gast.KindListItem {
			continue
		}
		item, err := renderListItem(child.(*gast.ListItem), list.IsTight, source)
		if err != nil {
			return nil, err
		}
		listNode.addChild(item)
	}
	return []*Node{listNode}, nil
}

func isTaskList(list *gast.List) bool {
	first := list.FirstChild()
	if first == nil || first.Kind() != gast.KindListItem {
		return false
	}
	return findTaskCheckbox(first.(*gast.ListItem)) != nil
}

func findTaskCheckbox(item *gast.ListItem) *extast.TaskCheckBox {
	for child := item.FirstChild(); child != nil; child = child.NextSibling() {
		if child.Kind() != gast.KindTextBlock && child.Kind() != gast.KindParagraph {
			continue
		}
		if first := child.FirstChild(); first != nil && first.Kind() == extast.KindTaskCheckBox {
			return first.(*extast.TaskCheckBox)
		}
	}
	return nil
}

func renderTaskList(list *gast.List, source []byte) ([]*Node, error) {
	var out []*Node
	for child := list.FirstChild(); child != nil; child = child.NextSibling() {
		if child.Kind() != gast.KindListItem {
			continue
		}
		item := child.(*gast.ListItem)
		checkboxAST := findTaskCheckbox(item)
		if checkboxAST == nil {
			li, err := renderListItem(item, list.IsTight, source)
			if err != nil {
				return nil, err
			}
			ul := newElement("ul", nil)
			ul.addChild(li)
			out = append(out, ul)
			continue
		}
		done := "false"
		if checkboxAST.IsChecked {
			done = "true"
		}
		checkbox := newElement("checkbox", map[string]string{"done": done})
		for block := item.FirstChild(); block != nil; block = block.NextSibling() {
			if block.Kind() == gast.KindTextBlock || block.Kind() == gast.KindParagraph {
				fragment, err := renderInlineFragment(block, source, true)
				if err != nil {
					return nil, err
				}
				nodes, err := parseMarkdownInlineFragment(fragment)
				if err != nil {
					return nil, err
				}
				for _, node := range nodes {
					checkbox.addChild(node)
				}
				continue
			}
			nodes, err := renderBlockNode(block, source)
			if err != nil {
				return nil, err
			}
			for _, node := range nodes {
				checkbox.addChild(node)
			}
		}
		out = append(out, checkbox)
	}
	return out, nil
}

func renderListItem(item *gast.ListItem, tight bool, source []byte) (*Node, error) {
	li := newElement("li", nil)
	children, err := renderBlockChildren(item, source)
	if err != nil {
		return nil, err
	}
	for _, child := range children {
		if child.tag == "p" && (tight || paragraphOnlyInline(child)) {
			for _, grandchild := range child.children {
				li.addChild(grandchild)
			}
			continue
		}
		li.addChild(child)
	}
	return li, nil
}

func renderInlineChildren(node gast.Node, source []byte) ([]*Node, error) {
	fragment, err := renderInlineFragment(node, source, false)
	if err != nil {
		return nil, err
	}
	nodes, err := parseMarkdownInlineFragment(fragment)
	if err != nil {
		return nil, err
	}
	stripMarkdownEscapesInNodes(nodes, false, false)
	return nodes, nil
}

// parseMarkdownInlineFragment wraps an inline fragment in a space-preserving
// parent while parsing so XML normalization keeps semantic spaces between
// adjacent inline elements. The wrapper is removed from the returned nodes.
func parseMarkdownInlineFragment(fragment string) ([]*Node, error) {
	nodes, err := parseXML("<p>" + fragment + "</p>")
	if err != nil {
		return nil, err
	}
	if len(nodes) != 1 || nodes[0].typ != nodeElement || nodes[0].tag != "p" {
		return nil, fmt.Errorf("invalid Markdown inline fragment")
	}
	children := nodes[0].children
	for _, child := range children {
		child.parent = nil
	}
	return children, nil
}

func renderInlineFragment(parent gast.Node, source []byte, skipCheckbox bool) (string, error) {
	var out strings.Builder
	for child := parent.FirstChild(); child != nil; child = child.NextSibling() {
		if skipCheckbox && child.Kind() == extast.KindTaskCheckBox {
			continue
		}
		fragment, err := renderInlineNode(child, source)
		if err != nil {
			return "", err
		}
		out.WriteString(fragment)
	}
	return out.String(), nil
}

func renderInlineNode(node gast.Node, source []byte) (string, error) {
	switch node.Kind() {
	case gast.KindText:
		textNode := node.(*gast.Text)
		value := escapeXMLText(stripBackslashEscapes(string(textNode.Value(source))))
		switch {
		case textNode.HardLineBreak():
			value += "<br/>"
		case textNode.SoftLineBreak():
			value += " "
		}
		return value, nil
	case gast.KindString:
		return escapeXMLText(string(node.(*gast.String).Value)), nil
	case gast.KindEmphasis:
		tag := "em"
		if node.(*gast.Emphasis).Level >= 2 {
			tag = "b"
		}
		return renderInlineContainer(node, tag, nil, source)
	case gast.KindCodeSpan:
		return elementXML("code", nil, escapeXMLText(collectMarkdownChildText(node, source))), nil
	case gast.KindLink:
		link := node.(*gast.Link)
		attrs := map[string]string{"href": string(link.Destination)}
		if len(link.Title) > 0 {
			attrs["title"] = string(link.Title)
		}
		children, err := renderInlineFragment(node, source, false)
		if err != nil {
			return "", err
		}
		if children == "" {
			children = escapeXMLText(string(link.Destination))
		}
		return elementXML("a", attrs, children), nil
	case gast.KindImage:
		image := node.(*gast.Image)
		destination := string(image.Destination)
		attrs := map[string]string{}
		if strings.HasPrefix(destination, "http://") || strings.HasPrefix(destination, "https://") {
			attrs["href"] = destination
		} else {
			attrs["src"] = destination
		}
		if len(image.Title) > 0 {
			attrs["title"] = string(image.Title)
		}
		return elementXML("img", attrs, ""), nil
	case gast.KindRawHTML:
		return string(node.(*gast.RawHTML).Segments.Value(source)), nil
	case gast.KindAutoLink:
		link := node.(*gast.AutoLink)
		return elementXML("a", map[string]string{"href": string(link.URL(source))}, escapeXMLText(string(link.Label(source)))), nil
	}

	switch node.Kind() {
	case extast.KindStrikethrough:
		return renderInlineContainer(node, "del", nil, source)
	case kindMathInline:
		return elementXML("latex", nil, escapeXMLText(stripLatexMarkdownEscapes(string(node.(*mathInline).content)))), nil
	case kindMathBlock:
		return elementXML("latex", nil, escapeXMLText(stripLatexMarkdownEscapes(string(node.(*mathBlock).content)))), nil
	case extast.KindTaskCheckBox:
		return "", nil
	}

	if node.Type() == gast.TypeBlock {
		return escapeXMLText(strings.TrimSpace(extractMarkdownText(node, source))), nil
	}
	return escapeXMLText(extractMarkdownText(node, source)), nil
}

func renderInlineContainer(node gast.Node, tag string, attrs map[string]string, source []byte) (string, error) {
	children, err := renderInlineFragment(node, source, false)
	if err != nil {
		return "", err
	}
	return elementXML(tag, attrs, children), nil
}

func elementXML(tag string, attrs map[string]string, inner string) string {
	node := newElement(tag, attrs)
	rendered := renderNodes([]*Node{node})
	if inner == "" {
		return rendered
	}
	close := "</" + tag + ">"
	if strings.HasSuffix(rendered, close) {
		return strings.TrimSuffix(rendered, close) + inner + close
	}
	return rendered
}

func wrapParagraphChildren(children []*Node) []*Node {
	var out []*Node
	var inline []*Node
	flush := func() {
		if len(inline) == 0 {
			return
		}
		paragraph := newElement("p", nil)
		for _, child := range inline {
			paragraph.addChild(child)
		}
		out = append(out, paragraph)
		inline = nil
	}
	for _, child := range children {
		if child != nil && child.typ == nodeElement && layoutOf(child.tag) == layoutBlock {
			flush()
			out = append(out, child)
			continue
		}
		inline = append(inline, child)
	}
	flush()
	return out
}

func paragraphOnlyInline(node *Node) bool {
	if node == nil || node.typ != nodeElement || node.tag != "p" {
		return false
	}
	for _, child := range node.children {
		if child.typ == nodeElement && layoutOf(child.tag) == layoutBlock {
			return false
		}
	}
	return true
}

func renderTable(node gast.Node, source []byte) ([]*Node, error) {
	table := newElement("table", nil)
	var body *Node
	for child := node.FirstChild(); child != nil; child = child.NextSibling() {
		switch child.Kind() {
		case extast.KindTableHeader:
			head := newElement("thead", nil)
			row, err := renderTableRow(child, true, source)
			if err != nil {
				return nil, err
			}
			head.addChild(row)
			table.addChild(head)
		case extast.KindTableRow:
			if body == nil {
				body = newElement("tbody", nil)
				table.addChild(body)
			}
			row, err := renderTableRow(child, false, source)
			if err != nil {
				return nil, err
			}
			body.addChild(row)
		}
	}
	return []*Node{table}, nil
}

func renderTableRow(node gast.Node, header bool, source []byte) (*Node, error) {
	row := newElement("tr", nil)
	for child := node.FirstChild(); child != nil; child = child.NextSibling() {
		if child.Kind() != extast.KindTableCell {
			continue
		}
		cellAST := child.(*extast.TableCell)
		tag := "td"
		if header {
			tag = "th"
		}
		attrs := map[string]string(nil)
		switch cellAST.Alignment {
		case extast.AlignCenter:
			attrs = map[string]string{"align": "center"}
		case extast.AlignRight:
			attrs = map[string]string{"align": "right"}
		}
		cell := newElement(tag, attrs)
		content, err := renderInlineChildren(cellAST, source)
		if err != nil {
			return nil, err
		}
		for _, inline := range content {
			cell.addChild(inline)
		}
		row.addChild(cell)
	}
	return row, nil
}

func renderDefinitionList(node gast.Node, source []byte) ([]*Node, error) {
	var out []*Node
	for child := node.FirstChild(); child != nil; child = child.NextSibling() {
		switch child.Kind() {
		case extast.KindDefinitionTerm:
			fragment, err := renderInlineFragment(child, source, false)
			if err != nil {
				return nil, err
			}
			nodes, err := parseXML(fragment)
			if err != nil {
				return nil, err
			}
			paragraph := newElement("p", nil)
			bold := newElement("b", nil)
			for _, node := range nodes {
				bold.addChild(node)
			}
			paragraph.addChild(bold)
			out = append(out, paragraph)
		case extast.KindDefinitionDescription:
			quote, err := renderContainer("blockquote", nil, child, source)
			if err != nil {
				return nil, err
			}
			out = append(out, quote...)
		}
	}
	return out, nil
}

func appendRawTextWithBreaks(parent *Node, content string) {
	if content == "" {
		return
	}
	start := 0
	for i := 0; i < len(content); i++ {
		if content[i] != '\n' && content[i] != '\r' {
			continue
		}
		if i > start {
			parent.addChild(newText(content[start:i]))
		}
		if content[i] == '\r' && i+1 < len(content) && content[i+1] == '\n' {
			i++
		}
		parent.addChild(newElement("br", nil))
		start = i + 1
	}
	if start < len(content) {
		parent.addChild(newText(content[start:]))
	}
}

func stripMarkdownEscapesInNodes(nodes []*Node, inCode, inLatex bool) {
	for _, node := range nodes {
		if node == nil {
			continue
		}
		if node.typ == nodeText {
			switch {
			case inCode:
			case inLatex:
				node.text = stripLatexMarkdownEscapes(node.text)
			default:
				node.text = stripBackslashEscapes(node.text)
			}
			continue
		}
		stripMarkdownEscapesInNodes(node.children, inCode || node.tag == "code" || node.tag == "pre", inLatex || node.tag == "latex")
	}
}

func stripBackslashEscapes(value string) string {
	if !strings.Contains(value, `\`) {
		return value
	}
	var out strings.Builder
	out.Grow(len(value))
	for i := 0; i < len(value); i++ {
		if value[i] == '\\' && i+1 < len(value) && isASCIIPunctuation(value[i+1]) {
			out.WriteByte(value[i+1])
			i++
			continue
		}
		out.WriteByte(value[i])
	}
	return out.String()
}

func stripLatexMarkdownEscapes(value string) string {
	if !strings.Contains(value, `\`) {
		return value
	}
	var out strings.Builder
	out.Grow(len(value))
	for i := 0; i < len(value); i++ {
		if value[i] == '\\' && i+1 < len(value) && strings.ContainsRune("_^&*[]$~<>`#+-=:", rune(value[i+1])) {
			out.WriteByte(value[i+1])
			i++
			continue
		}
		out.WriteByte(value[i])
	}
	return out.String()
}

func isASCIIPunctuation(ch byte) bool {
	return ch >= '!' && ch <= '/' || ch >= ':' && ch <= '@' || ch >= '[' && ch <= '`' || ch >= '{' && ch <= '~'
}

func trimOneTrailingNewline(value string) string {
	if strings.HasSuffix(value, "\r\n") {
		return value[:len(value)-2]
	}
	return strings.TrimSuffix(value, "\n")
}

func collectMarkdownChildText(node gast.Node, source []byte) string {
	var out strings.Builder
	for child := node.FirstChild(); child != nil; child = child.NextSibling() {
		switch child.Kind() {
		case gast.KindText:
			out.Write(child.(*gast.Text).Value(source))
		case gast.KindString:
			out.Write(child.(*gast.String).Value)
		default:
			out.WriteString(collectMarkdownChildText(child, source))
		}
	}
	return out.String()
}

func extractMarkdownText(node gast.Node, source []byte) string {
	switch node.Kind() {
	case gast.KindText:
		return string(node.(*gast.Text).Value(source))
	case gast.KindString:
		return string(node.(*gast.String).Value)
	case gast.KindCodeSpan:
		return collectMarkdownChildText(node, source)
	}
	if node.Type() == gast.TypeBlock && node.Lines() != nil && node.Lines().Len() > 0 {
		return string(node.Lines().Value(source))
	}
	var out strings.Builder
	for child := node.FirstChild(); child != nil; child = child.NextSibling() {
		out.WriteString(extractMarkdownText(child, source))
	}
	return out.String()
}

func headingTag(level int) string {
	if level < 1 || level > 6 {
		return "p"
	}
	return fmt.Sprintf("h%d", level)
}

func normalizeListIndent(markdown string) string {
	lines := strings.Split(markdown, "\n")
	type stackEntry struct{ indent int }
	var stack []stackEntry
	inFence := false
	changed := false
	lastOriginal, lastNormalized := 0, 0
	for i, line := range lines {
		trimmed := strings.TrimLeft(line, " ")
		if strings.HasPrefix(trimmed, "```") || strings.HasPrefix(trimmed, "~~~") {
			inFence = !inFence
			continue
		}
		if inFence || trimmed == "" {
			continue
		}
		indent := len(line) - len(trimmed)
		if markdownListMarkerLength(trimmed) > 0 {
			for len(stack) > 0 && indent <= stack[len(stack)-1].indent {
				stack = stack[:len(stack)-1]
			}
			normalized := len(stack) * 4
			stack = append(stack, stackEntry{indent: indent})
			lastOriginal, lastNormalized = indent, normalized
			if indent != normalized {
				lines[i] = strings.Repeat(" ", normalized) + trimmed
				changed = true
			}
		} else if len(stack) > 0 && indent > lastOriginal {
			delta := lastNormalized - lastOriginal
			if delta != 0 {
				normalized := indent + delta
				if normalized < 0 {
					normalized = 0
				}
				lines[i] = strings.Repeat(" ", normalized) + trimmed
				changed = true
			}
		}
	}
	if !changed {
		return markdown
	}
	return strings.Join(lines, "\n")
}

func markdownListMarkerLength(value string) int {
	if len(value) >= 2 && (value[0] == '-' || value[0] == '*' || value[0] == '+') && value[1] == ' ' {
		return 2
	}
	i := 0
	for i < len(value) && value[i] >= '0' && value[i] <= '9' {
		i++
	}
	if i > 0 && i+1 < len(value) && (value[i] == '.' || value[i] == ')') && value[i+1] == ' ' {
		return i + 2
	}
	return 0
}
