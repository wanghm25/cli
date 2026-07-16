// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package docxparse

import (
	"fmt"
	"html"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	MaxInputBytes   = 20_000_000
	MaxNestingDepth = 1024
)

var forbiddenXMLDeclaration = regexp.MustCompile(`(?i)<!\s*(?:DOCTYPE|ENTITY)\b`)

func validateSource(source string) error {
	if len(source) > MaxInputBytes {
		return fmt.Errorf("input is too large (%d bytes, limit %d)", len(source), MaxInputBytes)
	}
	if forbiddenXMLDeclaration.MatchString(source) {
		return fmt.Errorf("XML input must not contain DOCTYPE or ENTITY declarations")
	}
	if !utf8.ValidString(source) {
		return fmt.Errorf("input must be valid UTF-8")
	}
	return nil
}

func parseXML(source string) ([]*Node, error) {
	if err := validateSource(source); err != nil {
		return nil, err
	}
	source = strings.TrimPrefix(source, "\uFEFF")

	root := newElement("__fragment__", nil)
	stack := []*Node{root}
	for i := 0; i < len(source); {
		lt := strings.IndexByte(source[i:], '<')
		if lt < 0 {
			if err := validateXMLText(source[i:], i); err != nil {
				return nil, err
			}
			appendText(stack[len(stack)-1], source[i:])
			break
		}
		lt += i
		if err := validateXMLText(source[i:lt], i); err != nil {
			return nil, err
		}
		appendText(stack[len(stack)-1], source[i:lt])

		token, end, state := scanXMLToken(source, lt)
		switch state {
		case tokenComment, tokenProcessingInstruction:
			i = end
			continue
		case tokenCDATA:
			appendTextValue(stack[len(stack)-1], token.text)
			i = end
			continue
		case tokenInvalid:
			return nil, fmt.Errorf("invalid XML token at byte %d", lt)
		case tokenIncomplete:
			return nil, fmt.Errorf("unterminated XML tag at byte %d", lt)
		}

		spec, allowed := lookupTag(token.name)
		if !allowed {
			return nil, fmt.Errorf("unsupported LarkOpenCLI tag <%s> at byte %d", token.name, lt)
		}
		canonical := spec.canonical
		if token.spacingNormalized {
			return nil, fmt.Errorf("invalid whitespace in XML tag <%s> at byte %d", token.name, lt)
		}

		if token.closing {
			if isVoidTag(canonical) {
				return nil, fmt.Errorf("void tag <%s/> must not have a closing tag", canonical)
			}
			if len(stack) == 1 {
				return nil, fmt.Errorf("unexpected closing tag </%s> at byte %d", canonical, lt)
			}
			open := stack[len(stack)-1].tag
			if open != canonical {
				return nil, fmt.Errorf("mismatched closing tag </%s> at byte %d; expected </%s>", canonical, lt, open)
			}
			stack = stack[:len(stack)-1]
			i = end
			continue
		}

		if len(stack) > 1 && shouldAutoClose(stack[len(stack)-1].tag, canonical) {
			return nil, fmt.Errorf("invalid <%s> inside <%s> at byte %d", canonical, stack[len(stack)-1].tag, lt)
		}
		attrs := normalizeAttributes(token.name, canonical, token.attrs)
		node := newElement(canonical, attrs)
		stack[len(stack)-1].addChild(node)
		if !token.selfClosing && !isVoidTag(canonical) {
			if len(stack) > MaxNestingDepth {
				return nil, fmt.Errorf("XML nesting exceeds limit %d at byte %d", MaxNestingDepth, lt)
			}
			stack = append(stack, node)
		}
		i = end
	}

	if len(stack) > 1 {
		return nil, fmt.Errorf("missing closing tag </%s> at end of input", stack[len(stack)-1].tag)
	}
	normalizeParsedLineBreaks(root.children, false, false)
	for _, child := range root.children {
		child.parent = nil
	}
	return root.children, nil
}

// normalizeParsedLineBreaks removes formatting newlines from ordinary XML,
// while source-bearing code/whiteboard blocks keep semantic
// line breaks as explicit <br/> nodes. str_replace pattern/replacement payloads
// retain raw newlines because their string matching semantics depend on them.
func normalizeParsedLineBreaks(nodes []*Node, sourceBlock, stringMutation bool) {
	for _, node := range nodes {
		if node == nil || node.typ != nodeElement {
			continue
		}
		nextSourceBlock := sourceBlock || node.tag == "code" || node.tag == "whiteboard"
		nextStringMutation := stringMutation || node.tag == "str_replace"
		preserveRaw := nextStringMutation && (node.tag == "pattern" || node.tag == "replacement")
		if node.tag == "code" || node.tag == "whiteboard" {
			trimSourceBlockBoundaryNewlines(node.children)
		}
		children := make([]*Node, 0, len(node.children))
		for _, child := range node.children {
			if child.typ != nodeText || !strings.ContainsAny(child.text, "\r\n") {
				children = append(children, child)
				continue
			}
			switch {
			case preserveRaw:
				children = append(children, child)
			case nextSourceBlock:
				for _, replacement := range rawTextWithBreakNodes(child.text) {
					replacement.parent = node
					children = append(children, replacement)
				}
			default:
				child.text = strings.NewReplacer("\r", "", "\n", "").Replace(child.text)
				if child.text != "" {
					children = append(children, child)
				}
			}
		}
		node.children = children
		normalizeParsedLineBreaks(node.children, nextSourceBlock, nextStringMutation)
	}
}

func trimSourceBlockBoundaryNewlines(children []*Node) {
	for _, child := range children {
		if child.typ == nodeText {
			child.text = strings.TrimLeft(child.text, "\r\n")
			break
		}
		if child.typ == nodeElement {
			break
		}
	}
	for i := len(children) - 1; i >= 0; i-- {
		child := children[i]
		if child.typ == nodeText {
			child.text = strings.TrimRight(child.text, "\r\n")
			break
		}
		if child.typ == nodeElement {
			break
		}
	}
}

func rawTextWithBreakNodes(content string) []*Node {
	if content == "" {
		return nil
	}
	var nodes []*Node
	start := 0
	for i := 0; i < len(content); i++ {
		if content[i] != '\n' && content[i] != '\r' {
			continue
		}
		if i > start {
			nodes = append(nodes, newText(content[start:i]))
		}
		if content[i] == '\r' && i+1 < len(content) && content[i+1] == '\n' {
			i++
		}
		nodes = append(nodes, newElement("br", nil))
		start = i + 1
	}
	if start < len(content) {
		nodes = append(nodes, newText(content[start:]))
	}
	return nodes
}

type tokenState uint8

const (
	tokenOK tokenState = iota
	tokenInvalid
	tokenIncomplete
	tokenComment
	tokenProcessingInstruction
	tokenCDATA
)

type xmlToken struct {
	name              string
	attrs             map[string]string
	text              string
	closing           bool
	selfClosing       bool
	spacingNormalized bool
}

func scanXMLToken(source string, start int) (xmlToken, int, tokenState) {
	if strings.HasPrefix(source[start:], "<![CDATA[") {
		const marker = "<![CDATA["
		contentStart := start + len(marker)
		if closeAt := strings.Index(source[contentStart:], "]]>"); closeAt >= 0 {
			contentEnd := contentStart + closeAt
			return xmlToken{text: source[contentStart:contentEnd]}, contentEnd + len("]]>"), tokenCDATA
		}
		return xmlToken{}, len(source), tokenIncomplete
	}
	if strings.HasPrefix(source[start:], "<!--") {
		if closeAt := strings.Index(source[start+4:], "-->"); closeAt >= 0 {
			if strings.Contains(source[start+4:start+4+closeAt], "--") {
				return xmlToken{}, start + 1, tokenInvalid
			}
			return xmlToken{}, start + 4 + closeAt + 3, tokenComment
		}
		return xmlToken{}, len(source), tokenIncomplete
	}
	if strings.HasPrefix(source[start:], "<?") {
		if closeAt := strings.Index(source[start+2:], "?>"); closeAt >= 0 {
			return xmlToken{}, start + 2 + closeAt + 2, tokenProcessingInstruction
		}
		return xmlToken{}, len(source), tokenIncomplete
	}

	quote := byte(0)
	end := -1
	for i := start + 1; i < len(source); i++ {
		switch source[i] {
		case '\'', '"':
			if quote == 0 {
				quote = source[i]
			} else if quote == source[i] {
				quote = 0
			}
		case '>':
			if quote == 0 {
				end = i + 1
				i = len(source)
			}
		case '<':
			// A second unquoted '<' cannot belong to the current XML tag.
			// Stop here so a long sequence of invalid tag starts is scanned
			// once instead of repeatedly searching to a distant '>'.
			if quote == 0 {
				return xmlToken{}, start + 1, tokenInvalid
			}
		}
	}
	if end < 0 {
		candidate := strings.TrimSpace(source[start+1:])
		if candidate == "" || !isTagNameStart(candidate[0]) && candidate[0] != '/' {
			return xmlToken{}, start + 1, tokenInvalid
		}
		return xmlToken{}, len(source), tokenIncomplete
	}

	body := source[start+1 : end-1]
	if body == "" {
		return xmlToken{}, end, tokenInvalid
	}
	token := xmlToken{}
	position := 0
	for position < len(body) && isXMLSpace(body[position]) {
		position++
	}
	if position > 0 {
		token.spacingNormalized = true
	}
	if position >= len(body) || body[position] == '!' {
		return xmlToken{}, end, tokenInvalid
	}
	if body[position] == '/' {
		token.closing = true
		position++
		spaceStart := position
		for position < len(body) && isXMLSpace(body[position]) {
			position++
		}
		if position > spaceStart {
			token.spacingNormalized = true
		}
	}
	if position >= len(body) || !isTagNameStart(body[position]) {
		return xmlToken{}, end, tokenInvalid
	}
	nameStart := position
	position++
	for position < len(body) && isTagNamePart(body[position]) {
		position++
	}
	token.name = body[nameStart:position]
	rawRemainder := body[position:]
	remainder := strings.TrimRightFunc(rawRemainder, unicode.IsSpace)
	if token.closing {
		if strings.TrimSpace(remainder) != "" {
			return xmlToken{}, end, tokenInvalid
		}
		return token, end, tokenOK
	}
	if strings.HasSuffix(remainder, "/") {
		if len(remainder) != len(rawRemainder) {
			return xmlToken{}, end, tokenInvalid
		}
		token.selfClosing = true
		remainder = strings.TrimRightFunc(strings.TrimSuffix(remainder, "/"), unicode.IsSpace)
	}
	trimmedAttrs := strings.TrimLeftFunc(remainder, unicode.IsSpace)
	if trimmedAttrs != "" && !isAttributeNameStart(trimmedAttrs[0]) {
		return xmlToken{}, end, tokenInvalid
	}
	var ok bool
	token.attrs, ok = parseStrictAttributes(remainder)
	if !ok {
		return xmlToken{}, end, tokenInvalid
	}
	return token, end, tokenOK
}

func isXMLSpace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n'
}

func isTagNameStart(ch byte) bool {
	return ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z'
}

func isTagNamePart(ch byte) bool {
	return isTagNameStart(ch) || ch >= '0' && ch <= '9' || ch == '_' || ch == '-' || ch == '.' || ch == ':'
}

func isAttributeNameStart(ch byte) bool {
	return isTagNameStart(ch) || ch == '_' || ch == ':'
}

func parseAttributes(source string) map[string]string {
	attrs := map[string]string{}
	for i := 0; i < len(source); {
		for i < len(source) && unicode.IsSpace(rune(source[i])) {
			i++
		}
		if i >= len(source) {
			break
		}
		start := i
		for i < len(source) && isAttributeNameByte(source[i]) {
			i++
		}
		if start == i {
			i++
			continue
		}
		name := source[start:i]
		for i < len(source) && unicode.IsSpace(rune(source[i])) {
			i++
		}
		value := ""
		if i < len(source) && source[i] == '=' {
			i++
			for i < len(source) && unicode.IsSpace(rune(source[i])) {
				i++
			}
			if i < len(source) && (source[i] == '\'' || source[i] == '"') {
				quote := source[i]
				i++
				start = i
				for i < len(source) && source[i] != quote {
					i++
				}
				value = source[start:i]
				if i < len(source) {
					i++
				}
			} else {
				start = i
				for i < len(source) && !unicode.IsSpace(rune(source[i])) {
					i++
				}
				value = source[start:i]
			}
		}
		attrs[name] = html.UnescapeString(value)
	}
	if len(attrs) == 0 {
		return nil
	}
	return attrs
}

// parseStrictAttributes implements the quoted attribute grammar accepted by
// XML. parseAttributes remains intentionally permissive for the Markdown
// container extension, whose input is Markdown rather than an XML document.
func parseStrictAttributes(source string) (map[string]string, bool) {
	attrs := map[string]string{}
	for i := 0; i < len(source); {
		spaceStart := i
		for i < len(source) && isXMLSpace(source[i]) {
			i++
		}
		if i >= len(source) {
			break
		}
		if i == spaceStart || !isAttributeNameStart(source[i]) {
			return nil, false
		}

		nameStart := i
		i++
		for i < len(source) && isTagNamePart(source[i]) {
			i++
		}
		name := source[nameStart:i]
		if _, exists := attrs[name]; exists {
			return nil, false
		}

		for i < len(source) && isXMLSpace(source[i]) {
			i++
		}
		if i >= len(source) || source[i] != '=' {
			return nil, false
		}
		i++
		for i < len(source) && isXMLSpace(source[i]) {
			i++
		}
		if i >= len(source) || (source[i] != '\'' && source[i] != '"') {
			return nil, false
		}

		quote := source[i]
		i++
		valueStart := i
		for i < len(source) && source[i] != quote {
			if source[i] == '<' {
				return nil, false
			}
			i++
		}
		if i >= len(source) {
			return nil, false
		}
		rawValue := normalizeXMLAttributeAmpersands(source[valueStart:i])
		if invalidXMLEntityAt(rawValue) >= 0 {
			return nil, false
		}
		attrs[name] = html.UnescapeString(rawValue)
		i++
	}
	if len(attrs) == 0 {
		return nil, true
	}
	return attrs, true
}

func isAttributeNameByte(ch byte) bool {
	return ch > ' ' && ch != '=' && ch != '/' && ch != '>'
}

func appendText(parent *Node, raw string) {
	if parent == nil || raw == "" {
		return
	}
	appendTextValue(parent, html.UnescapeString(raw))
}

func appendTextValue(parent *Node, text string) {
	if parent == nil || text == "" {
		return
	}
	if strings.TrimSpace(text) == "" && !preserveSpaceTags[parent.tag] && parent.tag != "whiteboard" {
		return
	}
	if count := len(parent.children); count > 0 && parent.children[count-1].typ == nodeText {
		parent.children[count-1].text += text
		return
	}
	parent.addChild(newText(text))
}

func validateXMLText(value string, absoluteOffset int) error {
	if offset := strings.Index(value, "]]>"); offset >= 0 {
		return fmt.Errorf("invalid ]]> sequence in XML text at byte %d", absoluteOffset+offset)
	}
	if offset := invalidXMLEntityAt(value); offset >= 0 {
		return fmt.Errorf("invalid XML entity at byte %d", absoluteOffset+offset)
	}
	return nil
}

func invalidXMLEntityAt(value string) int {
	for cursor := 0; cursor < len(value); {
		relative := strings.IndexByte(value[cursor:], '&')
		if relative < 0 {
			return -1
		}
		start := cursor + relative
		endRelative := strings.IndexByte(value[start+1:], ';')
		if endRelative < 0 {
			return start
		}
		end := start + 1 + endRelative
		if !isValidXMLEntity(value[start+1 : end]) {
			return start
		}
		cursor = end + 1
	}
	return -1
}

func isValidXMLEntity(entity string) bool {
	switch entity {
	case "amp", "lt", "gt", "quot", "apos":
		return true
	}

	base := 10
	digits := ""
	switch {
	case strings.HasPrefix(entity, "#x"):
		base = 16
		digits = entity[2:]
	case strings.HasPrefix(entity, "#"):
		digits = entity[1:]
	default:
		return false
	}
	if digits == "" {
		return false
	}
	value, err := strconv.ParseUint(digits, base, 32)
	if err != nil {
		return false
	}
	r := rune(value)
	return r == '\t' || r == '\n' || r == '\r' ||
		r >= 0x20 && r <= 0xD7FF ||
		r >= 0xE000 && r <= 0xFFFD ||
		r >= 0x10000 && r <= utf8.MaxRune
}
