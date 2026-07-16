// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package docxparse

import "strings"

// normalizeXMLAttributeAmpersands escapes bare ampersands in an XML attribute
// value so local parsing matches the server SDK. Complete entity references
// remain untouched for the strict parser to validate.
func normalizeXMLAttributeAmpersands(value string) string {
	firstBare := -1
	for cursor := 0; cursor < len(value); {
		relative := strings.IndexByte(value[cursor:], '&')
		if relative < 0 {
			break
		}
		ampersand := cursor + relative
		if isBareXMLAttributeAmpersand(value, ampersand) {
			firstBare = ampersand
			break
		}
		cursor = ampersand + 1
	}
	if firstBare < 0 {
		return value
	}

	var out strings.Builder
	out.Grow(len(value))
	out.WriteString(value[:firstBare])

	for i := firstBare; i < len(value); i++ {
		if value[i] == '&' && isBareXMLAttributeAmpersand(value, i) {
			out.WriteString("&amp;")
			continue
		}
		out.WriteByte(value[i])
	}
	return out.String()
}

func isBareXMLAttributeAmpersand(value string, start int) bool {
	if start+1 >= len(value) {
		return true
	}
	if value[start+1] == '#' {
		return false
	}
	if !isTagNameStart(value[start+1]) && value[start+1] != '_' {
		return true
	}
	for i := start + 2; i < len(value); i++ {
		if value[i] == ';' {
			return false
		}
		if !isTagNamePart(value[i]) {
			return true
		}
	}
	return true
}
