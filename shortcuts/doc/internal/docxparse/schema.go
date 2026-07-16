// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package docxparse

import (
	"sort"
	"strconv"
	"strings"
)

type tagLayout string

const (
	layoutBlock      tagLayout = "block"
	layoutInline     tagLayout = "inline"
	layoutDual       tagLayout = "dual"
	layoutStructural tagLayout = "structural"
	layoutCommand    tagLayout = "command"
)

type tagSpec struct {
	canonical string
	layout    tagLayout
}

var tagSpecs = map[string]tagSpec{}

// tagAliases mirrors the public compatibility aliases declared by the
// LarkOpenCLI SDK. Parsing keeps the caller's XML unchanged; aliases are only
// canonicalized in the in-memory tree used for profiling and Markdown output.
var tagAliases = map[string]string{
	"strong":           "b",
	"text":             "span",
	"equation":         "latex",
	"lark-table":       "table",
	"lark-tr":          "tr",
	"lark-td":          "td",
	"image":            "img",
	"reference-synced": "synced_reference",
	"source-synced":    "synced-source",
	"at":               "cite",
	"chat-card":        "chat_card",
	"folder_manager":   "folder-manager",
}

type attributeAliasRule struct {
	canonical string
	transform func(string) (string, bool)
}

var commonAttributeAliases = map[string]attributeAliasRule{
	"color":            {canonical: "text-color"},
	"textcolor":        {canonical: "text-color"},
	"text_color":       {canonical: "text-color"},
	"bgcolor":          {canonical: "background-color"},
	"background_color": {canonical: "background-color"},
}

var tagAttributeAliases = map[string]map[string]attributeAliasRule{
	"img": {
		"url":      {canonical: "href"},
		"file_key": {canonical: "img_key"},
	},
	"callout": {
		"color": {canonical: "background-color"},
		"icon":  {canonical: "emoji"},
	},
	"column": {
		"width": {canonical: "width-ratio", transform: normalizeWidthRatio},
	},
	"chat_card": {
		"id": {canonical: "chat-id", transform: requireChatID},
	},
	"cite": {
		"user_id": {canonical: "user-id"},
	},
}

var rawTagAttributeAliases = map[string]map[string]attributeAliasRule{
	"at": {
		"id":      {canonical: "user-id"},
		"user_id": {canonical: "user-id"},
	},
}

var requiredAttributes = map[string][]string{
	"task": {"task-id"},
}

var requiredAnyAttributes = map[string][][]string{
	"img":        {{"src", "img_key", "href"}},
	"whiteboard": {{"token", "type"}},
	"chat_card":  {{"token", "chat-id"}},
	"bookmark":   {{"href", "name"}},
}

func init() {
	registerTags(layoutBlock,
		"title", "h1", "h2", "h3", "h4", "h5", "h6", "h7", "h8", "h9", "p",
		"div", "ul", "ol", "li", "blockquote", "grid", "column", "table", "thead",
		"tbody", "tfoot", "tr", "hr", "pre", "img", "source", "bitable", "sheet",
		"mindnote", "whiteboard", "base_refer", "synced_reference", "isv", "html5-block",
		"view", "synced-source", "readonly-block", "figure", "callout", "checkbox",
		"chat_card", "okr", "okr-objective", "okr-key-result", "okr-progress", "poll",
		"agenda", "folder-manager", "sub-page-list", "wiki_catalog", "wiki_recent_update",
		"chart-embedded", "chart-refer-host-perm", "chart_embedded", "chart_refer_host_perm",
		"bookmark", "task", "vc-tabs", "vc-summary-tab", "vc-transcribe-tab", "append",
	)
	registerTags(layoutInline, "b", "em", "u", "del", "i", "span", "br", "inline-file", "mention-date", "cite", "button", "time", "a")
	registerTags(layoutDual, "latex", "code")
	registerTags(layoutStructural, "th", "td", "colgroup", "col", "sub-page")
	registerTags(layoutCommand,
		"comment", "block_delete", "str_delete", "str_replace", "block_replace", "block_insert",
		"block_move", "block_copy_insert_after", "src_block_ids", "create", "answer", "response",
		"identifier", "genre", "anchor", "type", "revision", "pattern", "replacement",
		"replace_content", "action", "content", "parameter", "generation", "block_id",
	)

}

func registerTags(layout tagLayout, tags ...string) {
	for _, tag := range tags {
		tagSpecs[tag] = tagSpec{canonical: tag, layout: layout}
	}
}

func lookupTag(raw string) (tagSpec, bool) {
	key := strings.ToLower(strings.TrimSpace(raw))
	if canonical, ok := tagAliases[key]; ok {
		key = canonical
	}
	spec, ok := tagSpecs[key]
	if !ok {
		return tagSpec{}, false
	}
	return spec, true
}

func layoutOf(tag string) tagLayout {
	spec, ok := lookupTag(tag)
	if !ok {
		return ""
	}
	return spec.layout
}

var voidTags = map[string]bool{
	"br":       true,
	"col":      true,
	"hr":       true,
	"img":      true,
	"source":   true,
	"sub-page": true,
}

func isVoidTag(tag string) bool { return voidTags[tag] }

var preserveSpaceTags = map[string]bool{
	"title": true, "h1": true, "h2": true, "h3": true, "h4": true,
	"h5": true, "h6": true, "h7": true, "h8": true, "h9": true,
	"p": true, "i": true, "b": true, "em": true, "u": true, "del": true,
	"code": true, "li": true, "a": true, "span": true,
}

var strictPhrasingTags = map[string]bool{
	"title": true, "span": true, "b": true, "em": true,
	"u": true, "del": true, "a": true,
}

var autoCloseTags = map[string]map[string]bool{
	"li":     {"li": true},
	"tr":     {"tr": true},
	"td":     {"td": true, "th": true, "tr": true, "tbody": true, "tfoot": true},
	"th":     {"th": true, "td": true, "tr": true, "tbody": true, "tfoot": true},
	"tbody":  {"tbody": true, "tfoot": true},
	"thead":  {"tbody": true, "tfoot": true},
	"column": {"column": true},
}

var requiredAncestorTags = map[string]map[string]bool{
	"column":         {"grid": true},
	"thead":          {"table": true},
	"tbody":          {"table": true},
	"tfoot":          {"table": true},
	"tr":             {"table": true, "thead": true, "tbody": true, "tfoot": true},
	"th":             {"tr": true},
	"td":             {"tr": true},
	"colgroup":       {"table": true},
	"col":            {"table": true, "colgroup": true},
	"okr-objective":  {"okr": true},
	"okr-key-result": {"okr": true, "okr-objective": true},
	"okr-progress":   {"okr-objective": true, "okr-key-result": true},
	"sub-page":       {"sub-page-list": true},
}

func shouldAutoClose(openTag, nextTag string) bool {
	if strictPhrasingTags[openTag] && layoutOf(nextTag) == layoutBlock {
		return true
	}
	return autoCloseTags[openTag] != nil && autoCloseTags[openTag][nextTag]
}

func normalizeAttributes(rawTag, canonical string, attrs map[string]string) map[string]string {
	rules := make(map[string]attributeAliasRule, len(commonAttributeAliases)+4)
	for alias, rule := range commonAttributeAliases {
		rules[alias] = rule
	}
	for alias, rule := range tagAttributeAliases[canonical] {
		rules[alias] = rule
	}
	rawKey := strings.ToLower(strings.TrimSpace(rawTag))
	for alias, rule := range rawTagAttributeAliases[rawKey] {
		rules[alias] = rule
	}

	aliases := make([]string, 0, len(rules))
	for alias := range rules {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)
	for _, alias := range aliases {
		value, exists := attrs[alias]
		if !exists {
			continue
		}
		rule := rules[alias]
		if rule.transform != nil {
			var ok bool
			value, ok = rule.transform(value)
			if !ok {
				continue
			}
		}
		if canonicalValue, exists := attrs[rule.canonical]; !exists || strings.TrimSpace(canonicalValue) == "" {
			if attrs == nil {
				attrs = map[string]string{}
			}
			attrs[rule.canonical] = value
		}
		delete(attrs, alias)
	}

	if rawKey == "at" {
		if attrs == nil {
			attrs = map[string]string{}
		}
		attrs["type"] = "user"
	}
	return attrs
}

func normalizeWidthRatio(value string) (string, bool) {
	trimmed := strings.TrimSuffix(strings.TrimSpace(value), "%")
	if trimmed == "" {
		return value, false
	}
	width, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		return value, false
	}
	return strconv.FormatFloat(width/100, 'f', 6, 64), true
}

func requireChatID(value string) (string, bool) {
	trimmed := strings.TrimSpace(value)
	return trimmed, strings.HasPrefix(trimmed, "oc_")
}
