// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import "github.com/larksuite/cli/internal/event/model"

// TargetResourceEqual reports whether two target_resource strings denote the
// same resource + selector, tolerating URL-escaping and selector-ordering
// differences between the platform's echoed target_resource and a
// locally-built one (which is assembled as
// resource_type + "?" + key + "=" + url.QueryEscape(value)).
//
// Each side is a "<resource_type>?<selector query>" string (e.g.
// "im.message?chat_id=oc_1"): it is split on the FIRST '?' into the
// resource_type (left) and the selector query (right), and the query is decoded
// with url.ParseQuery into a selector map. Two target_resources are equal iff
// their resource_type matches AND their decoded selector maps are equal — the
// same keys with the same decoded values, independent of percent/plus escaping
// and of the order the selectors appear in.
//
// If EITHER side's selector query cannot be parsed, it falls back to an exact
// string comparison, so the result is never more lenient than a plain == on an
// input it cannot normalize. The comparison itself is owned by
// internal/event/model.TargetResourceEqual; this is a thin facade so existing
// event.* callers keep the unchanged spelling.
func TargetResourceEqual(a, b string) bool {
	return model.TargetResourceEqual(a, b)
}
