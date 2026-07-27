// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"net/url"
	"reflect"
	"sort"
	"strings"
)

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
// input it cannot normalize.
func TargetResourceEqual(a, b string) bool {
	aType, aSel, aOK := splitTargetResource(a)
	bType, bSel, bOK := splitTargetResource(b)
	if !aOK || !bOK {
		return a == b
	}
	if aType != bType {
		return false
	}
	return reflect.DeepEqual(aSel, bSel)
}

// splitTargetResource splits s into its resource_type (everything before the
// first '?') and its decoded selector map. Each selector's decoded values are
// sorted so the comparison in TargetResourceEqual is order-independent even for
// a repeated key. ok is false when the selector query cannot be parsed, which
// signals the caller to fall back to an exact string comparison.
func splitTargetResource(s string) (resourceType string, selectors url.Values, ok bool) {
	resourceType = s
	query := ""
	if i := strings.IndexByte(s, '?'); i >= 0 {
		resourceType = s[:i]
		query = s[i+1:]
	}
	values, err := url.ParseQuery(query)
	if err != nil {
		return "", nil, false
	}
	for _, v := range values {
		sort.Strings(v)
	}
	return resourceType, values, true
}
