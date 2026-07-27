// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package model

import (
	"net/url"
	"reflect"
	"sort"
	"strings"
)

// CanonicalTarget is the normalized form of a target_resource string — the
// resource_type plus a decoded, order-independent selector map — that resource
// comparison operates on. A target_resource is a "<resource_type>?<selector
// query>" string (e.g. "im.message?chat_id=oc_1"): ParseCanonicalTarget splits
// it on the FIRST '?' into the resource_type and the selector query, and
// decodes the query with url.ParseQuery so percent/plus escaping and selector
// order no longer matter to Equal.
type CanonicalTarget struct {
	// ResourceType is everything before the first '?' (the whole string when
	// there is no '?').
	ResourceType string
	// Selectors is the decoded selector query; each key's values are sorted so
	// Equal is order-independent even for a repeated key.
	Selectors url.Values
}

// ParseCanonicalTarget normalizes a target_resource string. ok is false when
// the selector query cannot be parsed, which signals the caller to fall back to
// an exact string comparison rather than treat the input more leniently than a
// plain ==.
func ParseCanonicalTarget(s string) (target CanonicalTarget, ok bool) {
	resourceType := s
	query := ""
	if i := strings.IndexByte(s, '?'); i >= 0 {
		resourceType = s[:i]
		query = s[i+1:]
	}
	values, err := url.ParseQuery(query)
	if err != nil {
		return CanonicalTarget{}, false
	}
	for _, v := range values {
		sort.Strings(v)
	}
	return CanonicalTarget{ResourceType: resourceType, Selectors: values}, true
}

// Equal reports whether t and other denote the same resource + selectors: the
// same resource_type AND the same decoded selector map (the same keys with the
// same decoded values, independent of escaping and selector order).
func (t CanonicalTarget) Equal(other CanonicalTarget) bool {
	if t.ResourceType != other.ResourceType {
		return false
	}
	return reflect.DeepEqual(t.Selectors, other.Selectors)
}
