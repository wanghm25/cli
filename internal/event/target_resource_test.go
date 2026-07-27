// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import "testing"

// TargetResourceEqual normalizes each side (resource_type + decoded selector
// map) so URL-escaping and selector-ordering differences between the
// platform's echoed target_resource and a locally-built one don't false-drop,
// while an unparseable query is never treated more leniently than a plain ==.
func TestTargetResourceEqual(t *testing.T) {
	tests := []struct {
		name string
		a, b string
		want bool
	}{
		// Identical strings are trivially equal.
		{"identical_with_selector", "im.message?chat_id=oc_1", "im.message?chat_id=oc_1", true},
		{"identical_no_selector", "im.message", "im.message", true},

		// Escaping-different-but-equal: a space encoded as %20 (platform) vs +
		// (url.QueryEscape, the local build) decodes to the same value.
		{"escaping_space_pct_vs_plus", "im.message?name=John%20Doe", "im.message?name=John+Doe", true},
		// A slash encoded (%2F, the local url.QueryEscape output) vs echoed raw.
		{"escaping_slash_encoded_vs_raw", "drive.file?path=a%2Fb", "drive.file?path=a/b", true},
		// The full local-build shape vs a platform echo that escapes a space.
		{"escaping_selector_value_space", "im.message?chat_id=oc%201", "im.message?chat_id=oc+1", true},

		// Selector order independence.
		{"selector_order_independent", "res?a=1&b=2", "res?b=2&a=1", true},

		// Genuine mismatches.
		{"selector_value_differs", "im.message?chat_id=oc_1", "im.message?chat_id=oc_2", false},
		{"resource_type_differs", "im.message?chat_id=oc_1", "im.contact?chat_id=oc_1", false},
		{"resource_type_differs_no_selector", "im.message", "mail.x", false},
		{"one_selector_of_many_differs", "res?a=1&b=2", "res?a=1&b=3", false},
		{"selector_key_differs", "res?a=1", "res?b=1", false},
		{"selector_present_vs_absent", "im.message?chat_id=oc_1", "im.message", false},

		// Unparseable query -> exact-compare fallback: never MORE lenient than ==.
		// Both unparseable + byte-identical -> exact compare says equal.
		{"unparseable_identical", "res?bad=%zz", "res?bad=%zz", true},
		// Both unparseable but different -> exact compare says not equal, even
		// though a decode (were it possible) might have normalized them.
		{"unparseable_semicolon_reordered", "res?a=1;b=2", "res?b=2;a=1", false},
		// One side unparseable -> fall back to exact compare (different strings).
		{"one_side_unparseable", "res?ok=1", "res?bad=%zz", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := TargetResourceEqual(tt.a, tt.b); got != tt.want {
				t.Errorf("TargetResourceEqual(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
			// Symmetry: the comparison must not depend on argument order.
			if got := TargetResourceEqual(tt.b, tt.a); got != tt.want {
				t.Errorf("TargetResourceEqual(%q, %q) [swapped] = %v, want %v", tt.b, tt.a, got, tt.want)
			}
		})
	}
}
