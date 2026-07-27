// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package model

import "testing"

// TestCanonicalTarget_Equal locks the normalization contract that
// event.TargetResourceEqual delegates to: escaping and selector order are
// ignored, resource_type must match, and an unparseable query is reported via
// ok=false so the caller can fall back to an exact comparison.
func TestCanonicalTarget_Equal(t *testing.T) {
	tests := []struct {
		name string
		a, b string
		want bool
	}{
		{"identical", "im.message?chat_id=oc_1", "im.message?chat_id=oc_1", true},
		{"escaping differs", "im.message?chat_id=oc%5F1", "im.message?chat_id=oc_1", true},
		{"selector order differs", "im.message?a=1&b=2", "im.message?b=2&a=1", true},
		{"different resource_type", "im.message?chat_id=oc_1", "im.chat?chat_id=oc_1", false},
		{"different value", "im.message?chat_id=oc_1", "im.message?chat_id=oc_2", false},
		{"no selector, equal", "im.message", "im.message", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ca, aok := ParseCanonicalTarget(tt.a)
			cb, bok := ParseCanonicalTarget(tt.b)
			if !aok || !bok {
				t.Fatalf("ParseCanonicalTarget failed: aok=%v bok=%v", aok, bok)
			}
			if got := ca.Equal(cb); got != tt.want {
				t.Errorf("Equal(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
			if got := cb.Equal(ca); got != tt.want {
				t.Errorf("Equal(%q, %q) [swapped] = %v, want %v", tt.b, tt.a, got, tt.want)
			}
		})
	}
}

func TestParseCanonicalTarget_UnparseableQuery(t *testing.T) {
	// A malformed percent-escape in the query is unparseable; ok must be false
	// so callers fall back to an exact string comparison.
	if _, ok := ParseCanonicalTarget("im.message?chat_id=%zz"); ok {
		t.Error("malformed query must report ok=false")
	}
}
