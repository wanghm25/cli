// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package catalog

import "testing"

func TestReverseResolve_LegacyEmptyTarget(t *testing.T) {
	registerResolveFixtures(t)

	key, ok := ReverseResolve("im.message.receive_v1", "")
	if !ok || key != "im.message.receive_v1" {
		t.Fatalf("legacy reverse = (%q, %v), want (im.message.receive_v1, true)", key, ok)
	}
}

func TestReverseResolve_RefinedChatID(t *testing.T) {
	registerResolveFixtures(t)

	key, ok := ReverseResolve("im.message.created_v1", "im.message?chat_id=oc_9f3b1c2d8a")
	if !ok || key != "im.message.created_v1/chat-id/oc_9f3b1c2d8a" {
		t.Fatalf("chat-id reverse = (%q, %v)", key, ok)
	}
}

func TestReverseResolve_RefinedOwnerMe(t *testing.T) {
	registerResolveFixtures(t)

	key, ok := ReverseResolve("im.message.created_v1", "im.message?owner=me")
	if !ok || key != "im.message.created_v1/owner/me" {
		t.Fatalf("owner/me reverse = (%q, %v)", key, ok)
	}
}

// TestReverseResolve_RoundTripsForwardResolve proves ReverseResolve is the exact
// inverse of ResolveEventKey — the reversed key equals the forward
// MaterializedKey byte-for-byte, including percent-escaping of a unicode value.
func TestReverseResolve_RoundTripsForwardResolve(t *testing.T) {
	registerResolveFixtures(t)

	for _, input := range []string{
		"im.message.created_v1/chat-id/oc_9f3b1c2d8a",
		"im.message.created_v1/chat-id/你好",
		"im.message.created_v1/chat-id/a=1&a=2?b=3%25",
		"im.message.created_v1/owner/me",
	} {
		fwd, err := ResolveEventKey(input)
		if err != nil {
			t.Fatalf("ResolveEventKey(%q): %v", input, err)
		}
		key, ok := ReverseResolve(fwd.Definition.EventType, fwd.TargetResource)
		if !ok || key != fwd.MaterializedKey {
			t.Errorf("round-trip %q: reverse = (%q, %v), want (%q, true)", input, key, ok, fwd.MaterializedKey)
		}
	}
}

// TestReverseResolve_Unavailable locks the explicit "unavailable" contract:
// ReverseResolve reports ok=false rather than fabricating a key.
func TestReverseResolve_Unavailable(t *testing.T) {
	registerResolveFixtures(t)

	cases := []struct {
		name           string
		eventType      string
		targetResource string
	}{
		{"unknown event_type, empty target", "does.not.exist", ""},
		{"unknown event_type, refined target", "does.not.exist", "im.message?chat_id=oc_1"},
		{"refined base with empty target (not materializable)", "im.message.created_v1", ""},
		{"legacy event_type carrying a target", "im.message.receive_v1", "im.message?chat_id=oc_1"},
		{"wrong resource_type", "im.message.created_v1", "im.chat?chat_id=oc_1"},
		{"unknown selector key", "im.message.created_v1", "im.message?thread_id=t_1"},
		{"fixed-value template mismatch", "im.message.created_v1", "im.message?owner=someone_else"},
		{"multiple selectors", "im.message.created_v1", "im.message?chat_id=oc_1&owner=me"},
		{"empty selector value", "im.message.created_v1", "im.message?chat_id="},
		{"target without a query", "im.message.created_v1", "im.message"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if key, ok := ReverseResolve(c.eventType, c.targetResource); ok {
				t.Errorf("expected unavailable (ok=false), got (%q, true)", key)
			}
		})
	}
}
