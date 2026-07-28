// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package session

import "testing"

// Ready is the real terminal readiness for the identity type, NOT merely
// "admitted": a user consumer is Ready only after Bound, an encrypted consumer
// only after KeyReady.
func TestConsumer_Ready_ByIdentityType(t *testing.T) {
	t.Run("no requirements: Ready once accepted AND source-ready", func(t *testing.T) {
		c := NewConsumer(false, false)
		if c.Ready() {
			t.Fatal("not Ready before accepted")
		}
		c.MarkAccepted()
		if c.Ready() {
			t.Fatal("even a no-requirement consumer must NOT be Ready before the WS source is up (source-ready gates every identity)")
		}
		c.MarkSourceReady()
		if !c.Ready() {
			t.Fatal("a consumer with no bind/key requirement must be Ready once accepted and source-ready")
		}
	})

	t.Run("requires bind: Accepted != Ready until Bound (and source-ready)", func(t *testing.T) {
		c := NewConsumer(true, false)
		if !c.RequiresBind() {
			t.Fatal("must RequireBind")
		}
		c.MarkAccepted()
		c.MarkSourceReady()
		if c.Ready() {
			t.Fatal("a bind-requiring consumer must NOT be Ready before BindUser (Accepted != Ready)")
		}
		c.MarkBound()
		if !c.Ready() {
			t.Fatal("Ready once Bound and source-ready")
		}
	})

	t.Run("requires bind and key: needs both (and source-ready)", func(t *testing.T) {
		c := NewConsumer(true, true)
		if !c.RequiresKey() {
			t.Fatal("must RequireKey")
		}
		c.MarkAccepted()
		c.MarkSourceReady()
		c.MarkKeyReady()
		if c.Ready() {
			t.Fatal("still not Ready: KeyReady alone is not enough for a bind-requiring consumer")
		}
		c.MarkBound()
		if !c.Ready() {
			t.Fatal("Ready once both Bound and KeyReady (and source-ready)")
		}
	})

	t.Run("no bind requirement", func(t *testing.T) {
		c := NewConsumer(false, false)
		if c.RequiresBind() {
			t.Fatal("must not RequireBind")
		}
	})
}

func TestConsumer_State_NeverClaimsUnearnedTerminal(t *testing.T) {
	c := NewConsumer(true, false)
	c.MarkAccepted()
	c.MarkSourceReady()
	if got := c.State(); got == StateRouteReady {
		t.Fatalf("State() = %v, must not be RouteReady before Bound", got)
	}
	c.MarkBound()
	if got := c.State(); got != StateRouteReady {
		t.Fatalf("State() = %v, want RouteReady after Bound", got)
	}
}
