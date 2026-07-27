// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package session

import "testing"

// Ready is the real terminal readiness for the identity type, NOT merely
// "admitted": a user consumer is Ready only after Bound, an encrypted consumer
// only after KeyReady.
func TestConsumer_Ready_ByIdentityType(t *testing.T) {
	t.Run("bot plaintext: Ready once accepted", func(t *testing.T) {
		c := NewConsumer("", false)
		if c.Ready() {
			t.Fatal("not Ready before accepted")
		}
		c.MarkAccepted()
		if !c.Ready() {
			t.Fatal("bot plaintext must be Ready once accepted (no bind, no key)")
		}
	})

	t.Run("user plaintext: Accepted != Ready until Bound", func(t *testing.T) {
		c := NewConsumer("ou_alice", false)
		if !c.RequiresBind() {
			t.Fatal("a user consumer must RequireBind")
		}
		c.MarkAccepted()
		if c.Ready() {
			t.Fatal("a user consumer must NOT be Ready before BindUser (Accepted != Ready)")
		}
		c.MarkBound()
		if !c.Ready() {
			t.Fatal("a user consumer must be Ready once Bound")
		}
	})

	t.Run("user encrypted: needs both Bound and KeyReady", func(t *testing.T) {
		c := NewConsumer("ou_alice", true)
		if !c.RequiresKey() {
			t.Fatal("an encrypted consumer must RequireKey")
		}
		c.MarkAccepted()
		c.MarkKeyReady()
		if c.Ready() {
			t.Fatal("still not Ready: KeyReady alone is not enough for a user consumer")
		}
		c.MarkBound()
		if !c.Ready() {
			t.Fatal("Ready once both Bound and KeyReady")
		}
	})

	t.Run("bot never requires bind", func(t *testing.T) {
		c := NewConsumer("", false)
		if c.RequiresBind() {
			t.Fatal("a bot/legacy consumer must never RequireBind")
		}
	})
}

func TestConsumer_State_NeverClaimsUnearnedTerminal(t *testing.T) {
	c := NewConsumer("ou_alice", false)
	c.MarkAccepted()
	if got := c.State(); got == StateRouteReady {
		t.Fatalf("State() = %v, must not be RouteReady before Bound", got)
	}
	c.MarkBound()
	if got := c.State(); got != StateRouteReady {
		t.Fatalf("State() = %v, want RouteReady after Bound", got)
	}
}
