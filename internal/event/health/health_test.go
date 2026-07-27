// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package health

import (
	"sync"
	"testing"
	"time"
)

func TestFacts_PerDimensionIsolation_SetOneLeavesOthers(t *testing.T) {
	h := New()
	h.Degrade(Subscription, "remote_subscription_deleted")
	h.Degrade(Decryption, "decrypt_failed")

	// Writing Identity must NOT disturb Subscription or Decryption.
	h.Degrade(Identity, "current_identity_unresolved")

	if got := h.Reason(Subscription); got != "remote_subscription_deleted" {
		t.Errorf("Subscription reason = %q, want unchanged remote_subscription_deleted", got)
	}
	if got := h.Reason(Decryption); got != "decrypt_failed" {
		t.Errorf("Decryption reason = %q, want unchanged decrypt_failed", got)
	}
	if got := h.Reason(Identity); got != "current_identity_unresolved" {
		t.Errorf("Identity reason = %q, want current_identity_unresolved", got)
	}
}

// The core anti-regression: clearing ONE dimension (as a successful BindUser
// clears Identity) must leave Subscription and Decryption untouched — the exact
// bug the old shared slot caused.
func TestFacts_ClearOneDimension_LeavesOthers(t *testing.T) {
	h := New()
	h.Degrade(Identity, "bind_failed: bind_api_error")
	h.Degrade(Subscription, "remote_subscription_expired")
	h.Degrade(Decryption, "decrypt_failed")

	h.Clear(Identity)

	if !h.Get(Identity).OK() {
		t.Errorf("Identity should be healthy after Clear, got %q", h.Reason(Identity))
	}
	if got := h.Reason(Subscription); got != "remote_subscription_expired" {
		t.Errorf("Subscription reason = %q, want remote_subscription_expired (Clear(Identity) must not touch it)", got)
	}
	if got := h.Reason(Decryption); got != "decrypt_failed" {
		t.Errorf("Decryption reason = %q, want decrypt_failed (Clear(Identity) must not touch it)", got)
	}
}

func TestFacts_Snapshot_ReturnsAllNonHealthyInDimensionOrder(t *testing.T) {
	h := New()
	// Set out of order; Snapshot must still return in Dimension order.
	h.Degrade(Decryption, "decrypt_failed")
	h.Degrade(Identity, "stale_identity")

	snap := h.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("Snapshot len = %d, want 2 (two independent facts at once)", len(snap))
	}
	if snap[0].Dimension != Identity || snap[1].Dimension != Decryption {
		t.Errorf("Snapshot order = [%v,%v], want [identity, decryption]", snap[0].Dimension, snap[1].Dimension)
	}
	if snap[0].Reason != "stale_identity" || snap[1].Reason != "decrypt_failed" {
		t.Errorf("Snapshot reasons = [%q,%q]", snap[0].Reason, snap[1].Reason)
	}
}

func TestFacts_HealthyByDefault(t *testing.T) {
	h := New()
	if h.Any() {
		t.Error("a fresh Facts must be all-healthy")
	}
	if snap := h.Snapshot(); snap != nil {
		t.Errorf("healthy Snapshot = %+v, want nil", snap)
	}
	if !h.Get(Identity).OK() {
		t.Error("Identity should be OK by default")
	}
}

func TestFacts_Set_StampsWhenWhenZero(t *testing.T) {
	h := New()
	h.Degrade(Subscription, "remote_subscription_suspended")
	if h.Get(Subscription).When.IsZero() {
		t.Error("Set must stamp When on a non-empty fact")
	}
	// A caller-provided When is preserved.
	fixed := time.Unix(1700000000, 0)
	h.Set(Source, Fact{Reason: "x", When: fixed})
	if !h.Get(Source).When.Equal(fixed) {
		t.Errorf("Set must preserve a caller-provided When, got %v", h.Get(Source).When)
	}
}

func TestSeverity_String(t *testing.T) {
	if SeverityAdvisory.String() != "advisory" || SeverityDegraded.String() != "degraded" {
		t.Errorf("Severity strings wrong: %q %q", SeverityAdvisory, SeverityDegraded)
	}
}

func TestDimension_String(t *testing.T) {
	cases := map[Dimension]string{
		Identity: "identity", Subscription: "subscription", Decryption: "decryption",
		Source: "source", Delivery: "delivery",
	}
	for d, want := range cases {
		if got := d.String(); got != want {
			t.Errorf("Dimension(%d).String() = %q, want %q", d, got, want)
		}
	}
}

// Run with -race: several goroutines write DIFFERENT dimensions and read
// concurrently, mirroring production's multi-goroutine mutation.
func TestFacts_ConcurrentPerDimension_Race(t *testing.T) {
	h := New()
	var wg sync.WaitGroup
	deadline := time.Now().Add(300 * time.Millisecond)
	dims := []Dimension{Identity, Subscription, Decryption, Source, Delivery}
	for _, d := range dims {
		wg.Add(1)
		go func(d Dimension) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				h.Degrade(d, "x")
				_ = h.Get(d)
				_ = h.Snapshot()
				h.Clear(d)
			}
		}(d)
	}
	wg.Wait()
}
