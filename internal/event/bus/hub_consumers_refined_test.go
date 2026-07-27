// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"net"
	"testing"
)

// TestHub_Consumers_PopulatesRefinedFields locks Task 16 (spec §4.6):
// Hub.Consumers() must surface a refined *Conn's remote_subscription_id,
// owner identity, and the identity-gate's stale/degraded state via the
// s.(*Conn) type-assert seam (mirrors the Publish gate's own assert at
// hub.go:394/402) — none of this is on the bare Subscriber interface except
// RemoteSubscriptionID/OwnerAppID/OwnerUserOpenID, which are read straight
// off it.
func TestHub_Consumers_PopulatesRefinedFields(t *testing.T) {
	h := NewHub()
	conn, _ := net.Pipe()
	defer conn.Close()
	c := NewConn(conn, nil, "im.message.created_v1/chat-id/oc_xxx", []string{"im.message.created_v1"}, 42, "")
	c.SetRemoteSubscriptionID("sub_abc123")
	c.SetOwnerIdentity("user", "cli_app1", "ou_xxx")
	c.SetStaleIdentity()
	c.SetIdentityDegraded("bind_failed: uat_unavailable")
	h.RegisterAndIsFirst(c)

	consumers := h.Consumers()
	if len(consumers) != 1 {
		t.Fatalf("got %d consumers, want 1", len(consumers))
	}
	got := consumers[0]

	if !got.RefinedSubscription {
		t.Errorf("RefinedSubscription = false, want true (RemoteSubscriptionID is set)")
	}
	if got.RemoteSubscriptionID != "sub_abc123" {
		t.Errorf("RemoteSubscriptionID = %q, want %q", got.RemoteSubscriptionID, "sub_abc123")
	}
	if got.OwnerIdentity != "user" {
		t.Errorf("OwnerIdentity = %q, want %q", got.OwnerIdentity, "user")
	}
	if got.OwnerAppID != "cli_app1" {
		t.Errorf("OwnerAppID = %q, want %q", got.OwnerAppID, "cli_app1")
	}
	if got.OwnerUserOpenID != "ou_xxx" {
		t.Errorf("OwnerUserOpenID = %q, want %q", got.OwnerUserOpenID, "ou_xxx")
	}
	if !got.StaleIdentity {
		t.Errorf("StaleIdentity = false, want true")
	}
	if len(got.Health) != 1 || got.Health[0].Dimension != "identity" || got.Health[0].Reason != "bind_failed: uat_unavailable" {
		t.Errorf("Health = %+v, want one identity fact reason=bind_failed: uat_unavailable", got.Health)
	}
}

// TestHub_Consumers_LegacyConnFieldsStayZero is the additive-output
// regression counterpart: a plain *Conn that never calls any of the Set*
// refined setters must report every Task 16 field at its zero value —
// legacy consumer output must stay unchanged.
func TestHub_Consumers_LegacyConnFieldsStayZero(t *testing.T) {
	h := NewHub()
	conn, _ := net.Pipe()
	defer conn.Close()
	c := NewConn(conn, nil, "mail.x", []string{"mail.x"}, 1, "mail.x:alice")
	h.RegisterAndIsFirst(c)

	consumers := h.Consumers()
	if len(consumers) != 1 {
		t.Fatalf("got %d consumers, want 1", len(consumers))
	}
	got := consumers[0]

	if got.RefinedSubscription {
		t.Errorf("RefinedSubscription = true, want false for a legacy conn")
	}
	if got.RemoteSubscriptionID != "" || got.OwnerIdentity != "" || got.OwnerAppID != "" ||
		got.OwnerUserOpenID != "" || got.StaleIdentity || len(got.Health) != 0 {
		t.Errorf("Task 16 fields not all zero for a legacy conn: %+v", got)
	}
	// Pre-existing fields must be completely unaffected by this change.
	if got.PID != 1 || got.EventKey != "mail.x" || got.SubscriptionID != "mail.x:alice" {
		t.Errorf("legacy fields changed: %+v", got)
	}
}

// TestHub_Consumers_NonConnSubscriber_NewFieldsStayZero guards the
// s.(*Conn) type-assert's miss path: a Subscriber implementation that is NOT
// *Conn (every hand-rolled test fake in this package, e.g. testConn/
// alwaysFailSubscriber/raceSubscriber) must never panic Consumers() and must
// leave the Conn-only fields (OwnerIdentity/StaleIdentity/DegradedReason) at
// their zero value — only the fields already on the bare Subscriber
// interface (RemoteSubscriptionID/OwnerAppID/OwnerUserOpenID) can ever be
// non-zero for such a fake.
func TestHub_Consumers_NonConnSubscriber_NewFieldsStayZero(t *testing.T) {
	h := NewHub()
	s := newRefinedTestConn("im.msg", []string{"im.message.receive_v1"}, "sub_xyz")
	h.RegisterAndIsFirst(s)

	consumers := h.Consumers()
	if len(consumers) != 1 {
		t.Fatalf("got %d consumers, want 1", len(consumers))
	}
	got := consumers[0]

	// RemoteSubscriptionID/RefinedSubscription DO flow through (they're on
	// the bare Subscriber interface, read unconditionally).
	if !got.RefinedSubscription || got.RemoteSubscriptionID != "sub_xyz" {
		t.Errorf("interface-level fields did not flow through for a non-*Conn subscriber: %+v", got)
	}
	// But the *Conn-only fields must stay zero: the type-assert misses.
	if got.OwnerIdentity != "" || got.StaleIdentity || len(got.Health) != 0 {
		t.Errorf("*Conn-only fields leaked non-zero for a non-*Conn subscriber: %+v", got)
	}
}
