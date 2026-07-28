// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"net"
	"testing"

	"github.com/larksuite/cli/internal/event/bus/lifecycle"
)

// TestHub_ConnsByRemoteSubscriptionID_MatchesAndFilters locks Task 17's
// consumer-lookup seam (mirrors userConns's shape): every *Conn bound to the
// given remote_subscription_id is returned, others are excluded.
func TestHub_ConnsByRemoteSubscriptionID_MatchesAndFilters(t *testing.T) {
	h := NewHub()

	conn1, _ := net.Pipe()
	defer conn1.Close()
	c1 := NewConn(conn1, nil, "im.msg/chat-id/oc_1", []string{"im.message.receive_v1"}, 1, "")
	c1.SetRemoteSubscriptionID("sub-1")
	h.RegisterAndIsFirst(c1)

	conn2, _ := net.Pipe()
	defer conn2.Close()
	c2 := NewConn(conn2, nil, "im.msg/chat-id/oc_2", []string{"im.message.receive_v1"}, 2, "")
	c2.SetRemoteSubscriptionID("sub-1")
	h.RegisterAndIsFirst(c2)

	conn3, _ := net.Pipe()
	defer conn3.Close()
	c3 := NewConn(conn3, nil, "im.msg/chat-id/oc_3", []string{"im.message.receive_v1"}, 3, "")
	c3.SetRemoteSubscriptionID("sub-2")
	h.RegisterAndIsFirst(c3)

	got := h.connsByRemoteSubscriptionID("sub-1")
	if len(got) != 2 {
		t.Fatalf("connsByRemoteSubscriptionID(sub-1) returned %d conns, want 2", len(got))
	}
	seen := map[int]bool{}
	for _, c := range got {
		seen[c.PID()] = true
	}
	if !seen[1] || !seen[2] {
		t.Errorf("expected PIDs 1 and 2, got %+v", got)
	}

	got2 := h.connsByRemoteSubscriptionID("sub-2")
	if len(got2) != 1 || got2[0].PID() != 3 {
		t.Errorf("connsByRemoteSubscriptionID(sub-2) = %+v, want exactly PID 3", got2)
	}

	if got3 := h.connsByRemoteSubscriptionID("sub-missing"); len(got3) != 0 {
		t.Errorf("connsByRemoteSubscriptionID(sub-missing) = %+v, want empty", got3)
	}
}

// TestHub_ConnsByRemoteSubscriptionID_EmptyIDReturnsEmpty: "" is never a
// valid remote_subscription_id to look up (there is no "legacy bucket" in
// this index) -- a legacy Conn (RemoteSubscriptionID()=="") must never match.
func TestHub_ConnsByRemoteSubscriptionID_EmptyIDReturnsEmpty(t *testing.T) {
	h := NewHub()
	conn, _ := net.Pipe()
	defer conn.Close()
	c := NewConn(conn, nil, "im.msg", []string{"im.message.receive_v1"}, 1, "")
	h.RegisterAndIsFirst(c)

	if got := h.connsByRemoteSubscriptionID(""); len(got) != 0 {
		t.Errorf("connsByRemoteSubscriptionID(\"\") = %+v, want empty", got)
	}
}

// TestHub_ConnsByRemoteSubscriptionID_SkipsNonConnSubscriber mirrors
// TestHub_Consumers_NonConnSubscriber_NewFieldsStayZero's rationale: this
// lookup only inspects *Conn (it needs to mutate Conn-only state), so a bare
// Subscriber fake must never match even if it reports a remote_subscription_id.
func TestHub_ConnsByRemoteSubscriptionID_SkipsNonConnSubscriber(t *testing.T) {
	h := NewHub()
	s := newRefinedTestConn("im.msg", []string{"im.message.receive_v1"}, "sub-xyz")
	h.RegisterAndIsFirst(s)

	if got := h.connsByRemoteSubscriptionID("sub-xyz"); len(got) != 0 {
		t.Errorf("connsByRemoteSubscriptionID must only match *Conn, got %+v for a bare Subscriber fake", got)
	}
}

// TestHub_Consumers_PopulatesLifecycleFields locks Task 17's extension of
// Hub.Consumers(): ConsumerInfo.LastLifecycleEvent/RemoteState (fields added
// by Task 16, left unpopulated) must now be read off the matching *Conn via
// the same s.(*Conn) type-assert seam the Publish gate/Task16 fields use.
func TestHub_Consumers_PopulatesLifecycleFields(t *testing.T) {
	h := NewHub()
	conn, _ := net.Pipe()
	defer conn.Close()
	c := NewConn(conn, nil, "im.msg/chat-id/oc_1", []string{"im.message.receive_v1"}, 1, "")
	c.SetRemoteSubscriptionID("sub-1")
	c.SetLifecycleSummary("event.subscription.suspended_v1", "evt-1", "suspended")
	h.RegisterAndIsFirst(c)

	consumers := h.Consumers()
	if len(consumers) != 1 {
		t.Fatalf("got %d consumers, want 1", len(consumers))
	}
	got := consumers[0]
	if got.LastLifecycleEvent != "event.subscription.suspended_v1" {
		t.Errorf("LastLifecycleEvent = %q, want %q", got.LastLifecycleEvent, "event.subscription.suspended_v1")
	}
	if got.RemoteState != "suspended" {
		t.Errorf("RemoteState = %q, want %q", got.RemoteState, "suspended")
	}
}

// TestHub_Consumers_LifecycleFieldsStayZero_WhenUnset is the additive-output
// regression counterpart (mirrors TestHub_Consumers_LegacyConnFieldsStayZero):
// a Conn that never observed a lifecycle event reports both fields at "".
func TestHub_Consumers_LifecycleFieldsStayZero_WhenUnset(t *testing.T) {
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
	if got.LastLifecycleEvent != "" || got.RemoteState != "" {
		t.Errorf("lifecycle fields not zero for a conn that never observed a lifecycle event: %+v", got)
	}
}

// --- Task 18: action-state fields (spec §5.5) ------------------------------

// TestHub_Consumers_PopulatesActionFields locks Task 18's extension of
// Hub.Consumers(): ConsumerInfo.{SuspensionReason,LastAction,
// LastActionError,NextAction} must be read off the matching *Conn, mirroring
// how Task 17 already does this for LastLifecycleEvent/RemoteState.
func TestHub_Consumers_PopulatesActionFields(t *testing.T) {
	h := NewHub()
	conn, _ := net.Pipe()
	defer conn.Close()
	c := NewConn(conn, nil, "im.msg/chat-id/oc_1", []string{"im.message.receive_v1"}, 1, "")
	c.SetRemoteSubscriptionID("sub-1")
	c.SetSuspensionReason("authority_revoked")
	c.SetLastAction("reactivate")
	c.SetLastActionError("missing_scopes")
	c.SetSubscriptionDegraded(lifecycle.ReasonRemoteSubscriptionSuspended)
	c.SetSubscriptionNextAction(lifecycle.NextActionReactivate)
	h.RegisterAndIsFirst(c)

	consumers := h.Consumers()
	if len(consumers) != 1 {
		t.Fatalf("got %d consumers, want 1", len(consumers))
	}
	got := consumers[0]
	if got.SuspensionReason != "authority_revoked" {
		t.Errorf("SuspensionReason = %q, want %q", got.SuspensionReason, "authority_revoked")
	}
	if got.LastAction != "reactivate" {
		t.Errorf("LastAction = %q, want %q", got.LastAction, "reactivate")
	}
	if got.LastActionError != "missing_scopes" {
		t.Errorf("LastActionError = %q, want %q", got.LastActionError, "missing_scopes")
	}
	if got.NextAction != lifecycle.NextActionReactivate {
		t.Errorf("NextAction = %q, want %q", got.NextAction, lifecycle.NextActionReactivate)
	}
}

// TestHub_Consumers_ActionFieldsStayZero_WhenUnset is the additive-output
// regression counterpart: a Conn that never had a lifecycle action recorded
// against it reports all four new fields at "".
func TestHub_Consumers_ActionFieldsStayZero_WhenUnset(t *testing.T) {
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
	if got.SuspensionReason != "" || got.LastAction != "" || got.LastActionError != "" || got.NextAction != "" {
		t.Errorf("action fields not zero for a conn with no recorded lifecycle action: %+v", got)
	}
}
