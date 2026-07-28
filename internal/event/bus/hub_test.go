// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/protocol"
	"github.com/larksuite/cli/internal/event/session"
)

func TestHub_Subscribe(t *testing.T) {
	h := NewHub()
	c := newTestConn("mail.user_mailbox.event.message_received_v1", []string{"mail.event.v1"})
	h.RegisterAndIsFirst(c)

	if h.ConnCount() != 1 {
		t.Errorf("expected 1 conn, got %d", h.ConnCount())
	}
}

func TestHub_Publish_RoutesToSubscriber(t *testing.T) {
	h := NewHub()
	c := newTestConn("im.msg", []string{"im.message.receive_v1"})
	h.RegisterAndIsFirst(c)

	raw := &event.RawEvent{
		EventID:   "evt-1",
		EventType: "im.message.receive_v1",
		Payload:   json.RawMessage(`{}`),
	}
	h.Publish(raw)

	select {
	case msg := <-c.sendCh:
		evt, ok := msg.(*protocol.Event)
		if !ok {
			t.Fatalf("expected *Event, got %T", msg)
		}
		if evt.EventType != "im.message.receive_v1" {
			t.Errorf("got event_type %q", evt.EventType)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for event")
	}
}

func TestHub_Publish_SkipsUnmatchedSubscriber(t *testing.T) {
	h := NewHub()
	c := newTestConn("mail.new", []string{"mail.event.v1"})
	h.RegisterAndIsFirst(c)

	raw := &event.RawEvent{
		EventID:   "evt-1",
		EventType: "im.message.receive_v1",
		Payload:   json.RawMessage(`{}`),
	}
	h.Publish(raw)

	select {
	case <-c.sendCh:
		t.Fatal("should not receive unmatched event")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestHub_Publish_NonBlocking(t *testing.T) {
	h := NewHub()
	c := newTestConn("im", []string{"im.message.receive_v1"})
	c.sendCh = make(chan interface{}, 1)
	h.RegisterAndIsFirst(c)

	c.sendCh <- &protocol.Event{}

	done := make(chan struct{})
	go func() {
		raw := &event.RawEvent{
			EventType: "im.message.receive_v1",
			Payload:   json.RawMessage(`{}`),
		}
		h.Publish(raw)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Publish blocked on full channel")
	}
}

// recvEvent drains ch with a bounded wait, reporting whether ANY message
// arrived (msg!=nil) and, if so, whether it decoded as *protocol.Event.
func recvEvent(ch chan interface{}, timeout time.Duration) (evt *protocol.Event, isEvent bool, msg interface{}) {
	select {
	case msg = <-ch:
		evt, isEvent = msg.(*protocol.Event)
		return evt, isEvent, msg
	case <-time.After(timeout):
		return nil, false, nil
	}
}

// mustReceiveEvent asserts a *protocol.Event arrives on ch within 100ms.
func mustReceiveEvent(t *testing.T, ch chan interface{}, label string) *protocol.Event {
	t.Helper()
	evt, isEvent, msg := recvEvent(ch, 100*time.Millisecond)
	if msg == nil {
		t.Fatalf("%s: timed out waiting for event", label)
	}
	if !isEvent {
		t.Fatalf("%s: expected *protocol.Event, got %T", label, msg)
	}
	return evt
}

// mustNotReceive asserts NOTHING arrives on ch within 50ms.
func mustNotReceive(t *testing.T, ch chan interface{}, label string) {
	t.Helper()
	if _, _, msg := recvEvent(ch, 50*time.Millisecond); msg != nil {
		t.Fatalf("%s: should not have received a message, got %#v", label, msg)
	}
}

// --- Task 13: Hub dual-index routing + split dedup (spec §4.3/§11.1) ---

// Four-quadrant routing matrix: {with, without} remote_subscription_id ×
// {legacy, refined} consumer. Refined consumers are matched ONLY by
// remote_subscription_id equality (never by guessing from event_type alone);
// legacy consumers keep today's event_type matching regardless of whether the
// event happens to carry a remote_subscription_id.
func TestHub_Publish_FourQuadrantRouting(t *testing.T) {
	h := NewHub()
	legacy := newTestConn("im.msg", []string{"im.message.receive_v1"})
	refinedR1 := newRefinedTestConn("im.msg/chat-id/oc_1", []string{"im.message.receive_v1"}, "R1")
	refinedR2 := newRefinedTestConn("im.msg/chat-id/oc_2", []string{"im.message.receive_v1"}, "R2")
	h.RegisterAndIsFirst(legacy)
	h.RegisterAndIsFirst(refinedR1)
	h.RegisterAndIsFirst(refinedR2)

	// Quadrant A: event carries remote_subscription_id="R1" — the matching
	// refined consumer (R1) gets it, R2 (a DIFFERENT remote subscription) does
	// NOT; legacy still gets it via event_type-compat delivery.
	h.Publish(&event.RawEvent{
		EventID:              "evt-with-remote",
		EventType:            "im.message.receive_v1",
		RemoteSubscriptionID: "R1",
		// A refined-native event always carries its full context; the refined
		// consumers here own "app" authority (test default), so the event's
		// cross-checked context must agree for delivery.
		Resource:  "im.message?chat_id=oc_1",
		Authority: "app",
		Payload:   json.RawMessage(`{}`),
	})

	mustReceiveEvent(t, legacy.sendCh, "legacy consumer (event carries remote_subscription_id, routed by event_type)")
	mustReceiveEvent(t, refinedR1.sendCh, "refined R1 consumer (matching remote_subscription_id)")
	mustNotReceive(t, refinedR2.sendCh, "refined R2 consumer (non-matching remote_subscription_id must NOT receive)")

	// Quadrant B: event carries NO remote_subscription_id — legacy still gets
	// it by event_type; NEITHER refined consumer gets it (spec: "不投，不猜
	// 资源" — never guess which resource an unqualified event belongs to).
	h.Publish(&event.RawEvent{
		EventID:   "evt-no-remote",
		EventType: "im.message.receive_v1",
		Payload:   json.RawMessage(`{}`),
	})

	mustReceiveEvent(t, legacy.sendCh, "legacy consumer (event without remote_subscription_id, by event_type)")
	mustNotReceive(t, refinedR1.sendCh, "refined R1 consumer (event has no remote_subscription_id — must not guess)")
	mustNotReceive(t, refinedR2.sendCh, "refined R2 consumer (event has no remote_subscription_id — must not guess)")
}

// Refined events additionally set v2 fields on the fanned-out message
// (RemoteSubscriptionID/TargetResource/Authority/SubscriptionEventID),
// normalized from RawEvent's Resource/Authority/SubscriptionEventID (spec
// §4.3; note the intentional Resource -> TargetResource name change).
func TestHub_Publish_SetsV2FieldsForRefinedEvent(t *testing.T) {
	h := NewHub()
	refinedR1 := newRefinedTestConn("im.msg/chat-id/oc_1", []string{"im.message.receive_v1"}, "R1")
	// Give this consumer an owner matching the raw event's own Authority
	// below ("user:ou_abc123"): Publish's refined cross-check compares them,
	// and this test's subject is v2 field mapping, not that cross-check.
	refinedR1.ownerUserOpenID = "ou_abc123"
	h.RegisterAndIsFirst(refinedR1)

	h.Publish(&event.RawEvent{
		EventID:              "evt-1",
		EventType:            "im.message.receive_v1",
		RemoteSubscriptionID: "R1",
		Resource:             "im.message?chat_id=oc_1",
		Authority:            "user:ou_abc123",
		SubscriptionEventID:  "sub-evt-1",
		Payload:              json.RawMessage(`{}`),
	})

	evt := mustReceiveEvent(t, refinedR1.sendCh, "refined R1 consumer")
	if evt.RemoteSubscriptionID != "R1" {
		t.Errorf("RemoteSubscriptionID = %q, want %q", evt.RemoteSubscriptionID, "R1")
	}
	if evt.TargetResource != "im.message?chat_id=oc_1" {
		t.Errorf("TargetResource = %q, want %q (from raw.Resource)", evt.TargetResource, "im.message?chat_id=oc_1")
	}
	if evt.Authority != "user:ou_abc123" {
		t.Errorf("Authority = %q, want %q", evt.Authority, "user:ou_abc123")
	}
	if evt.SubscriptionEventID != "sub-evt-1" {
		t.Errorf("SubscriptionEventID = %q, want %q", evt.SubscriptionEventID, "sub-evt-1")
	}
}

// --- refined cross-check: additive defense-in-depth on top of the
// remote_subscription_id match above, never a replacement for it ---

// TestHub_Publish_RefinedCrossCheck_EventTypeMismatch_DroppedAndCounted locks
// the strongest signal: even though remote_subscription_id matched, an
// event_type that disagrees with what this consumer registered for is a
// genuine anomaly — drop it, and count it so it's observable.
func TestHub_Publish_RefinedCrossCheck_EventTypeMismatch_DroppedAndCounted(t *testing.T) {
	h := NewHub()
	refinedR1 := newRefinedTestConn("im.msg/chat-id/oc_1", []string{"im.message.receive_v1"}, "R1")
	h.RegisterAndIsFirst(refinedR1)

	h.Publish(&event.RawEvent{
		EventID:              "evt-1",
		EventType:            "im.message.OTHER_v1", // disagrees with refinedR1's own EventTypes()
		RemoteSubscriptionID: "R1",
		Payload:              json.RawMessage(`{}`),
	})

	mustNotReceive(t, refinedR1.sendCh, "refined R1 consumer (event_type cross-check mismatch must drop)")
	if got := h.CrossCheckDroppedCount(); got != 1 {
		t.Errorf("CrossCheckDroppedCount() = %d, want 1", got)
	}
}

// TestHub_Publish_RefinedCrossCheck_TargetResourceMismatch_Dropped locks the
// target_resource dimension: TargetResource() is *Conn-only (this consumer's
// own stored listening intent from HelloV2), so a real *Conn is needed here.
func TestHub_Publish_RefinedCrossCheck_TargetResourceMismatch_Dropped(t *testing.T) {
	h := NewHub()
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	c := NewConn(server, nil, "im.msg/chat-id/oc_1", []string{"im.message.receive_v1"}, 1, "")
	c.SetRemoteSubscriptionID("R1")
	c.SetListenIntent("im.message?chat_id=oc_1", false, nil)
	c.sendCh = make(chan interface{}, 1)
	h.RegisterAndIsFirst(c)

	h.Publish(&event.RawEvent{
		EventID:              "evt-1",
		EventType:            "im.message.receive_v1",
		RemoteSubscriptionID: "R1",
		Resource:             "im.message?chat_id=oc_DIFFERENT", // disagrees with c's own TargetResource()
		Payload:              json.RawMessage(`{}`),
	})

	mustNotReceive(t, c.sendCh, "conn (target_resource cross-check mismatch must drop)")
	if got := h.CrossCheckDroppedCount(); got != 1 {
		t.Errorf("CrossCheckDroppedCount() = %d, want 1", got)
	}
}

// TestHub_Publish_RefinedCrossCheck_AuthorityMismatch_Dropped locks the
// authority dimension: raw.Authority ("user:<open_id>"/"app") must match this
// consumer's OWN owner identity (lifecycle.AuthorityMatchesOwner) — the same
// vocabulary/comparison the updated_v1 lifecycle compatibility check uses.
func TestHub_Publish_RefinedCrossCheck_AuthorityMismatch_Dropped(t *testing.T) {
	h := NewHub()
	refinedR1 := newRefinedTestConn("im.msg/chat-id/oc_1", []string{"im.message.receive_v1"}, "R1")
	refinedR1.ownerUserOpenID = "ou_the_real_owner"
	h.RegisterAndIsFirst(refinedR1)

	h.Publish(&event.RawEvent{
		EventID:              "evt-1",
		EventType:            "im.message.receive_v1",
		RemoteSubscriptionID: "R1",
		// target_resource present so the check reaches the authority dimension
		// (this mock is not a *Conn, so its value is only presence-checked).
		Resource:  "im.message?chat_id=oc_1",
		Authority: "user:ou_SOMEONE_ELSE", // disagrees with refinedR1's own owner
		Payload:   json.RawMessage(`{}`),
	})

	mustNotReceive(t, refinedR1.sendCh, "refined R1 consumer (authority cross-check mismatch must drop)")
	if got := h.CrossCheckDroppedCount(); got != 1 {
		t.Errorf("CrossCheckDroppedCount() = %d, want 1", got)
	}
}

// TestHub_Publish_RefinedCrossCheck_AllDimensionsMatch_Delivered proves the
// cross-check is not merely permissive by omission: it actively compares all
// 3 dimensions and still delivers when every one of them agrees.
func TestHub_Publish_RefinedCrossCheck_AllDimensionsMatch_Delivered(t *testing.T) {
	h := NewHub()
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	c := NewConn(server, nil, "im.msg/chat-id/oc_1", []string{"im.message.receive_v1"}, 1, "")
	c.SetRemoteSubscriptionID("R1")
	c.SetListenIntent("im.message?chat_id=oc_1", false, nil)
	c.SetOwnerIdentity("user", "cli_app", "ou_abc123")
	c.sendCh = make(chan interface{}, 1)
	h.RegisterAndIsFirst(c)

	h.Publish(&event.RawEvent{
		EventID:              "evt-1",
		EventType:            "im.message.receive_v1",
		RemoteSubscriptionID: "R1",
		Resource:             "im.message?chat_id=oc_1",
		Authority:            "user:ou_abc123",
		Payload:              json.RawMessage(`{}`),
	})

	mustReceiveEvent(t, c.sendCh, "conn (every dimension agrees)")
	if got := h.CrossCheckDroppedCount(); got != 0 {
		t.Errorf("CrossCheckDroppedCount() = %d, want 0", got)
	}
}

// TestHub_Publish_RefinedCrossCheck_MissingTargetResource_Dropped locks the
// fail-closed rule: a refined event matched by remote_subscription_id but
// carrying NO target_resource is an anomaly (a legit refined push always
// includes it) — drop it, and count it, rather than deliver blind. The
// consumer's own target_resource/owner are fully populated, proving the drop
// is the event's missing context, not a consumer misconfiguration.
func TestHub_Publish_RefinedCrossCheck_MissingTargetResource_Dropped(t *testing.T) {
	h := NewHub()
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	c := NewConn(server, nil, "im.msg/chat-id/oc_1", []string{"im.message.receive_v1"}, 1, "")
	c.SetRemoteSubscriptionID("R1")
	c.SetListenIntent("im.message?chat_id=oc_1", false, nil)
	c.SetOwnerIdentity("user", "cli_app", "ou_abc123")
	c.sendCh = make(chan interface{}, 1)
	h.RegisterAndIsFirst(c)

	h.Publish(&event.RawEvent{
		EventID:              "evt-1",
		EventType:            "im.message.receive_v1",
		RemoteSubscriptionID: "R1",
		// Resource intentionally empty; Authority present so the drop is
		// unambiguously the missing target_resource, not the authority.
		Authority: "user:ou_abc123",
		Payload:   json.RawMessage(`{}`),
	})

	mustNotReceive(t, c.sendCh, "conn (refined event without target_resource must fail closed)")
	if got := h.CrossCheckDroppedCount(); got != 1 {
		t.Errorf("CrossCheckDroppedCount() = %d, want 1", got)
	}
}

// TestHub_Publish_RefinedCrossCheck_MissingAuthority_Dropped is the authority
// half of the same fail-closed rule: full target_resource, but no authority ->
// drop + count.
func TestHub_Publish_RefinedCrossCheck_MissingAuthority_Dropped(t *testing.T) {
	h := NewHub()
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	c := NewConn(server, nil, "im.msg/chat-id/oc_1", []string{"im.message.receive_v1"}, 1, "")
	c.SetRemoteSubscriptionID("R1")
	c.SetListenIntent("im.message?chat_id=oc_1", false, nil)
	c.SetOwnerIdentity("user", "cli_app", "ou_abc123")
	c.sendCh = make(chan interface{}, 1)
	h.RegisterAndIsFirst(c)

	h.Publish(&event.RawEvent{
		EventID:              "evt-1",
		EventType:            "im.message.receive_v1",
		RemoteSubscriptionID: "R1",
		Resource:             "im.message?chat_id=oc_1",
		// Authority intentionally empty.
		Payload: json.RawMessage(`{}`),
	})

	mustNotReceive(t, c.sendCh, "conn (refined event without authority must fail closed)")
	if got := h.CrossCheckDroppedCount(); got != 1 {
		t.Errorf("CrossCheckDroppedCount() = %d, want 1", got)
	}
}

// TestHub_Publish_RefinedCrossCheck_BotPush_Delivered proves the fail-closed
// gate still delivers a legit BOT push: authority "app" matches a bot consumer
// (empty owner user), and the platform's echoed target_resource matches this
// consumer's own — even across URL-escaping differences (normalized compare).
func TestHub_Publish_RefinedCrossCheck_BotPush_Delivered(t *testing.T) {
	h := NewHub()
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	c := NewConn(server, nil, "im.msg/chat-id/oc_1", []string{"im.message.receive_v1"}, 1, "")
	c.SetRemoteSubscriptionID("R1")
	// Locally-built intent uses url.QueryEscape (space -> '+').
	c.SetListenIntent("im.message?chat_id=oc+1", false, nil)
	c.SetOwnerIdentity("bot", "cli_app", "") // bot: empty owner user
	c.sendCh = make(chan interface{}, 1)
	h.RegisterAndIsFirst(c)

	h.Publish(&event.RawEvent{
		EventID:              "evt-1",
		EventType:            "im.message.receive_v1",
		RemoteSubscriptionID: "R1",
		// Platform echoes the same selector with %20 escaping.
		Resource:  "im.message?chat_id=oc%201",
		Authority: "app",
		Payload:   json.RawMessage(`{}`),
	})

	mustReceiveEvent(t, c.sendCh, "bot consumer (authority app + normalized target_resource match)")
	if got := h.CrossCheckDroppedCount(); got != 0 {
		t.Errorf("CrossCheckDroppedCount() = %d, want 0", got)
	}
}

// TestHub_Publish_RefinedCrossCheck_MultiConsumerFanOutUnaffected proves the
// cross-check is evaluated PER-SUBSCRIBER, not as a single whole-Publish gate:
// two consumers sharing the SAME remote_subscription_id, one with a genuine
// owner mismatch and one without, must be judged independently — the
// mismatched one's drop must never affect the other's delivery.
func TestHub_Publish_RefinedCrossCheck_MultiConsumerFanOutUnaffected(t *testing.T) {
	h := NewHub()
	good := newRefinedTestConn("im.msg/chat-id/oc_1", []string{"im.message.receive_v1"}, "R1")
	good.ownerUserOpenID = "ou_abc123"
	bad := newRefinedTestConn("im.msg/chat-id/oc_1", []string{"im.message.receive_v1"}, "R1")
	bad.pid = 2
	bad.ownerUserOpenID = "ou_SOMEONE_ELSE"
	h.RegisterAndIsFirst(good)
	h.RegisterAndIsFirst(bad)

	h.Publish(&event.RawEvent{
		EventID:              "evt-1",
		EventType:            "im.message.receive_v1",
		RemoteSubscriptionID: "R1",
		// Full refined context present; the two consumers differ only on owner,
		// so the cross-check judges them independently on authority alone.
		Resource:  "im.message?chat_id=oc_1",
		Authority: "user:ou_abc123",
		Payload:   json.RawMessage(`{}`),
	})

	mustReceiveEvent(t, good.sendCh, "good consumer (owner matches — unaffected by bad's mismatch)")
	mustNotReceive(t, bad.sendCh, "bad consumer (owner mismatch must drop)")
	if got := h.CrossCheckDroppedCount(); got != 1 {
		t.Errorf("CrossCheckDroppedCount() = %d, want 1 (only the mismatched consumer)", got)
	}
}

// NOTE: the whitebox refined cross-check reason table that used to live here
// moved to internal/event/routing (TestPlan_RefinedCrossCheckReasons), where the
// decision now lives. The Hub's integration with it — that a mismatch drops the
// event AND increments CrossCheckDroppedCount — is still pinned by the
// TestHub_Publish_RefinedCrossCheck_* cases above.

// Split dedup (spec §4.3): the same event_id delivered under two DIFFERENT
// remote_subscription_id contexts (the underlying event matched two separate
// remote Subscriptions, so the source emits it as two distinct RawEvents
// sharing event_id) must reach EACH refined consumer once — dedup is
// per-domain, not a single global event_id gate — while the legacy consumer
// (whose dedup domain is unconditionally event_id) sees it only once.
func TestHub_Publish_SplitDedupAcrossRemoteSubscriptions(t *testing.T) {
	h := NewHub()
	legacy := newTestConn("im.msg", []string{"im.message.receive_v1"})
	refinedR1 := newRefinedTestConn("im.msg/chat-id/oc_1", []string{"im.message.receive_v1"}, "R1")
	refinedR2 := newRefinedTestConn("im.msg/chat-id/oc_2", []string{"im.message.receive_v1"}, "R2")
	h.RegisterAndIsFirst(legacy)
	h.RegisterAndIsFirst(refinedR1)
	h.RegisterAndIsFirst(refinedR2)

	const sharedEventID = "evt-shared-across-subs"
	h.Publish(&event.RawEvent{
		EventID:              sharedEventID,
		EventType:            "im.message.receive_v1",
		RemoteSubscriptionID: "R1",
		SubscriptionEventID:  "sub-evt-r1",
		Resource:             "im.message?chat_id=oc_1",
		Authority:            "app",
		Payload:              json.RawMessage(`{}`),
	})
	h.Publish(&event.RawEvent{
		EventID:              sharedEventID,
		EventType:            "im.message.receive_v1",
		RemoteSubscriptionID: "R2",
		SubscriptionEventID:  "sub-evt-r2",
		Resource:             "im.message?chat_id=oc_2",
		Authority:            "app",
		Payload:              json.RawMessage(`{}`),
	})

	mustReceiveEvent(t, refinedR1.sendCh, "refined R1 (own domain, first time)")
	mustReceiveEvent(t, refinedR2.sendCh, "refined R2 (own domain, first time - distinct remote_subscription_id, must NOT be swallowed by R1's dedup)")

	// legacy dedups by event_id alone: it already saw sharedEventID from the
	// first Publish, so the second Publish (same event_id, different remote
	// subscription) must NOT reach it a second time.
	first := mustReceiveEvent(t, legacy.sendCh, "legacy (first delivery)")
	if first.EventID != sharedEventID {
		t.Fatalf("legacy got event_id %q, want %q", first.EventID, sharedEventID)
	}
	mustNotReceive(t, legacy.sendCh, "legacy (must be deduped by event_id on the second, same-event_id delivery)")
}

// Redelivering the identical subscription_event_id must be deduped for that
// refined consumer, exercised through Hub.Publish itself: the refined domain is
// keyed on subscription_event_id alone (globally unique for a refined event).
func TestHub_Publish_RefinedDedupSameKeyTwice(t *testing.T) {
	h := NewHub()
	refinedR1 := newRefinedTestConn("im.msg/chat-id/oc_1", []string{"im.message.receive_v1"}, "R1")
	h.RegisterAndIsFirst(refinedR1)

	raw := &event.RawEvent{
		EventID:              "evt-1",
		EventType:            "im.message.receive_v1",
		RemoteSubscriptionID: "R1",
		SubscriptionEventID:  "sub-evt-1",
		Resource:             "im.message?chat_id=oc_1",
		Authority:            "app",
		Payload:              json.RawMessage(`{}`),
	}
	h.Publish(raw)
	h.Publish(raw) // redelivery: identical remote_subscription_id + subscription_event_id

	mustReceiveEvent(t, refinedR1.sendCh, "first delivery")
	mustNotReceive(t, refinedR1.sendCh, "redelivery with identical dedup key must be dropped")
}

// When subscription_event_id is empty the refined domain cannot build a dedup
// key — Publish must deliver EVERY time (no dedup, never silently drop) and log
// a warning, rather than collapsing distinct keyless events into one.
func TestHub_Publish_RefinedNoDedupKeyDeliversEveryTimeAndWarns(t *testing.T) {
	var logBuf bytes.Buffer
	h := NewHub()
	h.SetLogger(log.New(&logBuf, "", 0))
	refinedR1 := newRefinedTestConn("im.msg/chat-id/oc_1", []string{"im.message.receive_v1"}, "R1")
	h.RegisterAndIsFirst(refinedR1)

	raw := &event.RawEvent{
		EventType:            "im.message.receive_v1",
		RemoteSubscriptionID: "R1",
		Resource:             "im.message?chat_id=oc_1",
		Authority:            "app",
		// EventID and SubscriptionEventID both intentionally empty.
		Payload: json.RawMessage(`{}`),
	}
	h.Publish(raw)
	h.Publish(raw)

	mustReceiveEvent(t, refinedR1.sendCh, "first delivery (no dedup key possible)")
	mustReceiveEvent(t, refinedR1.sendCh, "second delivery MUST still arrive: no dedup key means no dedup, not a drop")

	if !strings.Contains(logBuf.String(), "WARN") {
		t.Errorf("expected a WARN log for the un-dedupable refined event, got log: %q", logBuf.String())
	}
}

// --- No-recipient events stay re-processable. The atomic dedup claim is gated
// on the domain having an eligible endpoint, so an event that matched no one
// claims nothing and a later genuine consumer still receives it. (An event whose
// delivery WAS attempted but not accepted is a different case: the claim is
// already taken — see TestHub_Publish_UnacceptedDeliveryStillClaims.) ---

// A legacy event published while NO consumer matches has no eligible endpoint,
// so nothing is claimed for its event_id: a consumer that registers afterward
// and receives the SAME event_id must still get it.
func TestHub_Publish_NoRecipientLegacyEventStaysReprocessable(t *testing.T) {
	h := NewHub()
	const eventID = "evt-no-recipient"

	// No consumer for this event type yet — the event reaches nobody.
	h.Publish(&event.RawEvent{EventID: eventID, EventType: "im.message.receive_v1", Payload: json.RawMessage(`{}`)})

	// A consumer registers afterward and the SAME event_id is redelivered.
	c := newTestConn("im.msg", []string{"im.message.receive_v1"})
	h.RegisterAndIsFirst(c)
	h.Publish(&event.RawEvent{EventID: eventID, EventType: "im.message.receive_v1", Payload: json.RawMessage(`{}`)})

	mustReceiveEvent(t, c.sendCh, "consumer must receive a redelivered event whose first publish had NO eligible endpoint, so nothing was claimed")
}

// The refined half of the same rule: a refined event published while no consumer
// is bound to its remote_subscription_id has no eligible refined endpoint, so
// its subscription_event_id is never claimed.
func TestHub_Publish_NoRecipientRefinedEventStaysReprocessable(t *testing.T) {
	h := NewHub()
	other := newRefinedTestConn("im.msg/chat-id/oc_2", []string{"im.message.receive_v1"}, "R2")
	h.RegisterAndIsFirst(other)

	raw := &event.RawEvent{
		EventID:              "evt-1",
		EventType:            "im.message.receive_v1",
		RemoteSubscriptionID: "R1",
		SubscriptionEventID:  "sub-evt-1",
		Resource:             "im.message?chat_id=oc_1",
		Authority:            "app",
		Payload:              json.RawMessage(`{}`),
	}
	// No R1 consumer yet: nothing eligible in the refined domain, so the refined
	// dedup key is never claimed.
	h.Publish(raw)

	// R1 consumer registers and the SAME refined event (same dedup key) arrives.
	r1 := newRefinedTestConn("im.msg/chat-id/oc_1", []string{"im.message.receive_v1"}, "R1")
	h.RegisterAndIsFirst(r1)
	h.Publish(raw)

	mustReceiveEvent(t, r1.sendCh, "R1 must receive a redelivered refined event whose first delivery had no R1 recipient")
	mustNotReceive(t, other.sendCh, "R2 must never receive an R1-scoped event")
}

// Atomic claim-before-deliver: the dedup key is claimed at the first delivery
// ATTEMPT, before and independent of whether any destination accepts it. So a
// same-event_id republish within the TTL is deduped even if the first attempt
// enqueued nowhere (its only consumer's queue was full). This deliberately
// supersedes the earlier "unaccepted delivery stays re-processable" behavior:
// claiming before delivery is precisely what stops two concurrent same-key
// Publishes from both delivering, and here a matched consumer DID exist — it was
// merely overwhelmed, so there is nothing a retention rule needs to protect. The
// claim is bounded by the TTL, so the id becomes re-claimable once it expires.
func TestHub_Publish_UnacceptedDeliveryStillClaims(t *testing.T) {
	h := NewHub()
	const eventID = "evt-unaccepted"

	// The only consumer can never enqueue, so the first delivery accepts nowhere.
	failing := &alwaysFailSubscriber{
		eventKey:   "im.msg",
		eventTypes: []string{"im.message.receive_v1"},
		sendCh:     make(chan interface{}, 1),
	}
	h.RegisterAndIsFirst(failing)
	h.Publish(&event.RawEvent{EventID: eventID, EventType: "im.message.receive_v1", Payload: json.RawMessage(`{}`)})
	if failing.Received() != 0 {
		t.Fatalf("precondition: always-fail subscriber should have Received 0, got %d", failing.Received())
	}

	// A working consumer registers and the SAME event_id is republished within
	// the TTL. The first attempt already claimed the key, so the republish is
	// deduped and never reaches the new consumer.
	working := newTestConn("im.msg2", []string{"im.message.receive_v1"})
	h.RegisterAndIsFirst(working)
	h.Publish(&event.RawEvent{EventID: eventID, EventType: "im.message.receive_v1", Payload: json.RawMessage(`{}`)})

	mustNotReceive(t, working.sendCh, "same event_id within the TTL is deduped: the first (unaccepted) attempt already claimed it — atomic claim-before-deliver")
}

// fan-out: multiple LOCAL consumers may share one remote_subscription_id
// (spec §4.3) — registration is unrestricted (unlike SingleConsumer legacy
// keys); each must receive the event independently with its OWN monotonic
// seq (Conn.NextSeq()), never aliasing another subscriber's Seq.
func TestHub_Publish_FanOutMultipleConsumersSameRemoteSubID(t *testing.T) {
	h := NewHub()
	connA, _ := net.Pipe()
	connB, _ := net.Pipe()
	defer connA.Close()
	defer connB.Close()

	subA := NewConn(connA, nil, "im.msg/chat-id/oc_1", []string{"im.message.receive_v1"}, 1, "im.msg/chat-id/oc_1:consumerA")
	subA.SetRemoteSubscriptionID("R1")
	subB := NewConn(connB, nil, "im.msg/chat-id/oc_1", []string{"im.message.receive_v1"}, 2, "im.msg/chat-id/oc_1:consumerB")
	subB.SetRemoteSubscriptionID("R1")

	if !h.RegisterAndIsFirst(subA) {
		t.Fatal("subA should be first for its own SubscriptionID")
	}
	if !h.RegisterAndIsFirst(subB) {
		t.Fatal("subB should ALSO be first (distinct SubscriptionID - fan-out is not exclusive)")
	}

	h.Publish(&event.RawEvent{
		EventID:              "evt-fanout",
		EventType:            "im.message.receive_v1",
		RemoteSubscriptionID: "R1",
		SubscriptionEventID:  "sub-evt-fanout",
		Resource:             "im.message?chat_id=oc_1",
		Authority:            "app",
		Payload:              json.RawMessage(`{"x":1}`),
	})

	evtA := mustReceiveEvent(t, subA.SendCh(), "consumer A")
	evtB := mustReceiveEvent(t, subB.SendCh(), "consumer B")

	if evtA.Seq != 1 {
		t.Errorf("consumer A seq = %d, want 1 (fresh per-conn counter)", evtA.Seq)
	}
	if evtB.Seq != 1 {
		t.Errorf("consumer B seq = %d, want 1 (independent per-conn counter, must not alias A's)", evtB.Seq)
	}
	if evtA.RemoteSubscriptionID != "R1" || evtB.RemoteSubscriptionID != "R1" {
		t.Errorf("both fan-out consumers should see RemoteSubscriptionID=R1: A=%q B=%q", evtA.RemoteSubscriptionID, evtB.RemoteSubscriptionID)
	}
	if evtA.SubscriptionEventID != "sub-evt-fanout" || evtB.SubscriptionEventID != "sub-evt-fanout" {
		t.Errorf("both fan-out consumers should see SubscriptionEventID propagated: A=%q B=%q", evtA.SubscriptionEventID, evtB.SubscriptionEventID)
	}
}

// --- Task 14: real-time identity gate — delivery side (spec §4.4) ---
//
// Hub.Publish's identity gate is the SECOND of the two gate points (the
// first, identity.go's onConnReady bind gate, is covered in identity_test.go):
// it catches an owner/current mismatch on every single event fan-out, not
// just at connect/reconnect time (e.g. a profile switch with no reconnect).

// owner == current: delivered, never marked stale.
func TestHub_Publish_IdentityGate_AllowsMatchingOwner(t *testing.T) {
	h := NewHub()
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	c := NewConn(server, nil, "im.msg", []string{"im.message.receive_v1"}, 1, "")
	c.SetOwnerIdentity("user", "app1", "ou_alice")
	h.RegisterAndIsFirst(c)
	h.SetCurrentResolver(func() (session.CurrentIdentity, error) {
		return session.CurrentIdentity{AppID: "app1", UserOpenID: "ou_alice"}, nil
	})

	h.Publish(&event.RawEvent{EventID: "evt-1", EventType: "im.message.receive_v1", Payload: json.RawMessage(`{}`)})

	mustReceiveEvent(t, c.SendCh(), "owner==current must be delivered")
	if c.StaleIdentity() {
		t.Error("should not be marked stale_identity when owner matches current")
	}
}

// owner != current: NOT delivered, marked stale_identity (spec §4.4's core invariant).
func TestHub_Publish_IdentityGate_DeniesMismatchedOwner(t *testing.T) {
	h := NewHub()
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	c := NewConn(server, nil, "im.msg", []string{"im.message.receive_v1"}, 1, "")
	c.SetOwnerIdentity("user", "app1", "ou_alice") // owner fixed at register
	h.RegisterAndIsFirst(c)
	h.SetCurrentResolver(func() (session.CurrentIdentity, error) {
		return session.CurrentIdentity{AppID: "app1", UserOpenID: "ou_bob"}, nil // current is now a DIFFERENT user
	})

	h.Publish(&event.RawEvent{EventID: "evt-1", EventType: "im.message.receive_v1", Payload: json.RawMessage(`{}`)})

	mustNotReceive(t, c.SendCh(), "owner!=current must NOT be delivered")
	if !c.StaleIdentity() {
		t.Error("should be marked stale_identity when owner mismatches current")
	}
}

// nil currentResolver (the zero value from NewHub()) must mean NO gating —
// every pre-Task-14 caller/test keeps exactly today's behavior.
func TestHub_Publish_IdentityGate_NilResolverIsLegacyBehavior(t *testing.T) {
	h := NewHub()
	c := newOwnedTestConn("im.msg", []string{"im.message.receive_v1"}, "app1", "ou_alice")
	h.RegisterAndIsFirst(c)

	h.Publish(&event.RawEvent{EventID: "evt-1", EventType: "im.message.receive_v1", Payload: json.RawMessage(`{}`)})

	mustReceiveEvent(t, c.sendCh, "nil currentResolver must mean NO gating (pre-Task-14/legacy behavior)")
}

// BOT consumers (OwnerUserOpenID()=="") are NEVER identity-gated (spec §4.4),
// even when a currentResolver IS configured and would mismatch a user owner.
func TestHub_Publish_IdentityGate_BotBypassesGate(t *testing.T) {
	h := NewHub()
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	c := NewConn(server, nil, "im.msg", []string{"im.message.receive_v1"}, 1, "")
	c.SetOwnerIdentity("bot", "app1", "") // bot: identity fixed, but no owning user
	h.RegisterAndIsFirst(c)
	h.SetCurrentResolver(func() (session.CurrentIdentity, error) {
		return session.CurrentIdentity{AppID: "app1", UserOpenID: "ou_alice"}, nil
	})

	h.Publish(&event.RawEvent{EventID: "evt-1", EventType: "im.message.receive_v1", Payload: json.RawMessage(`{}`)})

	mustReceiveEvent(t, c.SendCh(), "bot consumer must ALWAYS be delivered — never identity-gated")
	if c.StaleIdentity() {
		t.Error("bot consumer must never be marked stale_identity")
	}
}

// A resolveCurrent failure must fail CLOSED for user consumers (never
// deliver under an unresolved identity) while leaving bot consumers
// completely unaffected.
func TestHub_Publish_IdentityGate_ResolveCurrentErrorFailsClosedForUsersOnly(t *testing.T) {
	h := NewHub()
	userServer, userClient := net.Pipe()
	defer userServer.Close()
	defer userClient.Close()
	userConn := NewConn(userServer, nil, "im.msg", []string{"im.message.receive_v1"}, 1, "im.msg:user")
	userConn.SetOwnerIdentity("user", "app1", "ou_alice")
	h.RegisterAndIsFirst(userConn)

	botServer, botClient := net.Pipe()
	defer botServer.Close()
	defer botClient.Close()
	botConn := NewConn(botServer, nil, "im.msg", []string{"im.message.receive_v1"}, 2, "im.msg:bot")
	botConn.SetOwnerIdentity("bot", "app1", "")
	h.RegisterAndIsFirst(botConn)

	h.SetCurrentResolver(func() (session.CurrentIdentity, error) {
		return session.CurrentIdentity{}, errors.New("config.json unreadable")
	})

	h.Publish(&event.RawEvent{EventID: "evt-1", EventType: "im.message.receive_v1", Payload: json.RawMessage(`{}`)})

	mustNotReceive(t, userConn.SendCh(), "user consumer must fail CLOSED when current identity cannot be resolved")
	if got := userConn.IdentityDegradedReason(); got == "" {
		t.Error("user consumer should be marked degraded on the identity dimension when resolveCurrent errors")
	}
	mustReceiveEvent(t, botConn.SendCh(), "bot consumer must be unaffected by a resolveCurrent error")
}

// currentResolver must be resolved AT MOST ONCE per Publish call even when
// multiple user consumers match — cheap by construction, not just by luck.
func TestHub_Publish_IdentityGate_ResolvedOncePerPublishCall(t *testing.T) {
	h := NewHub()
	c1 := newOwnedTestConn("im.msg.one", []string{"im.message.receive_v1"}, "app1", "ou_alice")
	c2 := newOwnedTestConn("im.msg.two", []string{"im.message.receive_v1"}, "app1", "ou_alice")
	h.RegisterAndIsFirst(c1)
	h.RegisterAndIsFirst(c2)

	var calls int32
	h.SetCurrentResolver(func() (session.CurrentIdentity, error) {
		atomic.AddInt32(&calls, 1)
		return session.CurrentIdentity{AppID: "app1", UserOpenID: "ou_alice"}, nil
	})

	h.Publish(&event.RawEvent{EventID: "evt-shared", EventType: "im.message.receive_v1", Payload: json.RawMessage(`{}`)})

	mustReceiveEvent(t, c1.sendCh, "c1")
	mustReceiveEvent(t, c2.sendCh, "c2")

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("currentResolver call count for one Publish matching 2 user consumers = %d, want 1 (memoized once per call)", got)
	}
}

// SetCurrentResolver concurrently with Publish must not race on h.mu (both
// already share it with subscribers/subCounts) — the identity gate's
// resolver is set once at bus construction in production, but this proves
// the guard holds even under concurrent (mis)use. Run with -race.
func TestHub_SetCurrentResolver_ConcurrentWithPublish(t *testing.T) {
	h := NewHub()
	c := newOwnedTestConn("race.key", []string{"race.type"}, "app1", "ou_alice")
	h.RegisterAndIsFirst(c)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			h.SetCurrentResolver(func() (session.CurrentIdentity, error) {
				return session.CurrentIdentity{AppID: "app1", UserOpenID: "ou_alice"}, nil
			})
		}
	}()

	for i := 0; i < 500; i++ {
		h.Publish(&event.RawEvent{EventID: strconv.Itoa(i), EventType: "race.type", Payload: json.RawMessage(`{}`)})
	}
	close(stop)
	wg.Wait()
}

func TestHub_Unregister(t *testing.T) {
	h := NewHub()
	c := newTestConn("im", []string{"im.msg"})
	h.RegisterAndIsFirst(c)
	h.UnregisterAndIsLast(c)

	if h.ConnCount() != 0 {
		t.Errorf("expected 0 conns, got %d", h.ConnCount())
	}
}

func TestHub_UnregisterAndIsLast_NeverRegistered(t *testing.T) {
	h := NewHub()
	real := newTestConn("im", []string{"im.msg"})
	h.RegisterAndIsFirst(real)
	ghost := newTestConn("im", []string{"im.msg"})

	if h.UnregisterAndIsLast(ghost) {
		t.Error("ghost unregister returned true: must be false when subscriber never registered")
	}
	if got := h.EventKeyCount("im"); got != 1 {
		t.Errorf("keyCount for 'im' = %d after ghost unregister; want 1 (real still registered)", got)
	}
	if !h.UnregisterAndIsLast(real) {
		t.Error("real unregister returned false; expected true (sole subscriber)")
	}
}

func TestHub_UnregisterAndIsLast_DoubleUnregister(t *testing.T) {
	h := NewHub()
	c := newTestConn("im", []string{"im.msg"})
	h.RegisterAndIsFirst(c)

	if !h.UnregisterAndIsLast(c) {
		t.Fatal("first unregister returned false; expected true (sole subscriber)")
	}
	if h.UnregisterAndIsLast(c) {
		t.Error("second unregister returned true: duplicate unregister must report false")
	}
}

func TestHub_EventKeyCount(t *testing.T) {
	h := NewHub()
	c1 := newTestConn("mail.user_mailbox.event.message_received_v1", []string{"mail.v1"})
	c2 := newTestConn("mail.user_mailbox.event.message_received_v1", []string{"mail.v1"})
	h.RegisterAndIsFirst(c1)
	h.RegisterAndIsFirst(c2)

	if h.EventKeyCount("mail.user_mailbox.event.message_received_v1") != 2 {
		t.Errorf("expected 2, got %d", h.EventKeyCount("mail.user_mailbox.event.message_received_v1"))
	}

	h.UnregisterAndIsLast(c1)
	if h.EventKeyCount("mail.user_mailbox.event.message_received_v1") != 1 {
		t.Errorf("expected 1 after unregister, got %d", h.EventKeyCount("mail.user_mailbox.event.message_received_v1"))
	}
}

func TestHub_RegisterAndIsFirst_Concurrent(t *testing.T) {
	h := NewHub()
	const N = 200
	eventKey := "mail.user_mailbox.event.message_received_v1"

	var firstCount int32
	var wg sync.WaitGroup
	wg.Add(N)
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			<-start
			c := newTestConn(eventKey, []string{"mail.v1"})
			if h.RegisterAndIsFirst(c) {
				atomic.AddInt32(&firstCount, 1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := atomic.LoadInt32(&firstCount); got != 1 {
		t.Errorf("RegisterAndIsFirst returned true %d times across %d concurrent registrants; want exactly 1", got, N)
	}
	if got := h.EventKeyCount(eventKey); got != N {
		t.Errorf("EventKeyCount = %d, want %d", got, N)
	}
}

func TestHub_UnregisterAndIsLast_Concurrent(t *testing.T) {
	h := NewHub()
	const N = 200
	eventKey := "im.message.receive_v1"

	conns := make([]*testConn, N)
	for i := 0; i < N; i++ {
		conns[i] = newTestConn(eventKey, []string{"im.v1"})
		h.RegisterAndIsFirst(conns[i])
	}

	var lastCount int32
	var wg sync.WaitGroup
	wg.Add(N)
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		c := conns[i]
		go func() {
			defer wg.Done()
			<-start
			if h.UnregisterAndIsLast(c) {
				atomic.AddInt32(&lastCount, 1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := atomic.LoadInt32(&lastCount); got != 1 {
		t.Errorf("UnregisterAndIsLast returned true %d times; want exactly 1", got)
	}
	if got := h.EventKeyCount(eventKey); got != 0 {
		t.Errorf("EventKeyCount after all unregister = %d, want 0", got)
	}
}

type testConn struct {
	eventKey             string
	eventTypes           []string
	sendCh               chan interface{}
	pid                  int
	received             atomic.Int64
	remoteSubscriptionID string
	ownerAppID           string
	ownerUserOpenID      string
}

func newTestConn(eventKey string, eventTypes []string) *testConn {
	return &testConn{
		eventKey:   eventKey,
		eventTypes: eventTypes,
		sendCh:     make(chan interface{}, 100),
		pid:        1,
	}
}

// newRefinedTestConn builds a mock subscriber bound to a remote_subscription_id
// (spec §4.3): a non-empty RemoteSubscriptionID() marks it "refined" for Hub.Publish
// routing, matched by remote_subscription_id equality rather than EventTypes().
func newRefinedTestConn(eventKey string, eventTypes []string, remoteSubscriptionID string) *testConn {
	c := newTestConn(eventKey, eventTypes)
	c.remoteSubscriptionID = remoteSubscriptionID
	return c
}

// newOwnedTestConn builds a mock subscriber with its owner identity fixed
// (spec §4.4): a non-empty ownerUserOpenID marks it a "user" consumer for
// Hub.Publish's identity gate — mirrors newRefinedTestConn's pattern for
// RemoteSubscriptionID. The newTestConn default ("") reads as bot/legacy.
func newOwnedTestConn(eventKey string, eventTypes []string, ownerAppID, ownerUserOpenID string) *testConn {
	c := newTestConn(eventKey, eventTypes)
	c.ownerAppID = ownerAppID
	c.ownerUserOpenID = ownerUserOpenID
	return c
}

func (c *testConn) EventKey() string { return c.eventKey }

// SubscriptionID falls back to EventKey for test mocks that don't set a separate subscription ID.
func (c *testConn) SubscriptionID() string { return c.eventKey }
func (c *testConn) EventTypes() []string   { return c.eventTypes }

// RemoteSubscriptionID: "" (the zero value) means legacy — matches Subscriber's
// documented contract. Tests opt into refined behavior via newRefinedTestConn.
func (c *testConn) RemoteSubscriptionID() string { return c.remoteSubscriptionID }

// OwnerAppID/OwnerUserOpenID: "" (the zero value) means bot/legacy — matches
// Subscriber's documented contract. Tests opt into a "user" owner via
// newOwnedTestConn.
func (c *testConn) OwnerAppID() string       { return c.ownerAppID }
func (c *testConn) OwnerUserOpenID() string  { return c.ownerUserOpenID }
func (c *testConn) SendCh() chan interface{} { return c.sendCh }
func (c *testConn) PID() int                 { return c.pid }
func (c *testConn) IncrementReceived()       { c.received.Add(1) }
func (c *testConn) Received() int64          { return c.received.Load() }

func (c *testConn) DroppedCount() int64 { return 0 }

func (c *testConn) IncrementDropped() {}

func (c *testConn) NextSeq() uint64 { return 0 }

func (c *testConn) PushDropOldest(msg interface{}) (enqueued, dropped bool) {
	select {
	case c.sendCh <- msg:
		return true, false
	default:
	}
	select {
	case <-c.sendCh:
		dropped = true
	default:
	}
	select {
	case c.sendCh <- msg:
		return true, dropped
	default:
		return false, dropped
	}
}

func (c *testConn) TrySend(msg interface{}) bool {
	select {
	case c.sendCh <- msg:
		return true
	default:
		return false
	}
}

func TestHub_SubscriptionID_Isolation(t *testing.T) {
	h := NewHub()
	c1, _ := net.Pipe()
	c2, _ := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	s1 := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 1, "mail.x:alice")
	s2 := NewConn(c2, nil, "mail.x", []string{"mail.x"}, 2, "mail.x:bob")

	if !h.RegisterAndIsFirst(s1) {
		t.Error("s1 should be first for its subscription")
	}
	if !h.RegisterAndIsFirst(s2) {
		t.Error("s2 should ALSO be first (different SubscriptionID)")
	}
	if !h.UnregisterAndIsLast(s1) {
		t.Error("s1 should be last for mail.x:alice")
	}
	if !h.UnregisterAndIsLast(s2) {
		t.Error("s2 should be last for mail.x:bob")
	}
}

func TestHub_SameSubscriptionID_NotFirst(t *testing.T) {
	h := NewHub()
	c1, _ := net.Pipe()
	c2, _ := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	s1 := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 1, "mail.x:alice")
	s2 := NewConn(c2, nil, "mail.x", []string{"mail.x"}, 2, "mail.x:alice")

	if !h.RegisterAndIsFirst(s1) {
		t.Error("s1 first")
	}
	if h.RegisterAndIsFirst(s2) {
		t.Error("s2 same SubscriptionID should NOT be first")
	}
}

func TestHub_EventKeyCount_AggregatesAcrossSubscriptions(t *testing.T) {
	h := NewHub()
	c1, _ := net.Pipe()
	c2, _ := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	s1 := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 1, "mail.x:alice")
	s2 := NewConn(c2, nil, "mail.x", []string{"mail.x"}, 2, "mail.x:bob")
	h.RegisterAndIsFirst(s1)
	h.RegisterAndIsFirst(s2)
	if got := h.EventKeyCount("mail.x"); got != 2 {
		t.Errorf("EventKeyCount(mail.x) = %d, want 2 (aggregated across subscriptions)", got)
	}
	if got := h.SubCount("mail.x:alice"); got != 1 {
		t.Errorf("SubCount(mail.x:alice) = %d, want 1", got)
	}
	if got := h.SubCount("mail.x:bob"); got != 1 {
		t.Errorf("SubCount(mail.x:bob) = %d, want 1", got)
	}
}

func TestHub_Consumers_PopulatesSubscriptionID(t *testing.T) {
	h := NewHub()
	c1, _ := net.Pipe()
	defer c1.Close()
	s1 := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 1, "mail.x:alice")
	h.RegisterAndIsFirst(s1)
	consumers := h.Consumers()
	if len(consumers) != 1 {
		t.Fatalf("got %d consumers, want 1", len(consumers))
	}
	if consumers[0].SubscriptionID != "mail.x:alice" {
		t.Errorf("Consumers()[0].SubscriptionID = %q, want %q", consumers[0].SubscriptionID, "mail.x:alice")
	}
}

func TestHub_TryRegisterExclusive(t *testing.T) {
	h := NewHub()
	first := newTestConn("k.exclusive", []string{"k.exclusive"})
	first.pid = 100
	ok, _ := h.TryRegisterExclusive(first)
	if !ok {
		t.Fatal("first exclusive register should succeed")
	}

	second := newTestConn("k.exclusive", []string{"k.exclusive"})
	second.pid = 200
	ok, reason := h.TryRegisterExclusive(second)
	if ok {
		t.Error("second exclusive register should be rejected")
	}
	if !strings.Contains(reason, "pid 100") {
		t.Errorf("reject reason = %q, want it to name existing pid 100", reason)
	}
	if got := h.SubCount("k.exclusive"); got != 1 {
		t.Errorf("SubCount = %d, want 1 (second not registered)", got)
	}
}

func TestHub_TryRegisterExclusive_CleanupWaitTimeout(t *testing.T) {
	// A cleanup lock that never releases must not wedge a new exclusive consumer
	// forever — TryRegisterExclusive bounds the wait and rejects with a timeout reason.
	saved := exclusiveCleanupWaitTimeout
	exclusiveCleanupWaitTimeout = 20 * time.Millisecond
	defer func() { exclusiveCleanupWaitTimeout = saved }()

	h := NewHub()
	first := newTestConn("k.timeout", []string{"k.timeout"})
	if ok, _ := h.TryRegisterExclusive(first); !ok {
		t.Fatal("first exclusive register should succeed")
	}
	// Hold the cleanup lock and never release it.
	if !h.AcquireCleanupLock("k.timeout") {
		t.Fatal("AcquireCleanupLock should succeed for the sole subscriber")
	}

	start := time.Now()
	second := newTestConn("k.timeout", []string{"k.timeout"})
	ok, reason := h.TryRegisterExclusive(second)
	if ok {
		t.Error("second exclusive register should be rejected on cleanup-wait timeout")
	}
	if !strings.Contains(reason, "timed out") {
		t.Errorf("reject reason = %q, want a timeout reason", reason)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("wait took %v, want bounded by the ~20ms timeout (no deadlock)", elapsed)
	}
}

func TestHub_TryRegisterExclusive_DistinctSubscriptions(t *testing.T) {
	h := NewHub()
	a := newTestConn("k.a", []string{"k.a"})
	b := newTestConn("k.b", []string{"k.b"})
	if ok, _ := h.TryRegisterExclusive(a); !ok {
		t.Fatal("register a failed")
	}
	if ok, _ := h.TryRegisterExclusive(b); !ok {
		t.Error("distinct subscription b should register")
	}
}
