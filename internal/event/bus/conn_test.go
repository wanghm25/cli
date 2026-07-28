// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/bus/lifecycle"
	"github.com/larksuite/cli/internal/event/health"
	"github.com/larksuite/cli/internal/event/protocol"
)

func TestConn_SenderWritesEvents(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	bc := NewConn(server, nil, "im.msg", []string{"im.message.receive_v1"}, 12345, "")
	go bc.SenderLoop()

	bc.SendCh() <- &protocol.Event{
		Type:      protocol.MsgTypeEvent,
		EventType: "im.message.receive_v1",
	}

	scanner := bufio.NewScanner(client)
	client.SetReadDeadline(time.Now().Add(time.Second))
	if !scanner.Scan() {
		t.Fatalf("expected to read a line: %v", scanner.Err())
	}
	line := scanner.Bytes()
	if !bytes.Contains(line, []byte(`"event"`)) {
		t.Errorf("unexpected line: %s", line)
	}
}

type serializingDetector struct {
	net.Conn
	inFlight atomic.Int32
	violated atomic.Bool
}

func (s *serializingDetector) Write(b []byte) (int, error) {
	if s.inFlight.Add(1) > 1 {
		s.violated.Store(true)
	}
	time.Sleep(500 * time.Microsecond)
	defer s.inFlight.Add(-1)
	return s.Conn.Write(b)
}

// Two goroutines writing frames (event + ack) must not overlap on the underlying net.Conn.
func TestConn_ConcurrentWritesSerialised(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	det := &serializingDetector{Conn: server}
	bc := NewConn(det, nil, "im.msg", []string{"im.msg"}, 12345, "")

	go func() { _, _ = io.Copy(io.Discard, client) }()

	go bc.SenderLoop()

	var wg sync.WaitGroup
	const workers = 8
	const perWorker = 20
	deadline := time.Now().Add(2 * time.Second)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker && time.Now().Before(deadline); j++ {
				bc.SendCh() <- &protocol.Event{Type: protocol.MsgTypeEvent, EventType: "im.msg"}
			}
		}()
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker && time.Now().Before(deadline); j++ {
				bc.handleControlMessage(&protocol.PreShutdownCheck{EventKey: "im.msg"})
			}
		}()
	}

	wg.Wait()
	bc.Close()

	if det.violated.Load() {
		t.Error("concurrent Write on net.Conn detected: SenderLoop and handleControlMessage " +
			"overlapped without serialisation (framing / deadline race)")
	}
}

func TestConn_TrySend_NonEvicting(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	bc := NewConn(server, nil, "im.msg", []string{"im.msg"}, 12345, "")

	for i := 0; i < sendChCap; i++ {
		if !bc.TrySend(i) {
			t.Fatalf("TrySend returned false at iteration %d; expected all sendChCap (%d) to fit", i, sendChCap)
		}
	}
	if bc.TrySend("overflow") {
		t.Fatal("TrySend on full channel returned true: TrySend must be non-evicting")
	}
	first := <-bc.SendCh()
	if first != 0 {
		t.Errorf("first drained item = %v, want 0", first)
	}
}

func TestConn_ReaderDetectsEOF(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	bc := NewConn(server, nil, "im.msg", []string{"im.msg"}, 12345, "")

	done := make(chan struct{})
	go func() {
		bc.ReaderLoop()
		close(done)
	}()

	client.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ReaderLoop did not exit on EOF")
	}
}

func TestConn_SubscriptionID(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	conn := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 999, "mail.x:abc")
	if got := conn.SubscriptionID(); got != "mail.x:abc" {
		t.Errorf("SubscriptionID() = %q, want %q", got, "mail.x:abc")
	}
}

func TestConn_SubscriptionID_EmptyFallsBackToEventKey(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	conn := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 999, "")
	if got := conn.SubscriptionID(); got != "mail.x" {
		t.Errorf("SubscriptionID() with empty input = %q, want fallback %q", got, "mail.x")
	}
}

// RemoteSubscriptionID defaults to "" (legacy) until explicitly set — a fresh
// Conn built via NewConn (the Task-15 client hasn't populated Hello.RemoteSubscriptionID
// yet) must satisfy Subscriber's "empty = legacy" contract.
func TestConn_RemoteSubscriptionID_DefaultEmpty(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	conn := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 999, "")
	if got := conn.RemoteSubscriptionID(); got != "" {
		t.Errorf("RemoteSubscriptionID() on a fresh Conn = %q, want \"\" (legacy default)", got)
	}
}

func TestConn_RemoteSubscriptionID_SetterRoundTrips(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	conn := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 999, "")
	conn.SetRemoteSubscriptionID("sub_abc123")
	if got := conn.RemoteSubscriptionID(); got != "sub_abc123" {
		t.Errorf("RemoteSubscriptionID() after SetRemoteSubscriptionID(%q) = %q, want %q", "sub_abc123", got, "sub_abc123")
	}
}

// --- Task 14: owner identity + bind/stale/degraded state (spec §4.4) ---

// A fresh Conn (the CLIENT hasn't sent Hello.Identity/UserOpenID yet — that's
// Task 15 — or this is a bot) must default every owner field to "": the
// identity gate reads OwnerUserOpenID()=="" as its bot/legacy bypass signal.
func TestConn_OwnerIdentity_DefaultEmpty(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	conn := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 999, "")
	if got := conn.OwnerIdentity(); got != "" {
		t.Errorf("OwnerIdentity() on a fresh Conn = %q, want \"\"", got)
	}
	if got := conn.OwnerAppID(); got != "" {
		t.Errorf("OwnerAppID() on a fresh Conn = %q, want \"\"", got)
	}
	if got := conn.OwnerUserOpenID(); got != "" {
		t.Errorf("OwnerUserOpenID() on a fresh Conn = %q, want \"\" (legacy/bot default — never gated)", got)
	}
}

func TestConn_SetOwnerIdentity_RoundTrips(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	conn := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 999, "")
	conn.SetOwnerIdentity("user", "app_1", "ou_xyz")
	if got := conn.OwnerIdentity(); got != "user" {
		t.Errorf("OwnerIdentity() = %q, want %q", got, "user")
	}
	if got := conn.OwnerAppID(); got != "app_1" {
		t.Errorf("OwnerAppID() = %q, want %q", got, "app_1")
	}
	if got := conn.OwnerUserOpenID(); got != "ou_xyz" {
		t.Errorf("OwnerUserOpenID() = %q, want %q", got, "ou_xyz")
	}
}

func TestConn_ListenIntent_DefaultEmpty(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	conn := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 999, "")
	if got := conn.TargetResource(); got != "" {
		t.Errorf("TargetResource() on a fresh Conn = %q, want \"\" (legacy default)", got)
	}
	if got := conn.IncludeResourceDataIntent(); got {
		t.Errorf("IncludeResourceDataIntent() on a fresh Conn = %v, want false (legacy default)", got)
	}
	if got := conn.FilterIntent(); got != nil {
		t.Errorf("FilterIntent() on a fresh Conn = %v, want nil (legacy default)", got)
	}
}

func TestConn_SetListenIntent_RoundTrips(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	conn := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 999, "")
	filter := newListenIntentTestFilter("oc_1")
	conn.SetListenIntent("im.message?chat_id=oc_1", true, filter)
	if got := conn.TargetResource(); got != "im.message?chat_id=oc_1" {
		t.Errorf("TargetResource() = %q, want %q", got, "im.message?chat_id=oc_1")
	}
	if got := conn.IncludeResourceDataIntent(); !got {
		t.Errorf("IncludeResourceDataIntent() = %v, want true", got)
	}
	if got := conn.FilterIntent(); !event.Equal(got, filter) {
		t.Errorf("FilterIntent() = %+v, want the stored filter %+v", got, filter)
	}
}

// newListenIntentTestFilter builds a small, valid CLI filter for listen-intent
// round-trip assertions.
func newListenIntentTestFilter(chatID string) *event.Filter {
	return &event.Filter{Root: &event.FilterNode{
		LogicOp:  "and",
		Children: []*event.FilterNode{{Condition: &event.FilterCond{Operand: "chat_id", Op: "eq", Value: chatID}}},
	}}
}

func TestConn_BoundConnID_DefaultEmpty(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	conn := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 999, "")
	if got := conn.BoundConnID(); got != "" {
		t.Errorf("BoundConnID() on a fresh Conn = %q, want \"\" (never bound)", got)
	}
}

// SetBoundConnID marks a successful BindUser; a fresh success supersedes the
// IDENTITY state (stale flag + identity health fact) only.
func TestConn_SetBoundConnID_ClearsStaleAndIdentity(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	conn := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 999, "")
	conn.SetStaleIdentity()
	conn.SetIdentityDegraded("bind_failed: test")

	conn.SetBoundConnID("conn-42")

	if got := conn.BoundConnID(); got != "conn-42" {
		t.Errorf("BoundConnID() = %q, want %q", got, "conn-42")
	}
	if conn.StaleIdentity() {
		t.Error("SetBoundConnID must clear staleIdentity — a fresh successful bind supersedes it")
	}
	if got := conn.IdentityDegradedReason(); got != "" {
		t.Errorf("IdentityDegradedReason() after SetBoundConnID = %q, want \"\" (cleared by a fresh successful bind)", got)
	}
}

// MUST-FIX (per-dimension health): a successful BindUser (a rebind) clears ONLY
// the identity dimension — it must NOT wipe a still-true Subscription
// (deleted/expired/...) or Decryption (decrypt_failed) health fact, nor the
// subscription-tied next_action. The old shared slot cleared everything on a
// rebind; this test locks that that regression is gone.
func TestConn_SetBoundConnID_ClearsOnlyIdentity(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	conn := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 999, "")
	conn.SetIdentityDegraded("bind_failed: test")
	conn.SetSubscriptionDegraded(lifecycle.ReasonRemoteSubscriptionDeleted)
	conn.SetSubscriptionNextAction(lifecycle.NextActionRebuild)
	conn.RecordDecryptFailure()
	conn.RecordDecryptFailure()
	conn.RecordDecryptFailure() // cross the degrade threshold

	conn.SetBoundConnID("conn-42")

	if got := conn.IdentityDegradedReason(); got != "" {
		t.Errorf("IdentityDegradedReason() = %q, want \"\" (bind clears identity)", got)
	}
	if got := conn.SubscriptionDegradedReason(); got != lifecycle.ReasonRemoteSubscriptionDeleted {
		t.Errorf("SubscriptionDegradedReason() = %q, want %q (a rebind must NOT clear the subscription fact)", got, lifecycle.ReasonRemoteSubscriptionDeleted)
	}
	if got := conn.DecryptionDegradedReason(); got != decryptStateFailed {
		t.Errorf("DecryptionDegradedReason() = %q, want %q (a rebind must NOT clear the decryption fact)", got, decryptStateFailed)
	}
	if got := conn.NextAction(); got != lifecycle.NextActionRebuild {
		t.Errorf("NextAction() = %q, want %q (a rebind must NOT clear a subscription-tied next_action)", got, lifecycle.NextActionRebuild)
	}
}

func TestConn_SetStaleIdentity(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	conn := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 999, "")
	if conn.StaleIdentity() {
		t.Fatal("fresh Conn must not start stale_identity")
	}
	conn.SetStaleIdentity()
	if !conn.StaleIdentity() {
		t.Error("StaleIdentity() after SetStaleIdentity() = false, want true")
	}
}

func TestConn_SetIdentityDegraded_RoundTrips(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	conn := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 999, "")
	if got := conn.IdentityDegradedReason(); got != "" {
		t.Errorf("IdentityDegradedReason() on a fresh Conn = %q, want \"\"", got)
	}
	conn.SetIdentityDegraded("bind_failed: uat_unavailable")
	if got := conn.IdentityDegradedReason(); got != "bind_failed: uat_unavailable" {
		t.Errorf("IdentityDegradedReason() = %q, want %q", got, "bind_failed: uat_unavailable")
	}
}

// TestConn_IdentityGateState_ConcurrentAccessRace exercises every new
// identity-gate mutator/getter concurrently: in production this state is
// written from TWO independent goroutines (identity.go's onConnReady, driven
// by the WS ready/reconnect callback, and Hub.Publish's per-event delivery
// gate, driven by the source's emit goroutine) and read by a future status
// command from yet another — hence its own dedicated mutex rather than the
// zero-lock convention used for the write-once owner fields above. Run with
// -race (mirrors hub_publish_race_test.go's style for the Hub side).
// --- Task 17: lifecycle summary state (spec §5.1/§5.5) --------------------

func TestConn_LifecycleSummary_DefaultEmpty(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	conn := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 999, "")
	if got := conn.LastLifecycleEvent(); got != "" {
		t.Errorf("LastLifecycleEvent() on a fresh Conn = %q, want \"\"", got)
	}
	if got := conn.LastLifecycleEventID(); got != "" {
		t.Errorf("LastLifecycleEventID() on a fresh Conn = %q, want \"\"", got)
	}
	if got := conn.RemoteState(); got != "" {
		t.Errorf("RemoteState() on a fresh Conn = %q, want \"\"", got)
	}
}

func TestConn_SetLifecycleSummary_RoundTrips(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	conn := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 999, "")
	conn.SetLifecycleSummary("event.subscription.suspended_v1", "evt-1", "suspended")
	if got := conn.LastLifecycleEvent(); got != "event.subscription.suspended_v1" {
		t.Errorf("LastLifecycleEvent() = %q, want %q", got, "event.subscription.suspended_v1")
	}
	if got := conn.LastLifecycleEventID(); got != "evt-1" {
		t.Errorf("LastLifecycleEventID() = %q, want %q", got, "evt-1")
	}
	if got := conn.RemoteState(); got != "suspended" {
		t.Errorf("RemoteState() = %q, want %q", got, "suspended")
	}
}

// SetLifecycleSummary must NOT blank a previously-known remote_state when the
// new event's state is "" (e.g. deleted_v1's SDK body carries no state field
// at all) -- doing so would destroy real information for no benefit;
// lastLifecycleEvent alone already records that the newer event happened.
func TestConn_SetLifecycleSummary_EmptyStateDoesNotClearPriorRemoteState(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	conn := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 999, "")
	conn.SetLifecycleSummary("event.subscription.activated_v1", "evt-1", "active")
	conn.SetLifecycleSummary("event.subscription.deleted_v1", "evt-2", "")

	if got := conn.LastLifecycleEvent(); got != "event.subscription.deleted_v1" {
		t.Errorf("LastLifecycleEvent() = %q, want %q (must always update)", got, "event.subscription.deleted_v1")
	}
	if got := conn.LastLifecycleEventID(); got != "evt-2" {
		t.Errorf("LastLifecycleEventID() = %q, want %q (must always update)", got, "evt-2")
	}
	if got := conn.RemoteState(); got != "active" {
		t.Errorf("RemoteState() = %q, want %q (an empty new state must not clear a prior known value)", got, "active")
	}
}

// TestConn_LifecycleSummaryState_ConcurrentAccessRace mirrors
// TestConn_IdentityGateState_ConcurrentAccessRace's rationale for the Task 17
// fields: SetLifecycleSummary is written from the lifecycle executor's
// worker goroutines and read by a future status query concurrently. Run with
// -race.
func TestConn_LifecycleSummaryState_ConcurrentAccessRace(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	conn := NewConn(server, nil, "im.msg", []string{"im.msg"}, 1, "")

	var wg sync.WaitGroup
	const workers = 8
	const iterations = 200
	deadline := time.Now().Add(2 * time.Second)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < iterations && time.Now().Before(deadline); j++ {
				conn.SetLifecycleSummary("event.subscription.activated_v1", "evt-race", "active")
				_ = conn.LastLifecycleEvent()
				_ = conn.LastLifecycleEventID()
				_ = conn.RemoteState()
			}
		}(i)
	}
	wg.Wait()
}

// --- Task 18: lifecycle ACTION state (spec §5.3/§5.4/§5.5) ----------------

func TestConn_ActionState_DefaultEmpty(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	conn := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 999, "")
	if got := conn.SuspensionReason(); got != "" {
		t.Errorf("SuspensionReason() on a fresh Conn = %q, want \"\"", got)
	}
	if got := conn.LastAction(); got != "" {
		t.Errorf("LastAction() on a fresh Conn = %q, want \"\"", got)
	}
	if got := conn.LastActionError(); got != "" {
		t.Errorf("LastActionError() on a fresh Conn = %q, want \"\"", got)
	}
	if got := conn.NextAction(); got != "" {
		t.Errorf("NextAction() on a fresh Conn = %q, want \"\"", got)
	}
}

func TestConn_SetSuspensionReason_RoundTripsVerbatim(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	conn := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 999, "")
	// An UNRECOGNIZED, made-up code must round-trip unchanged (spec §5.4: an
	// open string, never a closed enum -- no validation, no rejection).
	conn.SetSuspensionReason("some_brand_new_future_code_the_cli_has_never_seen")
	if got := conn.SuspensionReason(); got != "some_brand_new_future_code_the_cli_has_never_seen" {
		t.Errorf("SuspensionReason() = %q, want verbatim passthrough", got)
	}
	conn.SetSuspensionReason("")
	if got := conn.SuspensionReason(); got != "" {
		t.Errorf("SuspensionReason() after clearing = %q, want \"\"", got)
	}
}

func TestConn_SetLastAction_And_SetLastActionError_RoundTrip(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	conn := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 999, "")
	conn.SetLastAction("reactivate")
	conn.SetLastActionError("missing_scopes")
	if got := conn.LastAction(); got != "reactivate" {
		t.Errorf("LastAction() = %q, want %q", got, "reactivate")
	}
	if got := conn.LastActionError(); got != "missing_scopes" {
		t.Errorf("LastActionError() = %q, want %q", got, "missing_scopes")
	}
	// A fresh attempt (even the same action name) must supersede a stale error.
	conn.SetLastAction("reactivate")
	conn.SetLastActionError("")
	if got := conn.LastActionError(); got != "" {
		t.Errorf("LastActionError() after a fresh successful attempt = %q, want \"\"", got)
	}
}

// NextAction() projects the surfaced recovery from the per-dimension health
// facts. A subscription-dimension recovery round-trips through the projection.
func TestConn_SubscriptionNextAction_RoundTripsThroughProjection(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	conn := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 999, "")
	conn.SetSubscriptionDegraded(lifecycle.ReasonRemoteSubscriptionSuspended)
	conn.SetSubscriptionNextAction(lifecycle.NextActionReactivate)
	if got := conn.NextAction(); got != lifecycle.NextActionReactivate {
		t.Errorf("NextAction() = %q, want %q", got, lifecycle.NextActionReactivate)
	}
}

// A per-dimension regression: an identity rebind and a subscription recovery are
// each surfaced independently, and clearing the subscription dimension does NOT
// clear the identity rebind — the projection then surfaces the surviving rebind.
func TestConn_PerDimensionNextAction_IdentityRebindSurvivesSubscriptionClear(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	conn := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 999, "")

	// Identity degraded + its own rebind recovery; subscription degraded + get.
	conn.SetIdentityDegraded("bind_failed: bind_api_error")
	conn.SetIdentityNextAction(lifecycle.NextActionRebind)
	conn.SetSubscriptionDegraded(lifecycle.ReasonRemoteSubscriptionConflict)
	conn.SetSubscriptionNextAction(lifecycle.NextActionGet)

	// Both dimensions retain their own recovery (read straight off the facts).
	if got := conn.health.Get(health.Identity).NextAction; got != lifecycle.NextActionRebind {
		t.Errorf("Identity next_action = %q, want %q", got, lifecycle.NextActionRebind)
	}
	if got := conn.health.Get(health.Subscription).NextAction; got != lifecycle.NextActionGet {
		t.Errorf("Subscription next_action = %q, want %q", got, lifecycle.NextActionGet)
	}

	// Clearing subscription must NOT wipe the identity rebind.
	conn.ClearSubscriptionDegraded()
	if got := conn.health.Get(health.Subscription).NextAction; got != "" {
		t.Errorf("Subscription next_action after clear = %q, want \"\"", got)
	}
	if got := conn.health.Get(health.Identity).NextAction; got != lifecycle.NextActionRebind {
		t.Errorf("Identity next_action after ClearSubscriptionDegraded = %q, want %q (must survive)", got, lifecycle.NextActionRebind)
	}
	// The projection now surfaces the surviving identity rebind.
	if got := conn.NextAction(); got != lifecycle.NextActionRebind {
		t.Errorf("NextAction() = %q, want %q (rebind survives, subscription cleared)", got, lifecycle.NextActionRebind)
	}
}

// ClearSubscriptionDegraded (used internally by subscriptionLifecycleAction)
// must clear the subscription-dimension fact AND the subscription-tied
// nextAction together, but must NEVER touch
// suspensionReason/lastAction/lastActionError -- those are historical
// record-keeping, not "is this consumer currently degraded" state.
func TestConn_ClearSubscriptionDegraded_ClearsOnlySubscriptionAndNextAction(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	conn := NewConn(c1, nil, "mail.x", []string{"mail.x"}, 999, "")
	conn.SetSubscriptionDegraded("remote_subscription_suspended")
	conn.SetSubscriptionNextAction(lifecycle.NextActionReactivate)
	conn.SetSuspensionReason("authority_revoked")
	conn.SetLastAction("reactivate")
	conn.SetLastActionError("some_error")

	conn.ClearSubscriptionDegraded()

	if got := conn.SubscriptionDegradedReason(); got != "" {
		t.Errorf("SubscriptionDegradedReason() = %q, want \"\"", got)
	}
	if got := conn.NextAction(); got != "" {
		t.Errorf("NextAction() = %q, want \"\"", got)
	}
	if got := conn.SuspensionReason(); got != "authority_revoked" {
		t.Errorf("SuspensionReason() = %q, want unchanged %q", got, "authority_revoked")
	}
	if got := conn.LastAction(); got != "reactivate" {
		t.Errorf("LastAction() = %q, want unchanged %q", got, "reactivate")
	}
	if got := conn.LastActionError(); got != "some_error" {
		t.Errorf("LastActionError() = %q, want unchanged %q", got, "some_error")
	}
}

// TestConn_ActionState_ConcurrentAccessRace mirrors
// TestConn_LifecycleSummaryState_ConcurrentAccessRace's rationale for Task
// 18's action-state fields: written by the lifecycle executor's worker
// goroutines (subscriptionLifecycleAction), read by a future status query
// concurrently. Run with -race.
func TestConn_ActionState_ConcurrentAccessRace(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	conn := NewConn(server, nil, "im.msg", []string{"im.msg"}, 1, "")

	var wg sync.WaitGroup
	const workers = 8
	const iterations = 200
	deadline := time.Now().Add(2 * time.Second)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < iterations && time.Now().Before(deadline); j++ {
				conn.SetSuspensionReason("authority_revoked")
				conn.SetLastAction("reactivate")
				conn.SetLastActionError("")
				conn.SetSubscriptionNextAction(lifecycle.NextActionReactivate)
				conn.ClearSubscriptionDegraded()
				_ = conn.SuspensionReason()
				_ = conn.LastAction()
				_ = conn.LastActionError()
				_ = conn.NextAction()
			}
		}(i)
	}
	wg.Wait()
}

func TestConn_IdentityGateState_ConcurrentAccessRace(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	conn := NewConn(server, nil, "im.msg", []string{"im.msg"}, 1, "")
	conn.SetOwnerIdentity("user", "app1", "ou_alice")

	var wg sync.WaitGroup
	const workers = 8
	const iterations = 200
	deadline := time.Now().Add(2 * time.Second)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < iterations && time.Now().Before(deadline); j++ {
				if i%2 == 0 {
					conn.SetBoundConnID("conn-race")
				} else {
					conn.SetStaleIdentity()
				}
				conn.SetIdentityDegraded("bind_failed: test")
				_ = conn.BoundConnID()
				_ = conn.StaleIdentity()
				_ = conn.IdentityDegradedReason()
				_ = conn.OwnerAppID()
				_ = conn.OwnerUserOpenID()
				_ = conn.OwnerIdentity()
			}
		}(i)
	}
	wg.Wait()
}
