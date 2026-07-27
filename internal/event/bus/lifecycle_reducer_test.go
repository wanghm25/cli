// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"context"
	"fmt"
	"testing"

	"github.com/larksuite/cli/internal/event/bus/lifecycle"
)

// --- §七 must-fix acceptance: ordered reducer + terminal priority + dedup ---

// Event type literals (the same the dispatch tests use); the lifecycle package
// keeps its own copies unexported.
const (
	evTypeActivated = "event.subscription.activated_v1"
	evTypeUpdated   = "event.subscription.updated_v1"
	evTypeExpired   = "event.subscription.expired_v1"
	evTypeDeleted   = "event.subscription.deleted_v1"
)

// A deleted subscription is TERMINAL: a late activated/updated for the same id
// must NOT revive it (no Get, no bind, subscription fact stays deleted).
func TestReducer_DeletedHit_TerminalSurvivesLateActivatedAndUpdated(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	hub.RegisterAndIsFirst(c)

	deleted := lifecycle.LifecycleEvent{EventType: evTypeDeleted, EventID: "evt-del", RemoteSubscriptionID: "sub-1"}
	if err := deps.action.Handle(context.Background(), deleted); err != nil {
		t.Fatalf("Handle(deleted): %v", err)
	}
	if got := c.SubscriptionDegradedReason(); got != lifecycle.ReasonRemoteSubscriptionDeleted {
		t.Fatalf("after deleted: SubscriptionDegradedReason() = %q, want %q", got, lifecycle.ReasonRemoteSubscriptionDeleted)
	}

	// Late activated: must be dropped (not revived), never binds.
	activated := lifecycle.LifecycleEvent{EventType: evTypeActivated, EventID: "evt-act", RemoteSubscriptionID: "sub-1", State: "active"}
	if err := deps.action.Handle(context.Background(), activated); err != nil {
		t.Fatalf("Handle(activated): %v", err)
	}
	// Late updated (would otherwise reconcile with a Get): must be dropped too.
	updated := lifecycle.LifecycleEvent{EventType: evTypeUpdated, EventID: "evt-upd", RemoteSubscriptionID: "sub-1", State: "active", Authority: ""}
	if err := deps.action.Handle(context.Background(), updated); err != nil {
		t.Fatalf("Handle(updated): %v", err)
	}

	if got := c.SubscriptionDegradedReason(); got != lifecycle.ReasonRemoteSubscriptionDeleted {
		t.Errorf("deleted must survive a late activated/updated, got SubscriptionDegradedReason()=%q", got)
	}
	if deps.bind.callCount() != 0 {
		t.Errorf("bindUser call count = %d, want 0 (a tombstoned activated must never revive/bind)", deps.bind.callCount())
	}
	if deps.client.getCount() != 0 {
		t.Errorf("Get call count = %d, want 0 (a tombstoned updated must never reconcile)", deps.client.getCount())
	}
}

// NEW terminal behavior: expired is ALSO terminal (the reducer's phase store
// tombstones it, extending the old deleted-only tombstone). A late updated
// after expired must NOT trigger a reconcile Get — the old code would have.
func TestReducer_ExpiredHit_TerminalSurvivesLateUpdated_NoReconcile(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	c.SetListenIntent("im.message?chat_id=oc_1", true, nil)
	hub.RegisterAndIsFirst(c)
	deps.client.getSub = buildGetSub("active", "") // would clear the conflict IF a Get ran

	expired := lifecycle.LifecycleEvent{EventType: evTypeExpired, EventID: "evt-exp", RemoteSubscriptionID: "sub-1", State: "expired"}
	if err := deps.action.Handle(context.Background(), expired); err != nil {
		t.Fatalf("Handle(expired): %v", err)
	}
	if got := c.SubscriptionDegradedReason(); got != lifecycle.ReasonRemoteSubscriptionExpired {
		t.Fatalf("after expired: SubscriptionDegradedReason() = %q, want %q", got, lifecycle.ReasonRemoteSubscriptionExpired)
	}

	// A late updated with unclear authority would normally issue a single Get to
	// reconcile — but expired is terminal, so it must be dropped instead.
	updated := lifecycle.LifecycleEvent{EventType: evTypeUpdated, EventID: "evt-upd", RemoteSubscriptionID: "sub-1", State: "active", Authority: ""}
	if err := deps.action.Handle(context.Background(), updated); err != nil {
		t.Fatalf("Handle(updated): %v", err)
	}
	if got := deps.client.getCount(); got != 0 {
		t.Errorf("Get call count = %d, want 0 (expired is terminal — a late updated must not reconcile)", got)
	}
	if got := c.SubscriptionDegradedReason(); got != lifecycle.ReasonRemoteSubscriptionExpired {
		t.Errorf("expired must survive a late updated, got SubscriptionDegradedReason()=%q", got)
	}
}

// Per-dimension independence: a successful Renew clears ONLY the Subscription
// dimension — it must NOT clear a persistent decrypt_failed (Decryption
// dimension), which the shared-slot design would have wiped.
func TestReducer_RenewSuccess_ClearsSubscriptionButNotDecryptFailed(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	hub.RegisterAndIsFirst(c)

	// A persistent decrypt failure (Decryption dimension) AND a subscription
	// expiring_soon fact (Subscription dimension).
	for i := 0; i < decryptFailDegradeThreshold; i++ {
		c.RecordDecryptFailure()
	}
	c.SetSubscriptionDegraded(lifecycle.ReasonRemoteSubscriptionExpiringSoon)

	le := lifecycle.LifecycleEvent{EventType: "event.subscription.expiration_reminder_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1"}
	if err := deps.action.Handle(context.Background(), le); err != nil { // Renew succeeds (fake client)
		t.Fatalf("Handle: %v", err)
	}
	if got := c.SubscriptionDegradedReason(); got != "" {
		t.Errorf("SubscriptionDegradedReason() = %q, want \"\" (a successful Renew clears the subscription fact)", got)
	}
	if got := c.DecryptionDegradedReason(); got != decryptStateFailed {
		t.Errorf("DecryptionDegradedReason() = %q, want %q (Renew must NOT clear the decryption fact)", got, decryptStateFailed)
	}
}

// KeepPending is the terminal-aware merge policy the bounded executor consults:
// a pending terminal (deleted/expired) is never superseded by a later
// activated/updated, but is superseded by / can supersede as normal otherwise.
func TestSubscriptionAction_KeepPending_TerminalMergePolicy(t *testing.T) {
	a := lifecycle.NewSubscriptionAction(NewHub().lifecycleRegistry(), discardTestLogger())
	deleted := lifecycle.LifecycleEvent{EventType: evTypeDeleted}
	expired := lifecycle.LifecycleEvent{EventType: evTypeExpired}
	activated := lifecycle.LifecycleEvent{EventType: evTypeActivated}
	updated := lifecycle.LifecycleEvent{EventType: evTypeUpdated}

	if !a.KeepPending(deleted, activated) {
		t.Error("a pending deleted must be KEPT over a later activated (not superseded)")
	}
	if !a.KeepPending(expired, updated) {
		t.Error("a pending expired must be KEPT over a later updated (not superseded)")
	}
	if a.KeepPending(activated, deleted) {
		t.Error("a pending activated must be superseded by a later deleted (terminal wins)")
	}
	if a.KeepPending(activated, updated) {
		t.Error("two non-terminal events must merge latest-wins (no keep)")
	}
}

// Two INDEPENDENT dimensions unhealthy at once are BOTH surfaced by
// Hub.Consumers() — the old single degraded_reason slot could only show one.
func TestHubConsumers_SurfacesMultipleHealthFactsAtOnce(t *testing.T) {
	hub := NewHub()
	c := newConnWithRemoteSub(t, 1, "sub-1")
	c.SetIdentityDegraded("bind_failed: uat_unavailable")
	c.SetSubscriptionDegraded(lifecycle.ReasonRemoteSubscriptionSuspended)
	hub.RegisterAndIsFirst(c)

	infos := hub.Consumers()
	if len(infos) != 1 {
		t.Fatalf("got %d consumers, want 1", len(infos))
	}
	got := infos[0].Health
	if len(got) != 2 {
		t.Fatalf("Health len = %d, want 2 (identity AND subscription at once): %+v", len(got), got)
	}
	// Dimension order (identity before subscription).
	if got[0].Dimension != "identity" || got[0].Reason != "bind_failed: uat_unavailable" {
		t.Errorf("Health[0] = %+v, want identity/bind_failed", got[0])
	}
	if got[1].Dimension != "subscription" || got[1].Reason != lifecycle.ReasonRemoteSubscriptionSuspended {
		t.Errorf("Health[1] = %+v, want subscription/%s", got[1], lifecycle.ReasonRemoteSubscriptionSuspended)
	}
}

// A full bounded queue must DROP an event without committing its dedup key, so a
// later retry of that exact event is still processed (not swallowed) — the
// commit-after-accept fix.
func TestLifecycleExecutor_QueueFull_RetryNotSwallowed(t *testing.T) {
	hub := NewHub()
	action := newRecordingAction()
	g := newTestGate()
	action.gate = g.ch
	exec := lifecycle.NewExecutor(hub.lifecycleRegistry(), action, discardTestLogger())
	defer func() { g.release(); exec.Cancel() }()

	// Occupy both workers on distinct keys (blocked on the gate).
	for i := 0; i < lifecycle.ExecutorWorkers; i++ {
		exec.Submit(context.Background(), lifecycle.LifecycleEvent{
			EventType: "t", EventID: fmt.Sprintf("worker-%d", i), RemoteSubscriptionID: fmt.Sprintf("sub-worker-%d", i),
		})
	}
	waitForCalls(t, action, lifecycle.ExecutorWorkers)

	// Fill the bounded queue with distinct keys.
	for i := 0; i < lifecycle.ExecutorSlots; i++ {
		exec.Submit(context.Background(), lifecycle.LifecycleEvent{
			EventType: "t", EventID: fmt.Sprintf("queued-%d", i), RemoteSubscriptionID: fmt.Sprintf("sub-queued-%d", i),
		})
	}

	// This one overflows and is dropped (executor at capacity) — its dedup key
	// must NOT be committed.
	retry := lifecycle.LifecycleEvent{EventType: "t", EventID: "evt-retry", RemoteSubscriptionID: "sub-retry"}
	exec.Submit(context.Background(), retry)

	// Drain everything.
	g.release()
	waitForCalls(t, action, lifecycle.ExecutorSlots) // the queued backlog runs

	// Retry the dropped event: it must run, proving dedup did not swallow it.
	exec.Submit(context.Background(), retry)
	waitForCalls(t, action, 1)

	found := false
	for _, c := range action.allCalls() {
		if c.EventID == "evt-retry" {
			found = true
			break
		}
	}
	if !found {
		t.Error("the retried event was swallowed — dedup was committed before the queue-full drop (the early-commit bug)")
	}
}

// Events for ONE remote_subscription_id are processed strictly serially (never
// concurrently) — the executor's per-id busy serialization is the ordering
// guarantee the reducer relies on. Two distinct ids proceed independently.
func TestLifecycleExecutor_OrderedPerSubscriptionID_NoConcurrentSameID(t *testing.T) {
	hub := NewHub()
	action := newRecordingAction()
	g := newTestGate()
	action.gate = g.ch
	exec := lifecycle.NewExecutor(hub.lifecycleRegistry(), action, discardTestLogger())
	defer func() { g.release(); exec.Cancel() }()

	// First event for sub-A starts and blocks on the gate.
	exec.Submit(context.Background(), lifecycle.LifecycleEvent{EventType: "t", EventID: "a1", RemoteSubscriptionID: "sub-A"})
	waitForCalls(t, action, 1)

	// More events for the SAME id while the first is in-flight: they must NOT
	// start concurrently (they merge/queue behind the in-flight run).
	for i := 2; i <= 4; i++ {
		exec.Submit(context.Background(), lifecycle.LifecycleEvent{EventType: "t", EventID: fmt.Sprintf("a%d", i), RemoteSubscriptionID: "sub-A"})
	}
	// A DIFFERENT id starts immediately on the other worker (independent).
	exec.Submit(context.Background(), lifecycle.LifecycleEvent{EventType: "t", EventID: "b1", RemoteSubscriptionID: "sub-B"})
	waitForCalls(t, action, 1) // sub-B's first run

	// Exactly 2 runs are in flight (sub-A's first + sub-B's first); sub-A's later
	// events have NOT started (still merged behind the in-flight sub-A run).
	if got := action.callCount(); got != 2 {
		t.Fatalf("in-flight call count = %d, want 2 (sub-A serialized to one run, sub-B independent)", got)
	}
}
