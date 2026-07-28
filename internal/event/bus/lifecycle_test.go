// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/larksuite/cli/internal/event/bus/lifecycle"
)

// --- test helpers -----------------------------------------------------------

// testGate is a close-exactly-once signal channel: recordingAction.Handle
// blocks on it (or ctx.Done()) so tests can deterministically control when a
// "running" action completes, without sleep-based timing races. release is
// safe to call multiple times (idempotent via sync.Once) so deferred test
// cleanup never double-closes a channel already released mid-test.
type testGate struct {
	ch   chan struct{}
	once sync.Once
}

func newTestGate() *testGate { return &testGate{ch: make(chan struct{})} }

func (g *testGate) release() { g.once.Do(func() { close(g.ch) }) }

// recordingAction is a test lifecycleAction: it records every Handle call
// (thread-safe), signals a buffered "started" channel per call so tests can
// wait for exactly N invocations without sleeping, and optionally blocks on a
// gate (or ctx.Done(), whichever first) until released.
type recordingAction struct {
	mu    sync.Mutex
	calls []lifecycle.LifecycleEvent

	started chan lifecycle.LifecycleEvent // buffered; one send per Handle call (best-effort, generously sized)
	gate    <-chan struct{}               // nil = never blocks
}

func newRecordingAction() *recordingAction {
	return &recordingAction{started: make(chan lifecycle.LifecycleEvent, 256)}
}

func (r *recordingAction) Handle(ctx context.Context, le lifecycle.LifecycleEvent) error {
	r.mu.Lock()
	r.calls = append(r.calls, le)
	r.mu.Unlock()

	select {
	case r.started <- le:
	default:
	}

	if r.gate != nil {
		select {
		case <-r.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (r *recordingAction) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *recordingAction) allCalls() []lifecycle.LifecycleEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]lifecycle.LifecycleEvent, len(r.calls))
	copy(out, r.calls)
	return out
}

func (r *recordingAction) lastCall() (lifecycle.LifecycleEvent, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.calls) == 0 {
		return lifecycle.LifecycleEvent{}, false
	}
	return r.calls[len(r.calls)-1], true
}

// waitForCalls blocks until action has recorded at least n Handle
// invocations (via its started channel) or fails the test after a bounded
// timeout.
func waitForCalls(t *testing.T, action *recordingAction, n int) {
	t.Helper()
	timeout := time.After(2 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case <-action.started:
		case <-timeout:
			t.Fatalf("timed out waiting for Handle call %d/%d (got %d so far)", i+1, n, action.callCount())
		}
	}
}

func newConnWithRemoteSub(t *testing.T, pid int, remoteSubID string) *Conn {
	t.Helper()
	conn, _ := net.Pipe()
	t.Cleanup(func() { conn.Close() })
	c := NewConn(conn, nil, "im.msg", []string{"im.message.receive_v1"}, pid, "")
	c.SetRemoteSubscriptionID(remoteSubID)
	return c
}

// --- Submit mechanics: validation, dedup, in-flight merge, full, cancel, timeout ---

func TestLifecycleExecutor_Submit_MissingEventIDOrRemoteSubID_Dropped(t *testing.T) {
	hub := NewHub()
	action := newRecordingAction()
	exec := lifecycle.NewExecutor(hub.lifecycleRegistry(), action, discardTestLogger())
	defer exec.Cancel()

	exec.Submit(context.Background(), lifecycle.LifecycleEvent{EventType: "t", EventID: "", RemoteSubscriptionID: "sub-1"})
	exec.Submit(context.Background(), lifecycle.LifecycleEvent{EventType: "t", EventID: "evt-1", RemoteSubscriptionID: ""})

	time.Sleep(50 * time.Millisecond)
	if got := action.callCount(); got != 0 {
		t.Fatalf("Handle called %d times, want 0 (both submissions are missing a required field)", got)
	}
}

func TestLifecycleExecutor_Submit_Valid_RunsAction(t *testing.T) {
	hub := NewHub()
	action := newRecordingAction()
	exec := lifecycle.NewExecutor(hub.lifecycleRegistry(), action, discardTestLogger())
	defer exec.Cancel()

	le := lifecycle.LifecycleEvent{EventType: "event.subscription.activated_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "active"}
	exec.Submit(context.Background(), le)
	waitForCalls(t, action, 1)

	got, ok := action.lastCall()
	if !ok || got != le {
		t.Errorf("Handle called with %+v, want %+v", got, le)
	}
}

// TestLifecycleExecutor_Dedup: the SAME remote_subscription_id+event_id
// delivered twice (spec §5.2: dedup keyed by remote_subscription_id+event_id,
// via a THIRD DedupFilter domain separate from Hub's legacy/refined) must run
// the action exactly once.
func TestLifecycleExecutor_Dedup_SameRemoteSubIDAndEventID_RunsOnce(t *testing.T) {
	hub := NewHub()
	action := newRecordingAction()
	exec := lifecycle.NewExecutor(hub.lifecycleRegistry(), action, discardTestLogger())
	defer exec.Cancel()

	le := lifecycle.LifecycleEvent{EventType: "t", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "active"}
	exec.Submit(context.Background(), le)
	exec.Submit(context.Background(), le)
	exec.Submit(context.Background(), le)

	waitForCalls(t, action, 1)
	time.Sleep(50 * time.Millisecond) // settle window: a wrongful 2nd/3rd call must not sneak in
	if got := action.callCount(); got != 1 {
		t.Fatalf("Handle called %d times, want 1 (dedup must drop the exact duplicates)", got)
	}
}

// TestLifecycleExecutor_Dedup_DifferentRemoteSubID_BothRun: dedup is keyed by
// remote_subscription_id+event_id TOGETHER (spec §5.2: "生命周期同样带 remote
// id，不只按 event_id") -- the SAME event_id under two different
// remote_subscription_ids must NOT be treated as a duplicate of each other.
func TestLifecycleExecutor_Dedup_DifferentRemoteSubID_BothRun(t *testing.T) {
	hub := NewHub()
	action := newRecordingAction()
	exec := lifecycle.NewExecutor(hub.lifecycleRegistry(), action, discardTestLogger())
	defer exec.Cancel()

	exec.Submit(context.Background(), lifecycle.LifecycleEvent{EventType: "t", EventID: "evt-shared", RemoteSubscriptionID: "sub-A"})
	exec.Submit(context.Background(), lifecycle.LifecycleEvent{EventType: "t", EventID: "evt-shared", RemoteSubscriptionID: "sub-B"})

	waitForCalls(t, action, 2)
	time.Sleep(50 * time.Millisecond)
	if got := action.callCount(); got != 2 {
		t.Fatalf("Handle called %d times, want 2 (different remote_subscription_id must not dedup against each other)", got)
	}
}

// TestLifecycleExecutor_FIFO_SameRemoteSubscriptionID_ProcessedInArrivalOrder
// locks the ordered reducer (#11): a burst of DISTINCT events for the SAME
// remote_subscription_id arriving while a run for that id is in-flight must EACH
// be processed, in ARRIVAL order — true FIFO, not the old latest-wins that ran
// only the newest and overwrote the middle events.
func TestLifecycleExecutor_FIFO_SameRemoteSubscriptionID_ProcessedInArrivalOrder(t *testing.T) {
	hub := NewHub()
	action := newRecordingAction()
	g := newTestGate()
	action.gate = g.ch
	exec := lifecycle.NewExecutor(hub.lifecycleRegistry(), action, discardTestLogger())
	defer func() { g.release(); exec.Cancel() }()

	first := lifecycle.LifecycleEvent{EventType: "t", EventID: "evt-1", RemoteSubscriptionID: "sub-fifo", State: "active"}
	exec.Submit(context.Background(), first)
	waitForCalls(t, action, 1) // first Handle call is now blocked on the gate

	// Four more DISTINCT events queue behind the in-flight run, in order.
	want := []string{"evt-1", "evt-2", "evt-3", "evt-4", "evt-5"}
	for i := 2; i <= 5; i++ {
		exec.Submit(context.Background(), lifecycle.LifecycleEvent{
			EventType: "t", EventID: fmt.Sprintf("evt-%d", i), RemoteSubscriptionID: "sub-fifo", State: "active",
		})
	}

	g.release() // let the in-flight run finish; the queued backlog drains in order
	waitForCalls(t, action, 4)

	time.Sleep(50 * time.Millisecond) // settle window: no extra/dropped events
	calls := action.allCalls()
	if len(calls) != len(want) {
		t.Fatalf("Handle called %d times, want %d (every queued event runs, not just the last); calls=%+v", len(calls), len(want), calls)
	}
	for i, w := range want {
		if calls[i].EventID != w {
			t.Errorf("call[%d].EventID = %q, want %q (FIFO arrival order, not latest-wins)", i, calls[i].EventID, w)
		}
	}
}

// TestLifecycleExecutor_InFlightMerge_DifferentRemoteSubID_BothRunIndependently
// is the merge-scope regression: two DIFFERENT remote_subscription_ids must
// never merge with each other, even while both are in flight simultaneously.
func TestLifecycleExecutor_InFlightMerge_DifferentRemoteSubID_BothRunIndependently(t *testing.T) {
	hub := NewHub()
	action := newRecordingAction()
	exec := lifecycle.NewExecutor(hub.lifecycleRegistry(), action, discardTestLogger())
	defer exec.Cancel()

	exec.Submit(context.Background(), lifecycle.LifecycleEvent{EventType: "t", EventID: "evt-a", RemoteSubscriptionID: "sub-a"})
	exec.Submit(context.Background(), lifecycle.LifecycleEvent{EventType: "t", EventID: "evt-b", RemoteSubscriptionID: "sub-b"})

	waitForCalls(t, action, 2)
	time.Sleep(50 * time.Millisecond)
	if got := action.callCount(); got != 2 {
		t.Fatalf("Handle called %d times, want 2 (independent remote_subscription_ids must not merge)", got)
	}
}

// TestLifecycleExecutor_QueueFull_MarksMatchedConsumerDegraded_NoBlock locks
// spec §5.2: capacity is a small, tested code constant (worker=2, slots=32);
// once truly full, Submit must NOT block, and the matched consumer(s) for the
// dropped event must be marked degraded with the lifecycle.ReasonExecutorFull
// classification.
func TestLifecycleExecutor_QueueFull_MarksMatchedConsumerDegraded_NoBlock(t *testing.T) {
	hub := NewHub()
	action := newRecordingAction()
	g := newTestGate()
	action.gate = g.ch
	exec := lifecycle.NewExecutor(hub.lifecycleRegistry(), action, discardTestLogger())
	defer func() { g.release(); exec.Cancel() }()

	// Occupy BOTH workers on distinct keys so neither can drain the queue.
	for i := 0; i < lifecycle.ExecutorWorkers; i++ {
		exec.Submit(context.Background(), lifecycle.LifecycleEvent{
			EventType: "t", EventID: fmt.Sprintf("worker-evt-%d", i), RemoteSubscriptionID: fmt.Sprintf("sub-worker-%d", i),
		})
	}
	waitForCalls(t, action, lifecycle.ExecutorWorkers)

	// Fill the bounded queue to capacity with DISTINCT keys (a repeated key
	// would merge instead of consuming a new slot).
	for i := 0; i < lifecycle.ExecutorSlots; i++ {
		exec.Submit(context.Background(), lifecycle.LifecycleEvent{
			EventType: "t", EventID: fmt.Sprintf("queued-evt-%d", i), RemoteSubscriptionID: fmt.Sprintf("sub-queued-%d", i),
		})
	}

	c := newConnWithRemoteSub(t, 99, "sub-overflow")
	hub.RegisterAndIsFirst(c)

	overflowDone := make(chan struct{})
	go func() {
		exec.Submit(context.Background(), lifecycle.LifecycleEvent{EventType: "t", EventID: "overflow-evt", RemoteSubscriptionID: "sub-overflow"})
		close(overflowDone)
	}()

	select {
	case <-overflowDone:
	case <-time.After(1 * time.Second):
		t.Fatal("Submit blocked on a full executor -- spec §5.2 requires it never block")
	}

	if got := c.SubscriptionDegradedReason(); got != lifecycle.ReasonExecutorFull {
		t.Errorf("DegradedReason() = %q, want %q", got, lifecycle.ReasonExecutorFull)
	}
	// Task 18 review Minor 2: now that a next_action field exists, the
	// full-queue path must ALSO set an explicit management next_action —
	// not just the degraded reason.
	if got := c.NextAction(); got != lifecycle.NextActionGet {
		t.Errorf("NextAction() = %q, want %q (Task 18 Minor 2: an explicit management next_action on queue-full)", got, lifecycle.NextActionGet)
	}
}

// TestLifecycleExecutor_Cancel_WaitsForInFlightRun_ButNotForcefully locks
// spec §5.2's cancel semantics: an already-started run finishes (bounded by
// its OWN per-action timeout), it is not killed the instant Cancel is called.
func TestLifecycleExecutor_Cancel_WaitsForInFlightRun(t *testing.T) {
	hub := NewHub()
	action := newRecordingAction()
	g := newTestGate()
	action.gate = g.ch
	exec := lifecycle.NewExecutor(hub.lifecycleRegistry(), action, discardTestLogger())

	exec.Submit(context.Background(), lifecycle.LifecycleEvent{EventType: "t", EventID: "evt-1", RemoteSubscriptionID: "sub-1"})
	waitForCalls(t, action, 1) // now blocked in Handle

	cancelDone := make(chan struct{})
	go func() {
		exec.Cancel()
		close(cancelDone)
	}()

	select {
	case <-cancelDone:
		t.Fatal("Cancel returned before the in-flight run finished")
	case <-time.After(150 * time.Millisecond):
		// expected: Cancel is still waiting on the in-flight run
	}

	g.release()

	select {
	case <-cancelDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Cancel did not return after the in-flight run finished")
	}
}

// TestLifecycleExecutor_Cancel_DiscardsNotStartedWork locks spec §5.2:
// not-yet-started (still queued) work is discarded on shutdown -- never run
// -- while already-started runs are still properly waited for. Submit after
// Cancel must be a deterministic no-op.
func TestLifecycleExecutor_Cancel_DiscardsNotStartedWork(t *testing.T) {
	hub := NewHub()
	action := newRecordingAction()
	g := newTestGate()
	action.gate = g.ch
	exec := lifecycle.NewExecutor(hub.lifecycleRegistry(), action, discardTestLogger())
	defer g.release()

	for i := 0; i < lifecycle.ExecutorWorkers; i++ {
		exec.Submit(context.Background(), lifecycle.LifecycleEvent{
			EventType: "t", EventID: fmt.Sprintf("busy-%d", i), RemoteSubscriptionID: fmt.Sprintf("sub-busy-%d", i),
		})
	}
	waitForCalls(t, action, lifecycle.ExecutorWorkers)

	// A 3rd, DISTINCT key: both workers are occupied, so this can only ever
	// sit in the queue, never start.
	exec.Submit(context.Background(), lifecycle.LifecycleEvent{EventType: "t", EventID: "never-started", RemoteSubscriptionID: "sub-never"})
	time.Sleep(20 * time.Millisecond) // let the (non-blocking) enqueue definitely land

	cancelDone := make(chan struct{})
	go func() {
		exec.Cancel()
		close(cancelDone)
	}()

	select {
	case <-cancelDone:
		t.Fatal("Cancel must still wait for the in-flight runs even though a 3rd was never started")
	case <-time.After(100 * time.Millisecond):
	}
	g.release()
	select {
	case <-cancelDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Cancel did not return")
	}

	if got := action.callCount(); got != lifecycle.ExecutorWorkers {
		t.Errorf("Handle called %d times, want %d (the never-started submission must be discarded, not run)", got, lifecycle.ExecutorWorkers)
	}

	// Submit after Cancel must be a no-op.
	exec.Submit(context.Background(), lifecycle.LifecycleEvent{EventType: "t", EventID: "post-cancel", RemoteSubscriptionID: "sub-post"})
	time.Sleep(20 * time.Millisecond)
	if got := action.callCount(); got != lifecycle.ExecutorWorkers {
		t.Errorf("Handle called %d times after Cancel()+late Submit, want unchanged %d (Submit after Cancel must be a no-op)", got, lifecycle.ExecutorWorkers)
	}
}

// TestLifecycleExecutor_ActionTimeout_BoundedAndWorkerRecovers locks spec
// §5.2: each action gets a short, bounded timeout; a timed-out action is
// treated as a failure (no retry, no re-queue) and must not wedge its
// worker -- a later, independent submission still runs.
func TestLifecycleExecutor_ActionTimeout_BoundedAndWorkerRecovers(t *testing.T) {
	saved := lifecycle.ActionTimeout
	lifecycle.ActionTimeout = 30 * time.Millisecond
	defer func() { lifecycle.ActionTimeout = saved }()

	hub := NewHub()
	var fastCalls atomic.Int64
	deadlineSeen := make(chan struct{})
	timedOut := make(chan struct{})
	var once sync.Once

	action := lifecycle.ActionFunc(func(ctx context.Context, le lifecycle.LifecycleEvent) error {
		if le.EventID == "evt-slow" {
			if _, ok := ctx.Deadline(); ok {
				once.Do(func() { close(deadlineSeen) })
			}
			<-ctx.Done() // well-behaved action: respects cancellation/timeout
			close(timedOut)
			return ctx.Err()
		}
		fastCalls.Add(1)
		return nil
	})
	exec := lifecycle.NewExecutor(hub.lifecycleRegistry(), action, discardTestLogger())
	defer exec.Cancel()

	exec.Submit(context.Background(), lifecycle.LifecycleEvent{EventType: "t", EventID: "evt-slow", RemoteSubscriptionID: "sub-slow"})

	select {
	case <-deadlineSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("action was never invoked with a deadlined context")
	}

	select {
	case <-timedOut:
	case <-time.After(2 * time.Second):
		t.Fatal("ctx.Done() never fired -- the per-action timeout did not bound the slow action")
	}

	// A second, independent key must still run afterward.
	exec.Submit(context.Background(), lifecycle.LifecycleEvent{EventType: "t", EventID: "evt-fast", RemoteSubscriptionID: "sub-fast"})
	deadline := time.Now().Add(2 * time.Second)
	for fastCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if fastCalls.Load() == 0 {
		t.Fatal("a second, independent lifecycle event never ran after a timeout -- worker pool appears wedged")
	}
}

// --- summary-only action (Task 17's pluggable-seam implementation) --------

func TestSummaryLifecycleAction_RecordsOnMatchedConsumer(t *testing.T) {
	hub := NewHub()
	c := newConnWithRemoteSub(t, 1, "sub-1")
	hub.RegisterAndIsFirst(c)

	action := lifecycle.SummaryAction(hub.lifecycleRegistry())
	le := lifecycle.LifecycleEvent{EventType: "event.subscription.suspended_v1", EventID: "evt-9", RemoteSubscriptionID: "sub-1", State: "suspended"}
	if err := action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}

	if got := c.LastLifecycleEvent(); got != le.EventType {
		t.Errorf("LastLifecycleEvent() = %q, want %q", got, le.EventType)
	}
	if got := c.LastLifecycleEventID(); got != le.EventID {
		t.Errorf("LastLifecycleEventID() = %q, want %q", got, le.EventID)
	}
	if got := c.RemoteState(); got != "suspended" {
		t.Errorf("RemoteState() = %q, want %q", got, "suspended")
	}
}

func TestSummaryLifecycleAction_Miss_RecordsNothing_NoOAPI(t *testing.T) {
	hub := NewHub() // no consumers registered at all -- a "miss"
	action := lifecycle.SummaryAction(hub.lifecycleRegistry())
	le := lifecycle.LifecycleEvent{EventType: "event.subscription.suspended_v1", EventID: "evt-9", RemoteSubscriptionID: "sub-missing", State: "suspended"}
	if err := action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v, want nil (a miss is not a failure)", err)
	}
}

func TestSummaryLifecycleAction_MultipleMatchedConsumers_AllUpdated(t *testing.T) {
	hub := NewHub()
	c1 := newConnWithRemoteSub(t, 1, "sub-shared")
	hub.RegisterAndIsFirst(c1)
	c2 := newConnWithRemoteSub(t, 2, "sub-shared")
	hub.RegisterAndIsFirst(c2)

	action := lifecycle.SummaryAction(hub.lifecycleRegistry())
	le := lifecycle.LifecycleEvent{EventType: "event.subscription.expired_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-shared"}
	if err := action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}
	if c1.LastLifecycleEvent() != le.EventType || c2.LastLifecycleEvent() != le.EventType {
		t.Errorf("both consumers sharing remote_subscription_id must be updated: c1=%q c2=%q", c1.LastLifecycleEvent(), c2.LastLifecycleEvent())
	}
}
