// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package lifecycle

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/larksuite/cli/internal/event"
)

const (
	// ExecutorWorkers/ExecutorSlots are the bounded in-memory executor's fixed
	// capacity. Deliberately code constants, not env-configurable — lifecycle
	// handling is control-plane housekeeping, never a scalable data path.
	ExecutorWorkers = 2
	ExecutorSlots   = 32
)

// ActionTimeout bounds every single action dispatch with a short timeout
// (timeout = failure). A package-level var (not a const) so tests can override
// it directly; restore the saved value when done.
var ActionTimeout = 5 * time.Second

// ReasonExecutorFull is the subscription-dimension classification used when the
// bounded queue is full (recorded as lifecycle_executor_full).
const ReasonExecutorFull = "lifecycle_executor_full"

// Action is the pluggable per-event action seam. One implementation
// (SummaryAction) only records lastLifecycleEvent/remoteState on the matched
// consumer(s) — no OAPI, no BindUser. The real action (SubscriptionAction:
// Reactivate/Renew/Get/BindUser) is swapped in by constructing the Executor
// with a different Action — the executor mechanics below (dedup/merge/bounded
// queue/cancel/timeout) never change.
type Action interface {
	// Handle runs ONE lifecycle event's action to completion or until ctx is
	// done (ActionTimeout). A returned error is classified and logged only —
	// the executor never retries or re-queues.
	Handle(ctx context.Context, le LifecycleEvent) error
}

// ActionFunc adapts a plain func to Action.
type ActionFunc func(ctx context.Context, le LifecycleEvent) error

func (f ActionFunc) Handle(ctx context.Context, le LifecycleEvent) error { return f(ctx, le) }

// Executor is the bounded, single-shot, in-memory executor FeishuSource's 6
// typed lifecycle handlers feed into via Bus.startSources wiring
// fs.OnLifecycleEvent = executor.Submit. No disk/persistence; every bit of
// state here is dropped on Cancel (bus shutdown) — a fresh bus always starts
// empty; a later status/subscription get re-reads the facts.
type Executor struct {
	registry Registry
	action   Action
	// dedup is the THIRD DedupFilter domain: separate from Hub's
	// legacyDedup/refinedDedup — lifecycle events never reach Hub.Publish, so
	// they need their own dedup domain here.
	dedup  *event.DedupFilter
	logger *log.Logger

	mu sync.Mutex
	// pending[remote_subscription_id] holds the LATEST not-yet-started event
	// for that id (the in-flight/pending MERGE target that keeps the latest
	// summary). busy[remote_subscription_id] marks that a run for that id is
	// currently queued-or-running — Submit consults it to decide whether to
	// also send a fresh queue token or just merge into pending. There is
	// currently one action kind, so remote_subscription_id ALONE is the
	// complete merge key (the merge scope is "same remote_subscription_id +
	// same action"); if multiple concurrently-distinct action kinds sharing one
	// remote_subscription_id, extend this key to
	// remoteSubID+"\x00"+actionKind.
	pending map[string]LifecycleEvent
	busy    map[string]bool
	closed  bool

	queue chan string // bounded to ExecutorSlots; carries "ready" remote_subscription_id tokens

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewExecutor constructs the executor over registry and action, and starts its
// bounded worker pool.
func NewExecutor(registry Registry, action Action, logger *log.Logger) *Executor {
	ctx, cancel := context.WithCancel(context.Background())
	e := &Executor{
		registry: registry,
		action:   action,
		dedup:    event.NewDedupFilter(),
		logger:   logger,
		pending:  make(map[string]LifecycleEvent),
		busy:     make(map[string]bool),
		queue:    make(chan string, ExecutorSlots),
		ctx:      ctx,
		cancel:   cancel,
	}
	for i := 0; i < ExecutorWorkers; i++ {
		e.wg.Add(1)
		go e.workerLoop()
	}
	return e
}

// Submit is FeishuSource.OnLifecycleEvent's implementation (bus.go's
// startSources wires fs.OnLifecycleEvent = executor.Submit). Runs in the
// SDK's own handler goroutine (never blocking it) — every branch below is an
// O(1) map/channel op, never a blocking receive or I/O.
func (e *Executor) Submit(ctx context.Context, le LifecycleEvent) {
	if le.EventID == "" || le.RemoteSubscriptionID == "" {
		e.logf("WARN: lifecycle event missing event_id/remote_subscription_id (type=%s); dropping", le.EventType)
		return
	}
	// RefinedDedupKey's subEventID param is forced to "" (to force the
	// event_id component) so the key always falls to the
	// remote_subscription_id+event_id branch, never subscription_event_id
	// (that field belongs to the CONSUMER-delivery dedup domain, hub.go's
	// refinedDedup — a different domain entirely).
	key, ok := event.RefinedDedupKey(le.RemoteSubscriptionID, "", le.EventID)
	if !ok {
		// Unreachable given the validation above (remoteSubID+eventID both
		// non-empty guarantees ok==true) — stay defensive rather than panic.
		e.logf("WARN: lifecycle event could not build a dedup key (type=%s remote_subscription_id=%s); dropping", le.EventType, le.RemoteSubscriptionID)
		return
	}

	mergeKey := le.RemoteSubscriptionID

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	// Dedup is CHECKED here but committed only AFTER the event is accepted
	// (queued / merged / superseded) below — never before. All Submits hold
	// e.mu across the check+commit, so the pair is atomic w.r.t. each other. The
	// point: an event dropped because the bounded queue was full is NOT recorded
	// as seen, so a later retry of the exact same key can still be processed
	// (fixing the early-commit bug where a full queue swallowed a retry).
	if e.dedup.Seen(key) {
		e.mu.Unlock()
		return
	}

	// Terminal-aware merge: never let a resurrection (activated/updated)
	// overwrite an already-pending terminal (deleted/expired) event before it is
	// processed. The executor stays action-agnostic — it only asks the Action
	// (when it implements SupersedePolicy) whether to keep the pending event.
	if prev, has := e.pending[mergeKey]; has {
		if sp, okp := e.action.(SupersedePolicy); okp && sp.KeepPending(prev, le) {
			e.dedup.Record(key) // accepted-and-superseded: keep prev pending, don't re-run this
			e.mu.Unlock()
			return
		}
	}

	e.pending[mergeKey] = le
	if e.busy[mergeKey] {
		// Already queued-or-running for this remote_subscription_id (a merge):
		// pending[mergeKey] above is now this submission's LATEST value —
		// whichever run drains this key next picks it up. No second queue send.
		e.dedup.Record(key) // accepted via in-flight merge
		e.mu.Unlock()
		return
	}
	e.busy[mergeKey] = true
	select {
	case e.queue <- mergeKey:
		e.dedup.Record(key) // accepted into the bounded queue
		e.mu.Unlock()
	default:
		// Full: never block, never pile up — and do NOT commit dedup, so a retry
		// of this exact event can still be processed later.
		delete(e.busy, mergeKey)
		delete(e.pending, mergeKey)
		e.mu.Unlock()
		e.markFull(le)
	}
}

func (e *Executor) workerLoop() {
	defer e.wg.Done()
	for {
		select {
		case <-e.ctx.Done():
			return
		case key, ok := <-e.queue:
			if !ok {
				return
			}
			e.runKey(key)
		}
	}
}

// runKey drains pending[key] and runs the action; if a NEWER event arrived
// while that run was in flight, it loops to pick that up too — this is the
// in-flight/pending merge, without ever letting Submit itself block or wait.
//
// The e.closed check below (NOT workerLoop's ctx.Done() select case) is what
// makes "not-started work is discarded" deterministic: workerLoop's `select {
// case <-e.ctx.Done(): ...; case key := <-e.queue: ...}` can still pick the
// queue branch even after Cancel() has fired (Go's select breaks ties between
// simultaneously-ready cases pseudo-randomly, it does not prefer ctx.Done()) —
// so a worker freed by an in-flight run finishing during shutdown could
// otherwise win the race and start a queued key that was never supposed to
// run. Checking e.closed here, under the SAME mutex Cancel() sets it under,
// closes that race: once Cancel() has set closed=true, no key can begin a NEW
// e.runOne call, no matter which select branch a worker happened to take to get
// here. An ALREADY-RUNNING e.runOne (this function's own in-progress call,
// entered before closed was set) is completely unaffected — it keeps running,
// bounded only by its own ActionTimeout.
func (e *Executor) runKey(key string) {
	for {
		e.mu.Lock()
		if e.closed {
			delete(e.pending, key)
			delete(e.busy, key)
			e.mu.Unlock()
			e.logf("lifecycle executor cancelled: discarding not-started event for remote_subscription_id=%s", key)
			return
		}
		le, ok := e.pending[key]
		delete(e.pending, key)
		e.mu.Unlock()
		if !ok {
			e.mu.Lock()
			delete(e.busy, key)
			e.mu.Unlock()
			return
		}

		e.runOne(le)

		e.mu.Lock()
		if _, again := e.pending[key]; !again {
			delete(e.busy, key)
			e.mu.Unlock()
			return
		}
		e.mu.Unlock()
		// A newer submission arrived mid-run — process it too, still on this
		// same worker/key, still marked busy throughout (no re-queue, no extra
		// slot consumed).
	}
}

// runOne runs the pluggable action with a bounded per-action timeout (short
// timeout, timeout=failure, no retry/re-queue). The timeout ctx is deliberately
// rooted in context.Background(), NOT e.ctx: Cancel() (shutdown) must let an
// ALREADY-STARTED run finish under its own timeout rather than killing it the
// instant Cancel is called. What stops the executor from STARTING new work is
// e.closed (runKey's check, under e.mu) — NOT e.ctx.Done(); the latter only
// wakes an idle worker blocked on an empty queue (workerLoop's select), it does
// not reliably win a race against a queue receive that's also ready (see
// runKey's doc for why that distinction matters).
func (e *Executor) runOne(le LifecycleEvent) {
	ctx, cancel := context.WithTimeout(context.Background(), ActionTimeout)
	defer cancel()
	if err := e.action.Handle(ctx, le); err != nil {
		e.logf("WARN: lifecycle action failed: type=%s remote_subscription_id=%s event_id=%s: %v",
			le.EventType, le.RemoteSubscriptionID, le.EventID, err)
	}
}

// markFull records the bounded-queue-full outcome (matched consumer(s) degraded
// via the ReasonExecutorFull classification; the handler itself never blocks —
// see Submit's select/default above). Also sets an explicit management
// next_action: the dropped event could have been ANY of the 6 lifecycle types,
// so rather than presuming a specific fix (e.g. "reactivate", which would be
// wrong if the dropped event were actually expired/deleted), NextActionGet
// points at the safe, universally-applicable "go read the current state" step
// (`event subscription get` / `event status`).
func (e *Executor) markFull(le LifecycleEvent) {
	e.logf("WARN: %s: remote_subscription_id=%s type=%s event_id=%s dropped (executor at capacity)",
		ReasonExecutorFull, le.RemoteSubscriptionID, le.EventType, le.EventID)
	for _, c := range e.registry.ConnsByRemoteSubscriptionID(le.RemoteSubscriptionID) {
		c.SetSubscriptionDegraded(ReasonExecutorFull)
		c.SetNextAction(NextActionGet)
	}
}

// Cancel stops accepting new work and lets already-started runs finish under
// their own ActionTimeout; not-yet-started (queued/pending) work is discarded
// once the worker goroutines exit — no disk, no cross-process handoff; a later
// status query re-reads the facts instead. Safe to call more than once
// (idempotent) and safe to call from Bus shutdown.
func (e *Executor) Cancel() {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	e.closed = true
	e.mu.Unlock()

	e.cancel()  // stop worker loops from picking up new keys off the queue
	e.wg.Wait() // let already-started runs finish (bounded by their own timeout)

	e.mu.Lock()
	discarded := len(e.pending)
	e.pending = make(map[string]LifecycleEvent)
	e.busy = make(map[string]bool)
	e.mu.Unlock()
	if discarded > 0 {
		e.logf("lifecycle executor cancelled: discarded %d not-started event(s)", discarded)
	}
}

func (e *Executor) logf(format string, args ...interface{}) {
	if e.logger != nil {
		e.logger.Printf(format, args...)
	}
}
