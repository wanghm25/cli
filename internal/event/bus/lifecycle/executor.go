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
	// ExecutorSlots bounds the number of DISTINCT remote_subscription_ids in
	// flight at once (one "ready" queue token per busy id).
	ExecutorWorkers = 2
	ExecutorSlots   = 32
	// ExecutorQueueDepth bounds the PER-remote_subscription_id FIFO backlog: how
	// many events for ONE subscription may wait, in arrival order, behind that
	// subscription's in-flight run. Reaching it drops the newest event EXPLICITLY
	// (logged + degraded, dedup NOT committed so a retry still runs), never
	// silently — the ordered reducer must not lose a middle event.
	ExecutorQueueDepth = 64
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
// with a different Action — the executor mechanics below (dedup/per-id FIFO
// queue/bounded/cancel/timeout) never change.
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
	// pending[remote_subscription_id] is that id's bounded FIFO queue of
	// not-yet-started events, held in ARRIVAL order so the reducer folds them
	// in order (true FIFO, not latest-wins — a burst of distinct events for one
	// id is each processed, never overwritten by the newest). busy[id] marks
	// that a run for that id is currently queued-or-running — Submit consults it
	// to decide whether to APPEND to that id's queue (an in-flight run will
	// drain it) or to claim a fresh queue token. One worker drains one id at a
	// time, so remote_subscription_id ALONE is the complete serialization key;
	// if multiple concurrently-distinct action kinds ever shared one id, extend
	// it to remoteSubID+"\x00"+actionKind.
	pending map[string][]LifecycleEvent
	busy    map[string]bool
	closed  bool

	queue chan string // bounded to ExecutorSlots; carries "ready" remote_subscription_id tokens (one per busy id)

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
		pending:  make(map[string][]LifecycleEvent),
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
	// (enqueued) below — never before. All Submits hold e.mu across the
	// check+commit, so the pair is atomic w.r.t. each other. The point: an event
	// dropped because a bounded queue was full is NOT recorded as seen, so a
	// later retry of the exact same key can still be processed (fixing the
	// early-commit bug where a full queue swallowed a retry).
	if e.dedup.Seen(key) {
		e.mu.Unlock()
		return
	}

	if e.busy[mergeKey] {
		// A run for this remote_subscription_id is already queued-or-running.
		// APPEND to that id's FIFO queue so it is reduced in ARRIVAL order behind
		// the events ahead of it — true FIFO, never overwriting them latest-wins.
		// Terminal-state priority is NOT enforced here (no supersede/merge): the
		// pure reducer's phase store drops a resurrection AFTER a terminal in
		// processing order, which is exactly arrival order under this FIFO.
		if len(e.pending[mergeKey]) >= ExecutorQueueDepth {
			// This id's backlog is full. Drop the newest EXPLICITLY (log +
			// degrade below) rather than growing unbounded or silently swallowing
			// a middle event — and do NOT commit dedup, so a retry still runs.
			e.mu.Unlock()
			e.markQueueFull(le)
			return
		}
		e.pending[mergeKey] = append(e.pending[mergeKey], le)
		e.dedup.Record(key) // accepted into this id's FIFO queue
		e.mu.Unlock()
		return
	}

	// Not busy: this id gets a worker. Seed its FIFO queue with this first event
	// and claim a queue token (which bounds the number of DISTINCT in-flight ids).
	e.busy[mergeKey] = true
	e.pending[mergeKey] = []LifecycleEvent{le}
	select {
	case e.queue <- mergeKey:
		e.dedup.Record(key) // accepted into the bounded queue
		e.mu.Unlock()
	default:
		// Full: too many distinct ids in flight. Never block, never pile up — and
		// do NOT commit dedup, so a retry of this exact event can still run later.
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

// runKey drains pending[key]'s FIFO queue one event at a time, IN ORDER, running
// the action for each; events that arrived (were appended) while an earlier run
// was in flight are picked up by the same loop — the ordered reducer, without
// ever letting Submit itself block or wait, and without a second worker ever
// touching this id (per-id serialization).
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
			e.logf("lifecycle executor cancelled: discarding not-started event(s) for remote_subscription_id=%s", key)
			return
		}
		q := e.pending[key]
		if len(q) == 0 {
			// Drained: release this id (clears busy so a future event re-claims a
			// worker/token for it).
			delete(e.pending, key)
			delete(e.busy, key)
			e.mu.Unlock()
			return
		}
		// FIFO: take the FRONT event; the rest stay queued in order. Events
		// appended by Submit mid-run are picked up by the next iteration.
		le := q[0]
		e.pending[key] = q[1:]
		e.mu.Unlock()

		e.runOne(le)
		// Loop: drain the next queued event for this id — still on this same
		// worker/key, still marked busy throughout (no re-queue, no extra slot).
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

// markFull records the DISTINCT-id capacity limit: too many different
// remote_subscription_ids are already in flight for a new one to claim a worker
// slot (Submit's queue-send default). The handler itself never blocks.
func (e *Executor) markFull(le LifecycleEvent) {
	e.logf("WARN: %s: remote_subscription_id=%s type=%s event_id=%s dropped (executor at capacity: too many distinct subscriptions in flight)",
		ReasonExecutorFull, le.RemoteSubscriptionID, le.EventType, le.EventID)
	e.degradeExecutorFull(le)
}

// markQueueFull records the PER-id FIFO-depth limit: one subscription's in-flight
// backlog reached ExecutorQueueDepth. The newest event is dropped EXPLICITLY —
// logged + degraded here, dedup deliberately NOT committed by Submit — so the
// ordered reducer never silently loses a middle event and a retry still runs.
func (e *Executor) markQueueFull(le LifecycleEvent) {
	e.logf("WARN: %s: remote_subscription_id=%s type=%s event_id=%s dropped (per-subscription lifecycle queue at capacity=%d)",
		ReasonExecutorFull, le.RemoteSubscriptionID, le.EventType, le.EventID, ExecutorQueueDepth)
	e.degradeExecutorFull(le)
}

// degradeExecutorFull marks the matched consumer(s) degraded via the shared
// ReasonExecutorFull classification and sets an explicit management next_action:
// the dropped event could have been ANY of the 6 lifecycle types, so rather than
// presuming a specific fix (e.g. "reactivate", which would be wrong if the
// dropped event were actually expired/deleted), NextActionGet points at the
// safe, universally-applicable "go read the current state" step
// (`event subscription get` / `event status`).
func (e *Executor) degradeExecutorFull(le LifecycleEvent) {
	for _, c := range e.registry.ConnsByRemoteSubscriptionID(le.RemoteSubscriptionID) {
		c.SetSubscriptionDegraded(ReasonExecutorFull)
		c.SetSubscriptionNextAction(NextActionGet)
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
	discarded := 0
	for _, q := range e.pending {
		discarded += len(q)
	}
	e.pending = make(map[string][]LifecycleEvent)
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
