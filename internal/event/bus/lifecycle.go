// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/source"
)

// LifecycleEvent re-exports source.LifecycleEvent under this package's own
// name (a type ALIAS, not a new type) so the rest of this package — and its
// tests — can write LifecycleEvent unqualified. The struct itself must live
// in package source (internal/event/source/feishu.go), not here: bus.go
// already imports source (source.FeishuSource/source.Source/source.All()),
// so source must never import bus back, and FeishuSource.OnLifecycleEvent's
// parameter type has to be nameable from within package source without that
// reverse import. Do not "fix" this by moving the struct here — that would
// create an import cycle.
type LifecycleEvent = source.LifecycleEvent

const (
	// lifecycleExecutorWorkers/lifecycleExecutorSlots are the bounded
	// in-memory executor's fixed capacity (spec §5.2: "代码常量固定小容量
	// (如 worker=2、槽位=32)，纳入测试"). Deliberately code constants, not
	// env-configurable (unlike e.g. hub.go's exclusiveCleanupWaitTimeout) —
	// lifecycle handling is control-plane housekeeping, never a scalable
	// data path.
	lifecycleExecutorWorkers = 2
	lifecycleExecutorSlots   = 32
)

// lifecycleActionTimeout bounds every single action dispatch (spec §5.2:
// "每个 OAPI 动作短超时；超时=失败"). A package-level var (not a const) so
// tests can override it directly, exactly like hub.go's
// exclusiveCleanupWaitTimeout — restore the saved value when done.
var lifecycleActionTimeout = 5 * time.Second

// reasonLifecycleExecutorFull is the SetDegraded classification used when
// the bounded queue is full (spec §5.2: "记 lifecycle_executor_full").
const reasonLifecycleExecutorFull = "lifecycle_executor_full"

// lifecycleAction is the pluggable per-event action seam (Task 17/18
// boundary): Task 17 supplies ONLY summaryLifecycleAction (record
// lastLifecycleEvent/remoteState on the matched consumer(s) — no OAPI, no
// BindUser). Task 18 swaps in the real action (Reactivate/Renew/Get/
// BindUser, spec §5.3/§5.4) by constructing lifecycleExecutor with a
// different lifecycleAction — the executor mechanics below (dedup/merge/
// bounded queue/cancel/timeout) never change.
type lifecycleAction interface {
	// Handle runs ONE lifecycle event's action to completion or until ctx is
	// done (lifecycleActionTimeout). A returned error is classified and
	// logged only — the executor never retries or re-queues (spec §5.2:
	// "超时=失败...不重试不入队").
	Handle(ctx context.Context, le LifecycleEvent) error
}

// lifecycleActionFunc adapts a plain func to lifecycleAction.
type lifecycleActionFunc func(ctx context.Context, le LifecycleEvent) error

func (f lifecycleActionFunc) Handle(ctx context.Context, le LifecycleEvent) error { return f(ctx, le) }

// lifecycleExecutor is the bounded, single-shot, in-memory executor (spec
// §5.2) FeishuSource's 6 typed lifecycle handlers feed into via
// Bus.startSources wiring fs.OnLifecycleEvent = executor.Submit. No disk/
// persistence; every bit of state here is dropped on Cancel (bus shutdown)
// — a fresh bus always starts empty (spec: "退出：不落盘、不跨进程恢复；
// 后续由 status/subscription get 重新读事实").
type lifecycleExecutor struct {
	hub    *Hub
	action lifecycleAction
	// dedup is the THIRD DedupFilter domain (spec §5.2): separate from
	// Hub's legacyDedup/refinedDedup — lifecycle events never reach
	// Hub.Publish, so they need their own dedup domain here.
	dedup  *event.DedupFilter
	logger *log.Logger

	mu sync.Mutex
	// pending[remote_subscription_id] holds the LATEST not-yet-started
	// event for that id (the in-flight/pending MERGE target, spec §5.2:
	// "保最新摘要"). busy[remote_subscription_id] marks that a run for that
	// id is currently queued-or-running — Submit consults it to decide
	// whether to also send a fresh queue token or just merge into pending.
	// Task 17 has exactly one action kind, so remote_subscription_id ALONE
	// is the complete merge key (spec §5.2's "同 remote_subscription_id+同动作"
	// merge scope); if Task 18 introduces multiple concurrently-distinct
	// action kinds sharing one remote_subscription_id, extend this key to
	// remoteSubID+"\x00"+actionKind.
	pending map[string]LifecycleEvent
	busy    map[string]bool
	closed  bool

	queue chan string // bounded to lifecycleExecutorSlots; carries "ready" remote_subscription_id tokens

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newLifecycleExecutor(hub *Hub, action lifecycleAction, logger *log.Logger) *lifecycleExecutor {
	ctx, cancel := context.WithCancel(context.Background())
	e := &lifecycleExecutor{
		hub:     hub,
		action:  action,
		dedup:   event.NewDedupFilter(),
		logger:  logger,
		pending: make(map[string]LifecycleEvent),
		busy:    make(map[string]bool),
		queue:   make(chan string, lifecycleExecutorSlots),
		ctx:     ctx,
		cancel:  cancel,
	}
	for i := 0; i < lifecycleExecutorWorkers; i++ {
		e.wg.Add(1)
		go e.workerLoop()
	}
	return e
}

// Submit is FeishuSource.OnLifecycleEvent's implementation (bus.go's
// startSources wires fs.OnLifecycleEvent = executor.Submit). Runs in the
// SDK's own handler goroutine (spec §5.2: "不阻塞 SDK handler") — every
// branch below is an O(1) map/channel op, never a blocking receive or I/O.
func (e *lifecycleExecutor) Submit(ctx context.Context, le LifecycleEvent) {
	if le.EventID == "" || le.RemoteSubscriptionID == "" {
		e.logf("WARN: lifecycle event missing event_id/remote_subscription_id (type=%s); dropping", le.EventType)
		return
	}
	// RefinedDedupKey's subEventID param is forced to "" (spec §5.2: "force
	// the event_id component") so the key always falls to the
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
	if e.dedup.IsDuplicate(key) {
		return
	}

	mergeKey := le.RemoteSubscriptionID

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	e.pending[mergeKey] = le
	if e.busy[mergeKey] {
		// Already queued-or-running for this remote_subscription_id (spec
		// §5.2 merge): pending[mergeKey] above is now this submission's
		// LATEST value — whichever run drains this key next (the one
		// already scheduled, or a re-run after it finishes, see runKey)
		// picks it up. No second queue send, no blocking wait.
		e.mu.Unlock()
		return
	}
	e.busy[mergeKey] = true
	e.mu.Unlock()

	select {
	case e.queue <- mergeKey:
	default:
		// Full: never block, never pile up (spec §5.2).
		e.mu.Lock()
		delete(e.busy, mergeKey)
		delete(e.pending, mergeKey)
		e.mu.Unlock()
		e.markFull(le)
	}
}

func (e *lifecycleExecutor) workerLoop() {
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
// spec §5.2 in-flight/pending merge, without ever letting Submit itself
// block or wait.
//
// The e.closed check below (NOT workerLoop's ctx.Done() select case) is
// what makes "not-started work is discarded" (spec §5.2) deterministic:
// workerLoop's `select { case <-e.ctx.Done(): ...; case key := <-e.queue:
// ...}` can still pick the queue branch even after Cancel() has fired (Go's
// select breaks ties between simultaneously-ready cases pseudo-randomly, it
// does not prefer ctx.Done()) — so a worker freed by an in-flight run
// finishing during shutdown could otherwise win the race and start a queued
// key that was never supposed to run. Checking e.closed here, under the SAME
// mutex Cancel() sets it under, closes that race: once Cancel() has set
// closed=true, no key can begin a NEW e.runOne call, no matter which select
// branch a worker happened to take to get here. An ALREADY-RUNNING e.runOne
// (this function's own in-progress call, entered before closed was set) is
// completely unaffected — it keeps running, bounded only by its own
// lifecycleActionTimeout (spec §5.2: "已开始用自身超时结束").
func (e *lifecycleExecutor) runKey(key string) {
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
		// A newer submission arrived mid-run — process it too, still on
		// this same worker/key, still marked busy throughout (no re-queue,
		// no extra slot consumed).
	}
}

// runOne runs the pluggable action with a bounded per-action timeout (spec
// §5.2: short timeout, timeout=failure, no retry/re-queue). The timeout ctx
// is deliberately rooted in context.Background(), NOT e.ctx: Cancel()
// (shutdown) must let an ALREADY-STARTED run finish under its own timeout
// rather than killing it the instant Cancel is called (spec §5.2: "已开始
// 用自身超时结束"). What stops the executor from STARTING new work is
// e.closed (runKey's check, under e.mu) — NOT e.ctx.Done(); the latter only
// wakes an idle worker blocked on an empty queue (workerLoop's select), it
// does not reliably win a race against a queue receive that's also ready
// (see runKey's doc for why that distinction matters).
func (e *lifecycleExecutor) runOne(le LifecycleEvent) {
	ctx, cancel := context.WithTimeout(context.Background(), lifecycleActionTimeout)
	defer cancel()
	if err := e.action.Handle(ctx, le); err != nil {
		e.logf("WARN: lifecycle action failed: type=%s remote_subscription_id=%s event_id=%s: %v",
			le.EventType, le.RemoteSubscriptionID, le.EventID, err)
	}
}

// markFull records the bounded-queue-full outcome (spec §5.2: matched
// consumer(s) degraded via the reasonLifecycleExecutorFull classification;
// the handler itself never blocks — see Submit's select/default above).
func (e *lifecycleExecutor) markFull(le LifecycleEvent) {
	e.logf("WARN: %s: remote_subscription_id=%s type=%s event_id=%s dropped (executor at capacity)",
		reasonLifecycleExecutorFull, le.RemoteSubscriptionID, le.EventType, le.EventID)
	for _, c := range e.hub.connsByRemoteSubscriptionID(le.RemoteSubscriptionID) {
		c.SetDegraded(reasonLifecycleExecutorFull)
	}
}

// Cancel stops accepting new work and lets already-started runs finish under
// their own lifecycleActionTimeout (spec §5.2: "已开始用自身超时结束");
// not-yet-started (queued/pending) work is discarded once the worker
// goroutines exit — no disk, no cross-process handoff (spec §5.2: "不落盘、
// 不跨进程恢复"); a later status query re-reads the facts instead. Safe to
// call more than once (idempotent) and safe to call from Bus shutdown.
func (e *lifecycleExecutor) Cancel() {
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

func (e *lifecycleExecutor) logf(format string, args ...interface{}) {
	if e.logger != nil {
		e.logger.Printf(format, args...)
	}
}

// summaryLifecycleAction is Task 17's ONLY lifecycleAction implementation:
// it performs no remote call and no BindUser — it just records this
// lifecycle event's summary on every LOCAL consumer currently bound to the
// event's remote_subscription_id (spec §5.1). A miss (no matched consumer)
// records nothing and is not an error — this bus simply has no live
// consumer for that remote Subscription right now. Task 18 replaces this
// with the real per-event action (Reactivate/Renew/Get/BindUser, §5.3/§5.4)
// by constructing the executor with a different lifecycleAction — this
// function's SHAPE (lifecycleAction) is the seam, not this implementation.
func summaryLifecycleAction(hub *Hub) lifecycleAction {
	return lifecycleActionFunc(func(_ context.Context, le LifecycleEvent) error {
		for _, c := range hub.connsByRemoteSubscriptionID(le.RemoteSubscriptionID) {
			c.SetLifecycleSummary(le.EventType, le.EventID, le.State)
		}
		return nil
	})
}
