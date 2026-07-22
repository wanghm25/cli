// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/core"
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
// Also sets the §5.2 explicit management next_action (Task 18 review Minor
// 2): the dropped event could have been ANY of the 6 lifecycle types, so
// rather than presuming a specific fix (e.g. "reactivate", which would be
// wrong if the dropped event were actually expired/deleted), nextActionGet
// points at the safe, universally-applicable "go read the current state"
// step (`event subscription get` / `event status`).
func (e *lifecycleExecutor) markFull(le LifecycleEvent) {
	e.logf("WARN: %s: remote_subscription_id=%s type=%s event_id=%s dropped (executor at capacity)",
		reasonLifecycleExecutorFull, le.RemoteSubscriptionID, le.EventType, le.EventID)
	for _, c := range e.hub.connsByRemoteSubscriptionID(le.RemoteSubscriptionID) {
		c.SetDegraded(reasonLifecycleExecutorFull)
		c.SetNextAction(nextActionGet)
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

// ============================================================================
// Task 18: subscriptionLifecycleAction — the REAL per-event action
// (spec §5.3/§5.4/§5.5), including the §8 owner==current security gate and
// the deleted-tombstone. Constructed once by bus.go's NewBus and wired into
// newLifecycleExecutor in place of summaryLifecycleAction; its two remote
// dependencies (identityGate, the SubscriptionClient factory) start nil and
// are filled in post-construction by bus.go's SetIdentityProviders/
// SetSubscriptionClient (mirrors those methods' own "optional, call before
// Run()" convention) — so a Bus that never wires either (every Task 17 test,
// and any bus not yet given credentials) still gets full summary recording
// with zero remote calls and zero panics.
// ============================================================================

// subscriptionActionClient is the subset of *eventlib.SubscriptionClient
// (internal/event/subscription_client.go) this action needs: a single Get
// (state-source-of-truth reconcile, spec §5.3's "顺序不明/未知 code -> Get
// 一次") and single Reactivate/Renew (spec §5.3/§5.4's "at most ONE such
// remote call per event, never a retry"). Declared here — narrower than
// SubscriptionClient's full Create/Get/List/Patch/Renew/Reactivate/Delete
// surface — purely as a test seam: *eventlib.SubscriptionClient satisfies it
// structurally (Go interfaces are duck-typed), so production code passes it
// straight through, while tests substitute a fake with no *lark.Client or
// network call involved (mirrors cmd/event/subscription's own per-command
// XxxAPI test-seam interfaces, e.g. reactivateSubscriptionAPI).
type subscriptionActionClient interface {
	Get(ctx context.Context, req *larkeventv1.GetSubscriptionReq) (*larkeventv1.GetSubscriptionResp, error)
	Reactivate(ctx context.Context, req *larkeventv1.ReactivateSubscriptionReq) (*larkeventv1.ReactivateSubscriptionResp, error)
	Renew(ctx context.Context, req *larkeventv1.RenewSubscriptionReq) (*larkeventv1.RenewSubscriptionResp, error)
}

// Event type strings, mirrored from source/feishu.go's identically-named,
// identically-valued but UNEXPORTED constants (lifecycleEventTypeActivated
// et al.) — package bus cannot reference those directly since they're
// private to package source, and re-exporting them there is out of this
// task's sanctioned feishu.go scope (comment-only change, see the
// "never memoize" doc update). These 6 strings are SDK facts (spec §5.1),
// stable across this whole feature — if they ever change, both copies must
// be updated together.
const (
	lifecycleEventTypeActivated          = "event.subscription.activated_v1"
	lifecycleEventTypeUpdated            = "event.subscription.updated_v1"
	lifecycleEventTypeSuspended          = "event.subscription.suspended_v1"
	lifecycleEventTypeExpirationReminder = "event.subscription.expiration_reminder_v1"
	lifecycleEventTypeExpired            = "event.subscription.expired_v1"
	lifecycleEventTypeDeleted            = "event.subscription.deleted_v1"
)

// suspensionCodeAuthorityRevoked is the ONLY confirmed stable suspension.code
// value (spec §5.4). suspension.code is otherwise an open string CLI never
// builds a closed enum for (source/feishu.go's LifecycleEvent.SuspensionCode
// doc) — any OTHER value (including a future, currently-unknown one) takes
// the §5.4 "default branch" (reconcileWithGet) rather than guessing that
// Reactivate is the right recovery action.
const suspensionCodeAuthorityRevoked = "authority_revoked"

// tombstoneTTL bounds how long a deleted remote_subscription_id is
// remembered, purely in-memory, so a late/out-of-order activated_v1 or
// updated_v1 for the SAME id arriving shortly after a deleted_v1 cannot
// resurrect it (spec §5.3). A package var (not const) so tests shrink it;
// deliberately a fixed default rather than env-configurable — mirrors
// lifecycleExecutorWorkers/Slots's own "control-plane housekeeping, not a
// scalable data path" rationale (this file's earlier doc comment).
var tombstoneTTL = 10 * time.Minute

// tombstoneStore is a TTL in-memory map[remote_subscription_id]expiry
// (spec §5.3). Purely in-memory: a bus restart clears it entirely, exactly
// like the lifecycleExecutor's own pending/busy maps — there is nothing to
// persist or recover across a process boundary here (spec §5.2's "不落盘、
// 不跨进程恢复" applies equally to this bookkeeping).
type tombstoneStore struct {
	mu     sync.Mutex
	expiry map[string]time.Time
}

func newTombstoneStore() *tombstoneStore {
	return &tombstoneStore{expiry: make(map[string]time.Time)}
}

// mark records/refreshes a live tombstone for remoteSubID (called on every
// deleted_v1, hit or miss — spec §5.3's table: BOTH columns "保 TTL 内存
// tombstone").
func (t *tombstoneStore) mark(remoteSubID string) {
	if remoteSubID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.expiry[remoteSubID] = time.Now().Add(tombstoneTTL)
}

// isLive reports whether remoteSubID currently has an unexpired tombstone,
// lazily evicting an expired entry it happens to find (bounded map growth
// without a separate background sweep).
func (t *tombstoneStore) isLive(remoteSubID string) bool {
	if remoteSubID == "" {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	exp, ok := t.expiry[remoteSubID]
	if !ok {
		return false
	}
	if time.Now().After(exp) {
		delete(t.expiry, remoteSubID)
		return false
	}
	return true
}

// --- degraded/next_action classification tokens (spec §5.5) ---------------
// Reused (never per-branch bespoke strings) so a status display can key off
// a small, stable vocabulary. reasonRemoteSubscriptionConflict's exact
// string is spec-literal ("不兼容标 degraded/remote_subscription_conflict");
// the others are Task 18's own short, consistent classifications, chosen to
// mirror the existing bind_failed:*/current_identity_unresolved/
// lifecycle_executor_full style already established by Task 14/17.
const (
	reasonRemoteSubscriptionConflict     = "remote_subscription_conflict"
	reasonRemoteSubscriptionSuspended    = "remote_subscription_suspended"
	reasonRemoteSubscriptionExpired      = "remote_subscription_expired"
	reasonRemoteSubscriptionExpiringSoon = "remote_subscription_expiring_soon"
	reasonRemoteSubscriptionDeleted      = "remote_subscription_deleted"
	reasonRemoteStateUnreconciled        = "remote_state_unreconciled"
)

// next_action tokens. Spec §5.5: "恢复命令统一 reactivate" — nextActionReactivate
// is used verbatim, literally spelled "reactivate", never "reactive"/"resume".
const (
	nextActionReactivate = "reactivate"
	nextActionRenew      = "renew"
	nextActionRebuild    = "rebuild" // expired/deleted: guide to rebuild, never auto
	nextActionRebind     = "rebind"  // a local BindUser is needed/failed
	nextActionGet        = "get"     // inspect current remote state before deciding
)

// classifyLifecycleActionError maps an OAPI action failure to a short,
// reusable classification (spec §5.5: "不新增私有错误码；OAPI 失败复用 typed
// 分类；权限问题 missing_scopes") — never a raw error string (which could leak
// upstream detail into a status display) and never a bespoke new code.
func classifyLifecycleActionError(err error) string {
	if err == nil {
		return ""
	}
	if errs.IsPermission(err) {
		return "missing_scopes"
	}
	if p, ok := errs.ProblemOf(err); ok {
		if p.Subtype != "" {
			return string(p.Subtype)
		}
		return string(p.Category)
	}
	return string(errs.SubtypeUnknown)
}

// markActionResult records which action was attempted and its classified
// outcome on every conn in conns (spec §5.5's last_action/last_action_error)
// — always both together, success (err==nil) included, so a fresh attempt
// always supersedes a stale error from a previous one.
func markActionResult(conns []*Conn, action string, err error) {
	errStr := classifyLifecycleActionError(err)
	for _, c := range conns {
		c.SetLastAction(action)
		c.SetLastActionError(errStr)
	}
}

// updateCompatibility classification (spec §5.3's updated_v1 row).
const (
	updateCompatible   = "compatible"
	updateIncompatible = "incompatible"
	updateUnclear      = "unclear"
)

// classifyUpdateCompatibility compares the updated_v1 event's Authority
// against lead's OWN fixed owner identity — the one piece of "local
// listening intent" (spec §5.3: "比较 Before/After 与本地意图") every matched
// consumer structurally carries, without Task 18 introducing a separate
// stored target_resource/authority-at-registration snapshot (deliberately
// out of this task's Conn field list). An empty Authority (the event didn't
// carry one, or the SDK payload couldn't be normalized into one) is always
// "unclear" — never guessed compatible or incompatible.
func classifyUpdateCompatibility(le LifecycleEvent, lead *Conn) string {
	if le.Authority == "" {
		return updateUnclear
	}
	if lead.OwnerUserOpenID() == "" {
		if le.Authority == "app" {
			return updateCompatible
		}
		return updateIncompatible
	}
	if le.Authority == "user:"+lead.OwnerUserOpenID() {
		return updateCompatible
	}
	return updateIncompatible
}

// errIdentityGateUnconfigured/errSubscriptionClientUnconfigured are returned
// (never panicked) when a dependency subscriptionLifecycleAction needs
// hasn't been wired yet (bus.go's SetIdentityProviders/SetSubscriptionClient
// not called) — this is the expected, tested state for every Task 17 test
// and any bus not yet given credentials, so it must degrade the specific
// consumer involved, never crash the executor's worker goroutine.
var (
	errIdentityGateUnconfigured       = errors.New("lifecycle action: no identity gate configured")
	errSubscriptionClientUnconfigured = errors.New("lifecycle action: no subscription client configured")
)

// eligibilityResult is eligibleConns's output: conns is the subset of the
// input allowed to trigger a remote action or BindUser (spec §8); cur is the
// current identity resolveCurrent produced (zero value if no user conn ever
// required resolving it, e.g. an all-bot match).
type eligibilityResult struct {
	conns []*Conn
	cur   currentIdentity
}

// subscriptionLifecycleAction is Task 18's REAL lifecycleAction: the
// per-event switch, the §8 owner==current security gate, single-action-per-
// event Reactivate/Renew/Get, and the deleted-tombstone. It still performs
// Task 17's summary recording (extended: see Handle) so it is a strict
// superset, never a regression, of summaryLifecycleAction.
type subscriptionLifecycleAction struct {
	hub    *Hub
	logger *log.Logger

	identityGate *identityGate
	newSubClient func(as core.Identity, uat string) (subscriptionActionClient, error)

	tombstone *tombstoneStore
}

// newSubscriptionLifecycleAction constructs the action with its deps
// unconfigured (identityGate/newSubClient nil) — bus.go's NewBus builds this
// once, then SetIdentityProviders/SetSubscriptionClient fill the two
// dependencies in post-construction via setIdentityGate/
// setNewSubscriptionClient below.
func newSubscriptionLifecycleAction(hub *Hub, logger *log.Logger) *subscriptionLifecycleAction {
	return &subscriptionLifecycleAction{hub: hub, logger: logger, tombstone: newTombstoneStore()}
}

func (a *subscriptionLifecycleAction) setIdentityGate(g *identityGate) { a.identityGate = g }

func (a *subscriptionLifecycleAction) setNewSubscriptionClient(fn func(as core.Identity, uat string) (subscriptionActionClient, error)) {
	a.newSubClient = fn
}

func (a *subscriptionLifecycleAction) logf(format string, args ...interface{}) {
	if a.logger != nil {
		a.logger.Printf(format, args...)
	}
}

// Handle implements lifecycleAction (spec §5.3/§5.4/§8). Every lifecycle
// event, hit or miss, eligible or not, ALWAYS gets its summary recorded
// first (Task 17 behavior, extended: deleted_v1 synthesizes an explicit
// "deleted" state since its body carries none — spec §5.3 "删 active 快照")
// — then a live deleted-tombstone drops a resurrecting activated_v1/
// updated_v1 before any further processing, then the per-event switch runs.
func (a *subscriptionLifecycleAction) Handle(ctx context.Context, le LifecycleEvent) error {
	conns := a.hub.connsByRemoteSubscriptionID(le.RemoteSubscriptionID)

	summaryState := le.State
	if le.EventType == lifecycleEventTypeDeleted {
		summaryState = "deleted"
	}
	for _, c := range conns {
		c.SetLifecycleSummary(le.EventType, le.EventID, summaryState)
	}

	if a.isTombstonedResurrection(le) {
		a.logf("lifecycle: dropping %s for remote_subscription_id=%s (tombstoned after an earlier deleted_v1, spec §5.3)",
			le.EventType, le.RemoteSubscriptionID)
		return nil
	}

	switch le.EventType {
	case lifecycleEventTypeActivated:
		return a.handleActivated(ctx, conns)
	case lifecycleEventTypeUpdated:
		return a.handleUpdated(ctx, le, conns)
	case lifecycleEventTypeSuspended:
		return a.handleSuspended(ctx, le, conns)
	case lifecycleEventTypeExpirationReminder:
		return a.handleExpirationReminder(ctx, le, conns)
	case lifecycleEventTypeExpired:
		return a.handleExpired(conns)
	case lifecycleEventTypeDeleted:
		return a.handleDeleted(le, conns)
	default:
		return nil // an unrecognized event type: summary already recorded above.
	}
}

func (a *subscriptionLifecycleAction) isTombstonedResurrection(le LifecycleEvent) bool {
	if le.EventType != lifecycleEventTypeActivated && le.EventType != lifecycleEventTypeUpdated {
		return false
	}
	return a.tombstone.isLive(le.RemoteSubscriptionID)
}

// eligibleConns splits conns into those allowed to trigger a REMOTE action
// or BindUser (spec §8's red line): a bot/legacy conn (OwnerUserOpenID()==
// "") is ALWAYS eligible — the same Task 14 precedent hub.go's Publish gate
// and identity.go's onConnReady already establish (bot/legacy consumers are
// NEVER identity-gated), since there is no separate "historical bot user"
// concept to mis-recover into. A USER conn is eligible ONLY when a FRESHLY
// resolved current identity (never cached, never UAT) matches its fixed
// owner — reusing ownerMatchesCurrent/resolveCurrent exactly. Every
// ineligible USER conn is marked (SetStaleIdentity on a mismatch;
// SetDegraded(reasonCurrentIdentityUnresolved) if resolveCurrent itself
// failed or no identity gate is configured at all) but otherwise left
// completely untouched: no remote call, no BindUser, no historical UAT load.
func (a *subscriptionLifecycleAction) eligibleConns(conns []*Conn) eligibilityResult {
	var res eligibilityResult
	var curErr error
	var curResolved bool

	for _, c := range conns {
		if c.OwnerUserOpenID() == "" {
			res.conns = append(res.conns, c)
			continue
		}
		if !curResolved {
			if a.identityGate == nil {
				curErr = errIdentityGateUnconfigured
			} else {
				res.cur, curErr = a.identityGate.resolveCurrent()
			}
			curResolved = true
		}
		if curErr != nil {
			c.SetDegraded(reasonCurrentIdentityUnresolved)
			continue
		}
		if !ownerMatchesCurrent(c.OwnerAppID(), c.OwnerUserOpenID(), res.cur) {
			c.SetStaleIdentity()
			continue
		}
		res.conns = append(res.conns, c)
	}
	return res
}

// buildClientForConn resolves the ONE SubscriptionClient this event's single
// remote action will use, bound to c's OWN identity — bot -> core.AsBot, no
// uat; user -> core.AsUser with a FRESH uat minted for cur (never a
// historical identity — c is only ever passed here after eligibleConns
// already verified ownerMatchesCurrent(c, cur), spec §8).
func (a *subscriptionLifecycleAction) buildClientForConn(ctx context.Context, c *Conn, cur currentIdentity) (subscriptionActionClient, error) {
	if a.newSubClient == nil {
		return nil, errSubscriptionClientUnconfigured
	}
	if c.OwnerUserOpenID() == "" {
		return a.newSubClient(core.AsBot, "")
	}
	uat, err := a.identityGate.resolveUAT(ctx, cur.appID, cur.userOpenID)
	if err != nil {
		return nil, err
	}
	return a.newSubClient(core.AsUser, uat)
}

// --- activated --------------------------------------------------------------

// handleActivated implements spec §5.3's activated_v1 row: store active +
// clear suspension (informational bookkeeping, applies to EVERY matched
// conn regardless of eligibility — no remote call/BindUser is involved in
// just clearing a local field); a user consumer additionally needs
// bindConsumer to succeed before being considered running again, else it
// stays degraded awaiting a rebind. A miss never builds a consumer (spec:
// "更新摘要，不建 consumer").
func (a *subscriptionLifecycleAction) handleActivated(ctx context.Context, conns []*Conn) error {
	if len(conns) == 0 {
		return nil
	}
	for _, c := range conns {
		c.SetSuspensionReason("")
	}
	res := a.eligibleConns(conns)
	for _, c := range res.conns {
		if c.OwnerUserOpenID() == "" {
			c.clearActionDegraded()
			continue
		}
		if err := a.identityGate.bindConsumer(ctx, c); err != nil {
			c.SetNextAction(nextActionRebind)
			continue
		}
		c.clearActionDegraded()
	}
	return nil
}

// --- updated -----------------------------------------------------------------

// handleUpdated implements spec §5.3's updated_v1 row. A miss just keeps the
// After summary already recorded by Handle. On a hit, only ELIGIBLE conns
// (spec §8 — this Get is a remote call too, never issued on behalf of a
// historical/non-current identity) are classified compatible/incompatible/
// unclear against the event's Authority.
func (a *subscriptionLifecycleAction) handleUpdated(ctx context.Context, le LifecycleEvent, conns []*Conn) error {
	if len(conns) == 0 {
		return nil
	}
	res := a.eligibleConns(conns)
	if len(res.conns) == 0 {
		return nil
	}
	switch classifyUpdateCompatibility(le, res.conns[0]) {
	case updateCompatible:
		for _, c := range res.conns {
			if c.DegradedReason() == reasonRemoteSubscriptionConflict {
				c.clearActionDegraded()
			}
		}
		return nil
	case updateIncompatible:
		for _, c := range res.conns {
			c.SetDegraded(reasonRemoteSubscriptionConflict)
			c.SetNextAction(nextActionGet)
		}
		return nil
	default: // order unclear (spec: "顺序不明 Get 一次")
		return a.reconcileWithGet(ctx, le, res)
	}
}

// --- suspended ---------------------------------------------------------------

// handleSuspended implements spec §5.3's suspended_v1 row + §5.4's recovery
// rule. suspension.code is ALWAYS recorded verbatim on every matched conn,
// hit or miss, eligible or not (bookkeeping, not an action). A miss or an
// ineligible match (owner != current, or no identity gate configured) never
// Reactivates or BindUsers — the §8 red line. Only the ONE confirmed stable
// code (authority_revoked) auto-Reactivates; any other value takes the
// §5.4 default branch (a single Get reconcile, never a guessed action).
func (a *subscriptionLifecycleAction) handleSuspended(ctx context.Context, le LifecycleEvent, conns []*Conn) error {
	for _, c := range conns {
		c.SetSuspensionReason(le.SuspensionCode)
	}
	if len(conns) == 0 {
		return nil
	}
	res := a.eligibleConns(conns)
	if len(res.conns) == 0 {
		return nil
	}
	if le.SuspensionCode != suspensionCodeAuthorityRevoked {
		return a.reconcileWithGet(ctx, le, res)
	}
	return a.reactivateAndMaybeBind(ctx, le, res)
}

// reactivateAndMaybeBind issues the SINGLE Reactivate call and, on success,
// additionally requires bindConsumer for every USER conn (spec §5.4: bot
// only needs Reactivate; user needs Reactivate AND bindConsumer — either
// failing means NOT running).
func (a *subscriptionLifecycleAction) reactivateAndMaybeBind(ctx context.Context, le LifecycleEvent, res eligibilityResult) error {
	if len(res.conns) == 0 {
		return nil
	}
	lead := res.conns[0]
	client, err := a.buildClientForConn(ctx, lead, res.cur)
	if err != nil {
		markActionResult(res.conns, "reactivate", err)
		for _, c := range res.conns {
			c.SetDegraded(reasonRemoteSubscriptionSuspended)
			c.SetNextAction(nextActionReactivate)
		}
		return err
	}

	req := larkeventv1.NewReactivateSubscriptionReqBuilder().SubscriptionId(le.RemoteSubscriptionID).Build()
	_, callErr := client.Reactivate(ctx, req)
	markActionResult(res.conns, "reactivate", callErr)
	if callErr != nil {
		for _, c := range res.conns {
			c.SetDegraded(reasonRemoteSubscriptionSuspended)
			c.SetNextAction(nextActionReactivate)
		}
		return callErr
	}

	for _, c := range res.conns {
		if c.OwnerUserOpenID() == "" {
			c.clearActionDegraded()
			continue
		}
		if bindErr := a.identityGate.bindConsumer(ctx, c); bindErr != nil {
			c.SetNextAction(nextActionRebind)
			continue
		}
		c.clearActionDegraded()
	}
	return nil
}

// --- expiration_reminder -----------------------------------------------------

// handleExpirationReminder implements spec §5.3's expiration_reminder_v1
// row: a SINGLE Renew on a hit+eligible match; success clears any prior
// degraded state (the remote expire_time itself is refreshed server-side —
// no local field caches it, spec RemoteSubscriptionInfo.ExpireTime is a
// status.go-only, separately-fetched concept).
func (a *subscriptionLifecycleAction) handleExpirationReminder(ctx context.Context, le LifecycleEvent, conns []*Conn) error {
	if len(conns) == 0 {
		return nil
	}
	res := a.eligibleConns(conns)
	if len(res.conns) == 0 {
		return nil
	}
	lead := res.conns[0]
	client, err := a.buildClientForConn(ctx, lead, res.cur)
	if err != nil {
		markActionResult(res.conns, "renew", err)
		for _, c := range res.conns {
			c.SetDegraded(reasonRemoteSubscriptionExpiringSoon)
			c.SetNextAction(nextActionRenew)
		}
		return err
	}

	req := larkeventv1.NewRenewSubscriptionReqBuilder().SubscriptionId(le.RemoteSubscriptionID).Build()
	_, callErr := client.Renew(ctx, req)
	markActionResult(res.conns, "renew", callErr)
	if callErr != nil {
		for _, c := range res.conns {
			c.SetDegraded(reasonRemoteSubscriptionExpiringSoon)
			c.SetNextAction(nextActionRenew)
		}
		return callErr
	}
	for _, c := range res.conns {
		c.clearActionDegraded()
	}
	return nil
}

// --- expired / deleted: local bookkeeping only, NEVER a remote call --------

// handleExpired implements spec §5.3's expired_v1 row: degraded, guide
// rebuild, and — unconditionally, regardless of eligibility — NO Renew, NO
// auto-Reactivate. Applying this to every matched conn (even one whose
// owner != current) is safe: it is pure local bookkeeping, never a remote
// call or BindUser, so it isn't the kind of "action" spec §8 gates.
func (a *subscriptionLifecycleAction) handleExpired(conns []*Conn) error {
	for _, c := range conns {
		c.SetDegraded(reasonRemoteSubscriptionExpired)
		c.SetNextAction(nextActionRebuild)
	}
	return nil
}

// handleDeleted implements spec §5.3's deleted_v1 row: delete the active
// snapshot (Handle already synthesized remoteState="deleted"), degraded,
// NO rebuild — and tombstones remoteSubID for BOTH hit and miss (spec's
// table: both columns "保 TTL 内存 tombstone"), so a late/out-of-order
// activated_v1/updated_v1 for the same id cannot resurrect it.
func (a *subscriptionLifecycleAction) handleDeleted(le LifecycleEvent, conns []*Conn) error {
	a.tombstone.mark(le.RemoteSubscriptionID)
	for _, c := range conns {
		c.SetDegraded(reasonRemoteSubscriptionDeleted)
		c.SetNextAction(nextActionRebuild)
	}
	return nil
}

// --- reconcile (single Get) --------------------------------------------------

// reconcileWithGet issues the SINGLE Get spec §5.3/§5.4 calls for when
// order/compatibility is unclear (updated_v1) or the suspension.code isn't
// the one confirmed stable value (suspended_v1's default branch) — "state
// source of truth = Get/List, never second-level update_time". The fetched
// state (not the triggering event's own, possibly-stale State) refreshes
// the summary and, for the states this SDK's suspension model actually
// documents (mirrors status.go's remoteDegradedAdvisory: only "suspended"/
// "expired" are special-cased; everything else, including "active" or any
// future open-vocabulary value, is left alone rather than guessed at).
func (a *subscriptionLifecycleAction) reconcileWithGet(ctx context.Context, le LifecycleEvent, res eligibilityResult) error {
	if len(res.conns) == 0 {
		return nil
	}
	lead := res.conns[0]
	client, err := a.buildClientForConn(ctx, lead, res.cur)
	if err != nil {
		markActionResult(res.conns, "get", err)
		for _, c := range res.conns {
			c.SetNextAction(nextActionGet)
		}
		return err
	}

	req := larkeventv1.NewGetSubscriptionReqBuilder().SubscriptionId(le.RemoteSubscriptionID).Build()
	resp, callErr := client.Get(ctx, req)
	markActionResult(res.conns, "get", callErr)
	if callErr != nil {
		for _, c := range res.conns {
			c.SetDegraded(reasonRemoteStateUnreconciled)
			c.SetNextAction(nextActionGet)
		}
		return callErr
	}

	var state, suspensionCode string
	if resp != nil && resp.Data != nil && resp.Data.Subscription != nil {
		d := resp.Data.Subscription
		if d.State != nil {
			state = *d.State
		}
		if d.Suspension != nil && d.Suspension.Code != nil {
			suspensionCode = *d.Suspension.Code
		}
	}
	for _, c := range res.conns {
		c.SetLifecycleSummary(le.EventType, le.EventID, state)
		switch state {
		case "active":
			c.SetSuspensionReason("")
			c.clearActionDegraded()
		case "suspended":
			c.SetSuspensionReason(suspensionCode)
			c.SetDegraded(reasonRemoteSubscriptionSuspended)
			c.SetNextAction(nextActionReactivate)
		case "expired":
			c.SetDegraded(reasonRemoteSubscriptionExpired)
			c.SetNextAction(nextActionRebuild)
		default:
			// Open vocabulary: don't guess (mirrors status.go's
			// remoteDegradedAdvisory).
		}
	}
	return nil
}
