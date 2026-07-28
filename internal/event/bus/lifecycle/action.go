// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package lifecycle

import (
	"context"
	"errors"
	"log"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/model"
	"github.com/larksuite/cli/internal/event/session"
)

// SummaryAction is one Action implementation: it performs no remote call and
// no BindUser — it just records this lifecycle event's summary on every LOCAL
// consumer currently bound to the event's remote_subscription_id. A miss (no
// matched consumer) records nothing and is not an error — this bus simply has
// no live consumer for that remote Subscription right now. The real per-event
// action (Reactivate/Renew/Get/BindUser) replaces this by constructing the
// executor with a different Action — this function's SHAPE (Action) is the
// seam, not this implementation.
func SummaryAction(registry Registry) Action {
	return ActionFunc(func(_ context.Context, le LifecycleEvent) error {
		for _, c := range registry.ConnsByRemoteSubscriptionID(le.RemoteSubscriptionID) {
			c.SetLifecycleSummary(le.EventType, le.EventID, le.State)
		}
		return nil
	})
}

// classifyLifecycleActionError maps an OAPI action failure to a short, reusable
// classification (reuse the typed error classification, mapping permission
// problems to missing_scopes) — never a raw error string (which could leak
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
// outcome on every conn in conns (last_action/last_action_error) — always both
// together, success (err==nil) included, so a fresh attempt always supersedes a
// stale error from a previous one.
func markActionResult(conns []Conn, action string, err error) {
	errStr := classifyLifecycleActionError(err)
	for _, c := range conns {
		c.SetLastAction(action)
		c.SetLastActionError(errStr)
	}
}

// classifyUpdateCompatibility compares the updated_v1 event's AFTER snapshot
// against lead's own stored local listening intent: target_resource,
// include_resource_data, authority, and the server-side filter. A remote change
// to just the subscription's target_resource, payload_options, or filter — with
// Authority left untouched — must not be mis-judged as compatible.
//
// Each of the four dimensions is checked independently: an EMPTY/absent value
// on the EVENT side (le.Authority=="", le.TargetResource=="",
// !le.PayloadOptionsPresent, or !le.FilterPresent) means that ONE dimension
// can't be judged, but never by itself forces "unclear" — a CONFIRMED mismatch
// found on any OTHER dimension is conclusive on its own and wins immediately (no
// reason to wait on a Get to double-check a dimension we already know
// disagrees). The filter is compared with event.Equal (a struct, not a scalar):
// a nil intent and a nil remote filter are equal ("no filter" both sides), and a
// nil-vs-set pair is a confirmed mismatch. Only when NONE of the four dimensions
// produces a confirmed mismatch, but at least one couldn't be judged, does this
// return "unclear" (the reducer then returns EffectReconcileGet and the caller
// issues a single Get to reconcile — see runReconcileGet) instead of defaulting
// to "compatible".
func classifyUpdateCompatibility(le LifecycleEvent, intent Intent) string {
	unclear := false

	switch {
	case le.Authority == "":
		unclear = true
	case !AuthorityMatchesOwner(le.Authority, intent.OwnerUserOpenID):
		return updateIncompatible
	}

	switch {
	case le.TargetResource == "":
		unclear = true
	case le.TargetResource != intent.TargetResource:
		return updateIncompatible
	}

	switch {
	case !le.PayloadOptionsPresent:
		unclear = true
	case le.IncludeResourceData != intent.IncludeResourceData:
		return updateIncompatible
	}

	switch {
	case !le.FilterPresent:
		unclear = true
	case !event.Equal(le.Filter, intent.Filter):
		return updateIncompatible
	}

	if unclear {
		return updateUnclear
	}
	return updateCompatible
}

// AuthorityMatchesOwner reports whether authority (an already-normalized
// "user:<open_id>" / "app" wire-vocabulary string — see
// source.FormatLifecycleAuthority / RawEvent.Authority) matches an owner's
// own fixed identity, expressed as ownerUserOpenID ("" for a bot or legacy
// owner — the same discriminator OwnerUserOpenID() uses everywhere else in
// this codebase). The comparison itself now lives in
// internal/event/model.AuthorityMatchesOwner — the one place either "user:"+id
// or "app" is spelled out — so the lifecycle update-compatibility check and the
// routing cross-check share it and can never drift apart. This is kept as a
// thin facade for the existing lifecycle call sites. authority=="" is unclear
// and must never reach here; callers gate on that themselves.
func AuthorityMatchesOwner(authority, ownerUserOpenID string) bool {
	return model.AuthorityMatchesOwner(authority, ownerUserOpenID)
}

// errIdentityGateUnconfigured/errSubscriptionClientUnconfigured are returned
// (never panicked) when a dependency SubscriptionAction needs hasn't been wired
// yet (bus.go's SetIdentityProviders/SetSubscriptionClient not called) — this
// is the expected, tested state for a summary-only bus and any bus not yet
// given credentials, so it must degrade the specific consumer involved, never
// crash the executor's worker goroutine.
var (
	errIdentityGateUnconfigured       = errors.New("lifecycle action: no identity gate configured")
	errSubscriptionClientUnconfigured = errors.New("lifecycle action: no subscription client configured")
)

// eligibilityResult is eligibleConns's output: conns is the subset of the input
// allowed to trigger a remote action or BindUser; cur is the current identity
// resolveCurrent produced (zero value if no user conn ever required resolving
// it, e.g. an all-bot match).
type eligibilityResult struct {
	conns []Conn
	cur   session.CurrentIdentity
}

// SubscriptionAction is the REAL Action: the per-event switch, the
// owner==current security gate, single-action-per-event Reactivate/Renew/Get,
// and the deleted-tombstone. It still performs the summary recording (extended:
// see Handle) so it is a strict superset, never a regression, of SummaryAction.
type SubscriptionAction struct {
	registry Registry
	logger   *log.Logger

	gate         IdentityGate
	newSubClient func(as core.Identity, uat string) (SubscriptionClient, error)

	// encryptKeyRemover releases a subscription's cached encrypt_key from the
	// bus-side provider when its remote Subscription is gone: on deleted_v1
	// (handleDeleted). nil until wired by bus.go's NewBus — a bus with no
	// provider configured (never, in production) simply skips the release.
	// Releasing the key on deleted_v1 ensures the key does not outlive the
	// subscription in memory.
	encryptKeyRemover func(subID string)

	// phases is the reducer's per-remote_subscription_id state (the ordered
	// fold's current Phase per id, TTL-bounded). It is the source of truth for
	// terminal-state priority.
	phases *phaseStore
}

// NewSubscriptionAction constructs the action with its deps unconfigured
// (gate/newSubClient nil) — bus.go's NewBus builds this once, then
// SetIdentityProviders/SetSubscriptionClient fill the two dependencies in
// post-construction via SetIdentityGate/SetNewSubscriptionClient below.
func NewSubscriptionAction(registry Registry, logger *log.Logger) *SubscriptionAction {
	return &SubscriptionAction{registry: registry, logger: logger, phases: newPhaseStore()}
}

// SetIdentityGate wires the owner==current gate + bindConsumer dependency.
func (a *SubscriptionAction) SetIdentityGate(g IdentityGate) { a.gate = g }

// SetNewSubscriptionClient wires the factory that builds the ONE remote client
// each action allows, bound to a resolved identity.
func (a *SubscriptionAction) SetNewSubscriptionClient(fn func(as core.Identity, uat string) (SubscriptionClient, error)) {
	a.newSubClient = fn
}

// SetEncryptKeyRemover wires the deleted_v1 encrypt-key release.
func (a *SubscriptionAction) SetEncryptKeyRemover(fn func(subID string)) {
	a.encryptKeyRemover = fn
}

// IdentityGate returns the wired identity gate (nil until SetIdentityGate).
func (a *SubscriptionAction) IdentityGate() IdentityGate { return a.gate }

// NewSubscriptionClientFunc returns the wired client factory (nil until
// SetNewSubscriptionClient).
func (a *SubscriptionAction) NewSubscriptionClientFunc() func(as core.Identity, uat string) (SubscriptionClient, error) {
	return a.newSubClient
}

func (a *SubscriptionAction) logf(format string, args ...interface{}) {
	if a.logger != nil {
		a.logger.Printf(format, args...)
	}
}

// Handle implements Action as an ordered per-remote_subscription_id reducer +
// effect executor. The steps:
//
//  1. eligibleConns applies the IDENTITY-dimension gate (owner==current): every
//     ineligible USER conn is marked stale/unresolved and excluded; bot/legacy
//     conns are always eligible. This is a pure identity concern, kept out of
//     the subscription reducer.
//  2. reduce() — the PURE fold — turns (current phase, event, eligible-intent)
//     into (new phase, health decision, single effect). It performs no remote
//     call and mutates no Conn. Terminal-state priority lives here: a
//     resurrection against a terminal phase yields a DROPPED reduction.
//  3. A DROPPED event is a no-op: it returns here WITHOUT recording the summary,
//     so a terminal-tombstoned late activated/updated can never flip the status
//     summary's remote_state back to active or overwrite last_event. The summary
//     must reflect ONLY events the reducer accepted — hence it is written AFTER
//     reduce (below), not before it.
//  4. The summary is recorded on every matched conn, hit or miss, eligible or
//     not (deleted_v1 synthesizes an explicit "deleted" state since its body
//     carries none).
//  5. Handle APPLIES the reduction's Conn mutations (suspension bookkeeping +
//     the subscription-dimension health decision) and records the new phase.
//  6. Handle RUNS the returned effect (Reactivate/Renew/Get/release-key). The
//     effect issues the single remote call and applies its result-dependent
//     health — the ONLY place a remote call or a client-driven Conn mutation
//     happens.
func (a *SubscriptionAction) Handle(ctx context.Context, le LifecycleEvent) error {
	conns := a.registry.ConnsByRemoteSubscriptionID(le.RemoteSubscriptionID)

	res := a.eligibleConns(conns)
	cur := a.phases.phaseOf(le.RemoteSubscriptionID)
	red := reduce(cur, le, intentOf(res.conns))

	if red.dropped {
		// Terminal tombstone hit: NOTHING is recorded — not the summary
		// (remote_state/last_event stay terminal), not the phase, no effect.
		a.logf("lifecycle: dropping %s for remote_subscription_id=%s (terminal phase — not revived; summary untouched)",
			le.EventType, le.RemoteSubscriptionID)
		return nil
	}

	// Summary reflects only accepted events — recorded AFTER reduce.
	summaryState := le.State
	if le.EventType == LifecycleEventTypeDeleted {
		summaryState = "deleted"
	}
	for _, c := range conns {
		c.SetLifecycleSummary(le.EventType, le.EventID, summaryState)
	}

	if red.suspension != nil {
		for _, c := range conns {
			c.SetSuspensionReason(*red.suspension)
		}
	}
	scopeConns := res.conns
	if red.scope == scopeAll {
		scopeConns = conns
	}
	applySubDecision(scopeConns, red.sub)
	a.phases.record(le.RemoteSubscriptionID, red.Phase)

	return a.runEffect(ctx, le, red.effect, res, intentOf(res.conns))
}

// applySubDecision applies a reduction's subscription-dimension health decision
// to conns — the ONLY place (besides an effect runner) the Subscription health
// fact is mutated for a reduction.
func applySubDecision(conns []Conn, d subDecision) {
	switch d.op {
	case subSet:
		for _, c := range conns {
			c.SetSubscriptionDegraded(d.reason)
			c.SetSubscriptionNextAction(d.next)
		}
	case subClearIfSuspended:
		for _, c := range conns {
			if c.SubscriptionDegradedReason() == ReasonRemoteSubscriptionSuspended {
				c.ClearSubscriptionDegraded()
			}
		}
	case subClearIfConflict:
		for _, c := range conns {
			if c.SubscriptionDegradedReason() == ReasonRemoteSubscriptionConflict {
				c.ClearSubscriptionDegraded()
			}
		}
	case subLeave:
		// nothing
	}
}

// intentOf builds the shared listening Intent from the lead eligible conn (all
// conns sharing one remote_subscription_id are expected to share one intent);
// the zero Intent when there is no eligible conn.
func intentOf(conns []Conn) Intent {
	if len(conns) == 0 {
		return Intent{}
	}
	lead := conns[0]
	return Intent{
		OwnerUserOpenID:     lead.OwnerUserOpenID(),
		TargetResource:      lead.TargetResource(),
		IncludeResourceData: lead.IncludeResourceDataIntent(),
		Filter:              lead.FilterIntent(),
	}
}

// eligibleConns splits conns into those allowed to trigger a REMOTE action or
// BindUser (the security red line): a bot/legacy conn (OwnerUserOpenID()=="") is
// ALWAYS eligible — the same precedent hub.go's Publish gate and identity.go's
// onConnReady already establish (bot/legacy consumers are NEVER
// identity-gated), since there is no separate "historical bot user" concept to
// mis-recover into. A USER conn is eligible ONLY when a FRESHLY resolved current
// identity (never cached, never UAT) matches its fixed owner — routed through
// the shared session.Gate exactly like the other three gates. Every ineligible
// USER conn is marked (SetStaleIdentity on a mismatch;
// SetIdentityDegraded(session.ReasonCurrentIdentityUnresolved) if resolveCurrent
// itself failed or no identity gate is configured at all) but otherwise left completely
// untouched: no remote call, no BindUser, no historical UAT load.
func (a *SubscriptionAction) eligibleConns(conns []Conn) eligibilityResult {
	var res eligibilityResult
	var curErr error
	var curResolved bool

	for _, c := range conns {
		if c.OwnerUserOpenID() == "" {
			res.conns = append(res.conns, c)
			continue
		}
		if !curResolved {
			if a.gate == nil {
				curErr = errIdentityGateUnconfigured
			} else {
				res.cur, curErr = a.gate.ResolveCurrent()
			}
			curResolved = true
		}
		owner := model.OwnerRef{AppID: c.OwnerAppID(), UserOpenID: c.OwnerUserOpenID()}
		switch session.Gate(owner, res.cur, curErr) {
		case session.AdmitUnresolved:
			c.SetIdentityDegraded(session.ReasonCurrentIdentityUnresolved)
		case session.AdmitStale:
			c.SetStaleIdentity()
		default: // AdmitDeliver
			res.conns = append(res.conns, c)
		}
	}
	return res
}

// buildClientForConn resolves the ONE SubscriptionClient this event's single
// remote action will use, bound to c's OWN identity — bot -> core.AsBot, no
// uat; user -> core.AsUser with a FRESH uat minted for cur (never a historical
// identity — c is only ever passed here after eligibleConns already admitted it
// through the shared session.Gate against cur).
func (a *SubscriptionAction) buildClientForConn(ctx context.Context, c Conn, cur session.CurrentIdentity) (SubscriptionClient, error) {
	if a.newSubClient == nil {
		return nil, errSubscriptionClientUnconfigured
	}
	if c.OwnerUserOpenID() == "" {
		return a.newSubClient(core.AsBot, "")
	}
	uat, err := a.gate.ResolveUAT(ctx, cur.AppID, cur.UserOpenID)
	if err != nil {
		return nil, err
	}
	return a.newSubClient(core.AsUser, uat)
}
