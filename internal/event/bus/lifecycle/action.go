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
// return "unclear" (the caller then issues a single Get to reconcile — see
// reconcileWithGet) instead of defaulting to "compatible".
func classifyUpdateCompatibility(le LifecycleEvent, lead Conn) string {
	unclear := false

	switch {
	case le.Authority == "":
		unclear = true
	case !authorityMatchesConn(le.Authority, lead):
		return updateIncompatible
	}

	switch {
	case le.TargetResource == "":
		unclear = true
	case le.TargetResource != lead.TargetResource():
		return updateIncompatible
	}

	switch {
	case !le.PayloadOptionsPresent:
		unclear = true
	case le.IncludeResourceData != lead.IncludeResourceDataIntent():
		return updateIncompatible
	}

	switch {
	case !le.FilterPresent:
		unclear = true
	case !event.Equal(le.Filter, lead.FilterIntent()):
		return updateIncompatible
	}

	if unclear {
		return updateUnclear
	}
	return updateCompatible
}

// authorityMatchesConn reports whether authority (the updated_v1 event's
// already-normalized After.Authority, e.g. "user:ou_xxx"/"app") matches lead's
// OWN fixed owner identity — the same "app" vs "user:<open_id>" vocabulary
// source/feishu.go's formatLifecycleAuthority/formatSubscriptionAuthority
// already establish. authority=="" (unclear) must never reach here —
// classifyUpdateCompatibility's own switch guards that.
func authorityMatchesConn(authority string, lead Conn) bool {
	return AuthorityMatchesOwner(authority, lead.OwnerUserOpenID())
}

// AuthorityMatchesOwner reports whether authority (an already-normalized
// "user:<open_id>" / "app" wire-vocabulary string — see
// source.FormatLifecycleAuthority / RawEvent.Authority) matches an owner's
// own fixed identity, expressed as ownerUserOpenID ("" for a bot or legacy
// owner — the same discriminator OwnerUserOpenID() uses everywhere else in
// this codebase). Exported so a host package (e.g. the hub's own
// delivery-time cross-check) can reuse this exact comparison — the one
// place either "user:"+id or "app" is spelled out — rather than
// re-deriving it and risking the two copies drifting apart. authority=="" is
// unclear and must never reach here; callers gate on that themselves.
func AuthorityMatchesOwner(authority, ownerUserOpenID string) bool {
	if ownerUserOpenID == "" {
		return authority == "app"
	}
	return authority == "user:"+ownerUserOpenID
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

	tombstone *tombstoneStore
}

// NewSubscriptionAction constructs the action with its deps unconfigured
// (gate/newSubClient nil) — bus.go's NewBus builds this once, then
// SetIdentityProviders/SetSubscriptionClient fill the two dependencies in
// post-construction via SetIdentityGate/SetNewSubscriptionClient below.
func NewSubscriptionAction(registry Registry, logger *log.Logger) *SubscriptionAction {
	return &SubscriptionAction{registry: registry, logger: logger, tombstone: newTombstoneStore()}
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

// Handle implements Action. Every lifecycle event, hit or miss, eligible or
// not, ALWAYS gets its summary recorded first (extended: deleted_v1 synthesizes
// an explicit "deleted" state since its body carries none) — then a live
// deleted-tombstone drops a resurrecting activated_v1/updated_v1 before any
// further processing, then the per-event switch runs.
func (a *SubscriptionAction) Handle(ctx context.Context, le LifecycleEvent) error {
	conns := a.registry.ConnsByRemoteSubscriptionID(le.RemoteSubscriptionID)

	summaryState := le.State
	if le.EventType == LifecycleEventTypeDeleted {
		summaryState = "deleted"
	}
	for _, c := range conns {
		c.SetLifecycleSummary(le.EventType, le.EventID, summaryState)
	}

	if a.isTombstonedResurrection(le) {
		a.logf("lifecycle: dropping %s for remote_subscription_id=%s (tombstoned after an earlier deleted_v1)",
			le.EventType, le.RemoteSubscriptionID)
		return nil
	}

	switch le.EventType {
	case lifecycleEventTypeActivated:
		return a.handleActivated(conns)
	case lifecycleEventTypeUpdated:
		return a.handleUpdated(ctx, le, conns)
	case lifecycleEventTypeSuspended:
		return a.handleSuspended(ctx, le, conns)
	case lifecycleEventTypeExpirationReminder:
		return a.handleExpirationReminder(ctx, le, conns)
	case lifecycleEventTypeExpired:
		return a.handleExpired(conns)
	case LifecycleEventTypeDeleted:
		return a.handleDeleted(le, conns)
	default:
		return nil // an unrecognized event type: summary already recorded above.
	}
}

func (a *SubscriptionAction) isTombstonedResurrection(le LifecycleEvent) bool {
	if le.EventType != lifecycleEventTypeActivated && le.EventType != lifecycleEventTypeUpdated {
		return false
	}
	return a.tombstone.isLive(le.RemoteSubscriptionID)
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
// SetDegraded(session.ReasonCurrentIdentityUnresolved) if resolveCurrent itself
// failed or no identity gate is configured at all) but otherwise left completely
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
			c.SetDegraded(session.ReasonCurrentIdentityUnresolved)
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
