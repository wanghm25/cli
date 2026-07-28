// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package lifecycle

import (
	"context"

	"github.com/larksuite/cli/internal/event/model"
)

// runEffect executes the single side effect a reduction returned. The reducer
// itself performs no remote call and mutates no Conn; this is the ONE place a
// lifecycle remote call (at most one per event, never a retry) and its
// result-dependent health are applied.
func (a *SubscriptionAction) runEffect(ctx context.Context, le LifecycleEvent, effect EffectKind, res eligibilityResult, intent Intent) error {
	switch effect {
	case EffectNone:
		return nil
	case EffectReleaseEncryptKey:
		// The subscription is gone — release its cached encrypt_key from the bus
		// provider so the key does not outlive the subscription in memory.
		// Best-effort, idempotent, runs regardless of eligibility (even a miss):
		// a nil remover or unknown id is a no-op.
		if a.encryptKeyRemover != nil && le.RemoteSubscriptionID != "" {
			a.encryptKeyRemover(le.RemoteSubscriptionID)
		}
		return nil
	case EffectReactivate:
		return a.runReactivate(ctx, le, res)
	case EffectRenew:
		return a.runRenew(ctx, le, res)
	case EffectReconcileGet:
		return a.runReconcileGet(ctx, le, res, intent)
	default:
		return nil
	}
}

// runReactivate issues the SINGLE Reactivate and, on success, additionally
// requires bindConsumer for every USER conn (a bot only needs Reactivate; a
// user needs Reactivate AND bindConsumer — either failing means NOT running).
// A miss / all-ineligible input issues no remote call.
func (a *SubscriptionAction) runReactivate(ctx context.Context, le LifecycleEvent, res eligibilityResult) error {
	if len(res.conns) == 0 {
		return nil
	}
	lead := res.conns[0]
	client, err := a.buildClientForConn(ctx, lead, res.cur)
	if err != nil {
		markActionResult(res.conns, "reactivate", err)
		for _, c := range res.conns {
			c.SetSubscriptionDegraded(ReasonRemoteSubscriptionSuspended)
			c.SetSubscriptionNextAction(NextActionReactivate)
		}
		return err
	}

	_, callErr := client.Reactivate(ctx, le.RemoteSubscriptionID)
	markActionResult(res.conns, "reactivate", callErr)
	if callErr != nil {
		for _, c := range res.conns {
			c.SetSubscriptionDegraded(ReasonRemoteSubscriptionSuspended)
			c.SetSubscriptionNextAction(NextActionReactivate)
		}
		return callErr
	}

	for _, c := range res.conns {
		if c.OwnerUserOpenID() == "" {
			c.ClearSubscriptionDegraded()
			continue
		}
		// A bind failure marks the IDENTITY dimension (via bindConsumer) and
		// records rebind as THAT dimension's own recovery; the subscription
		// itself reactivated OK, so its dimension is not degraded here (and its
		// own next_action is untouched — the rebind lives on Identity).
		if bindErr := a.gate.BindConsumer(ctx, c); bindErr != nil {
			c.SetIdentityNextAction(NextActionRebind)
			continue
		}
		c.ClearSubscriptionDegraded()
	}
	return nil
}

// runRenew issues the SINGLE Renew; success clears the subscription-dimension
// fact (the remote expire_time is refreshed server-side). A miss /
// all-ineligible input issues no remote call.
func (a *SubscriptionAction) runRenew(ctx context.Context, le LifecycleEvent, res eligibilityResult) error {
	if len(res.conns) == 0 {
		return nil
	}
	lead := res.conns[0]
	client, err := a.buildClientForConn(ctx, lead, res.cur)
	if err != nil {
		markActionResult(res.conns, "renew", err)
		for _, c := range res.conns {
			c.SetSubscriptionDegraded(ReasonRemoteSubscriptionExpiringSoon)
			c.SetSubscriptionNextAction(NextActionRenew)
		}
		return err
	}

	_, callErr := client.Renew(ctx, le.RemoteSubscriptionID)
	markActionResult(res.conns, "renew", callErr)
	if callErr != nil {
		for _, c := range res.conns {
			c.SetSubscriptionDegraded(ReasonRemoteSubscriptionExpiringSoon)
			c.SetSubscriptionNextAction(NextActionRenew)
		}
		return callErr
	}
	for _, c := range res.conns {
		c.ClearSubscriptionDegraded()
	}
	return nil
}

// runReconcileGet issues the SINGLE Get for when order/compatibility is unclear
// (updated_v1) or the suspension.code isn't the one confirmed-stable value
// (suspended_v1's default branch) — "state source of truth = Get/List". The
// fetched state (not the triggering event's own, possibly-stale State)
// refreshes the summary and the reducer's phase, and drives the health outcome.
//
// An "active" result is NOT by itself proof this consumer's own local listening
// intent is still honored: the remote Subscription could have been updated
// without an observed updated_v1. So "active" additionally projects the fetched
// Subscription into the same 4-dimension shape classifyUpdateCompatibility
// compares an updated_v1's After snapshot through, and reuses that identical
// compare against the stored intent — degraded is cleared ONLY when active AND
// compatible; active-but-incompatible degrades exactly like updated_v1's own
// incompatible row (same reason, same next_action).
func (a *SubscriptionAction) runReconcileGet(ctx context.Context, le LifecycleEvent, res eligibilityResult, intent Intent) error {
	if len(res.conns) == 0 {
		return nil
	}
	lead := res.conns[0]
	client, err := a.buildClientForConn(ctx, lead, res.cur)
	if err != nil {
		markActionResult(res.conns, "get", err)
		for _, c := range res.conns {
			// Couldn't even build a client to reconcile: the subscription state
			// is unverified, so degrade the SUBSCRIPTION dimension (its own
			// next_action = get), mirroring the get-call-error path below rather
			// than leaving a next_action with no owning dimension.
			c.SetSubscriptionDegraded(reasonRemoteStateUnreconciled)
			c.SetSubscriptionNextAction(NextActionGet)
		}
		return err
	}

	sub, callErr := client.Get(ctx, le.RemoteSubscriptionID)
	markActionResult(res.conns, "get", callErr)
	if callErr != nil {
		for _, c := range res.conns {
			c.SetSubscriptionDegraded(reasonRemoteStateUnreconciled)
			c.SetSubscriptionNextAction(NextActionGet)
		}
		return callErr
	}

	var state, suspensionCode string
	if sub != nil {
		state = sub.State
		suspensionCode = sub.SuspensionReason
	}
	// Reconcile the reducer's phase to the authoritative fetched state.
	if p, ok := phaseFromState(state); ok {
		a.phases.record(le.RemoteSubscriptionID, p)
	}
	// Consulted only by the "active" branch — computed once against the shared
	// intent (every conn sharing one remote_subscription_id shares the intent).
	compatible := classifyUpdateCompatibility(projectSubscriptionCompatibility(sub), intent) == updateCompatible

	for _, c := range res.conns {
		c.SetLifecycleSummary(le.EventType, le.EventID, state)
		switch state {
		case "active":
			c.SetSuspensionReason("")
			if compatible {
				c.ClearSubscriptionDegraded()
			} else {
				c.SetSubscriptionDegraded(ReasonRemoteSubscriptionConflict)
				c.SetSubscriptionNextAction(NextActionGet)
			}
		case "suspended":
			c.SetSuspensionReason(suspensionCode)
			c.SetSubscriptionDegraded(ReasonRemoteSubscriptionSuspended)
			c.SetSubscriptionNextAction(NextActionReactivate)
		case "expired":
			c.SetSubscriptionDegraded(ReasonRemoteSubscriptionExpired)
			c.SetSubscriptionNextAction(NextActionRebuild)
		default:
			// Open vocabulary: don't guess (mirrors status.go's
			// remoteDegradedAdvisory).
		}
	}
	return nil
}

// projectSubscriptionCompatibility turns a Get's RemoteSubscription snapshot
// into the same {Authority,TargetResource,IncludeResourceData,
// PayloadOptionsPresent,Filter,FilterPresent} shape classifyUpdateCompatibility
// compares an updated_v1 event's After snapshot through — using the SAME
// authority vocabulary (RemoteAuthority.String()) a lifecycle event would carry
// — so the "active" reconcile reuses that identical 4-dimension compare rather
// than trusting state=="active" alone. sub==nil projects to the zero value:
// every dimension reads "absent", which classifyUpdateCompatibility treats as
// "unclear" rather than a confirmed match — never silently "compatible".
func projectSubscriptionCompatibility(sub *model.RemoteSubscription) LifecycleEvent {
	if sub == nil {
		return LifecycleEvent{}
	}
	le := LifecycleEvent{
		Authority:      sub.Authority.String(),
		TargetResource: sub.TargetResource,
	}
	if sub.IncludeResourceData != nil {
		le.PayloadOptionsPresent = true
		le.IncludeResourceData = *sub.IncludeResourceData
	}
	// A Get carries the authoritative filter, so this dimension is always known
	// here: the gateway projects an absent SDK filter to an empty (no-filter)
	// model — distinct from the updated_v1 path, where an absent filter means
	// "unknown".
	le.FilterPresent = true
	le.Filter = sub.Filter
	return le
}
