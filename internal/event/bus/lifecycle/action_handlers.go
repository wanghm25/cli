// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package lifecycle

import (
	"context"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/internal/event/source"
)

// --- activated --------------------------------------------------------------

// handleActivated implements the activated_v1 row: clear the suspension
// bookkeeping on EVERY matched conn (informational — the remote subscription is
// no longer suspended), and clear a prior suspension-degraded state for the
// eligible conns whose owner still matches current. It NEVER calls
// bindConsumer: receiving activated_v1 does not prove the subscription is
// locally started, so BindUser is left strictly to the three places that DO
// prove it — a new local consume start (onConnReady), a CLI-executed Reactivate
// recovery (reactivateAndMaybeBind), and a WS reconnect (onConnReady). No
// remote call is issued either. A miss never builds a consumer.
//
// Event DELIVERY to a user consumer stays gated by owner==current on every
// fan-out (hub.go Publish), independent of BindUser — so clearing the
// suspension-degraded flag here is advisory only and never opens a delivery
// path for a mismatched owner (eligibleConns marks those stale instead).
func (a *SubscriptionAction) handleActivated(conns []Conn) error {
	if len(conns) == 0 {
		return nil
	}
	for _, c := range conns {
		c.SetSuspensionReason("")
	}
	res := a.eligibleConns(conns)
	for _, c := range res.conns {
		if c.DegradedReason() == ReasonRemoteSubscriptionSuspended {
			c.ClearActionDegraded()
		}
	}
	return nil
}

// --- updated -----------------------------------------------------------------

// handleUpdated implements the updated_v1 row. A miss just keeps the After
// summary already recorded by Handle. On a hit, only ELIGIBLE conns (this Get
// is a remote call too, never issued on behalf of a historical/non-current
// identity) are classified compatible/incompatible/unclear against the event's
// Authority.
func (a *SubscriptionAction) handleUpdated(ctx context.Context, le LifecycleEvent, conns []Conn) error {
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
			if c.DegradedReason() == ReasonRemoteSubscriptionConflict {
				c.ClearActionDegraded()
			}
		}
		return nil
	case updateIncompatible:
		for _, c := range res.conns {
			c.SetDegraded(ReasonRemoteSubscriptionConflict)
			c.SetNextAction(NextActionGet)
		}
		return nil
	default: // order unclear: Get once
		return a.reconcileWithGet(ctx, le, res)
	}
}

// --- suspended ---------------------------------------------------------------

// handleSuspended implements the suspended_v1 row + recovery rule.
// suspension.code is ALWAYS recorded verbatim on every matched conn, hit or
// miss, eligible or not (bookkeeping, not an action). A miss or an ineligible
// match (owner != current, or no identity gate configured) never Reactivates or
// BindUsers — the security red line. Only the ONE confirmed stable code
// (authority_revoked) auto-Reactivates; any other value takes the default
// branch (a single Get reconcile, never a guessed action).
func (a *SubscriptionAction) handleSuspended(ctx context.Context, le LifecycleEvent, conns []Conn) error {
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
// additionally requires bindConsumer for every USER conn (bot only needs
// Reactivate; user needs Reactivate AND bindConsumer — either failing means NOT
// running).
func (a *SubscriptionAction) reactivateAndMaybeBind(ctx context.Context, le LifecycleEvent, res eligibilityResult) error {
	if len(res.conns) == 0 {
		return nil
	}
	lead := res.conns[0]
	client, err := a.buildClientForConn(ctx, lead, res.cur)
	if err != nil {
		markActionResult(res.conns, "reactivate", err)
		for _, c := range res.conns {
			c.SetDegraded(ReasonRemoteSubscriptionSuspended)
			c.SetNextAction(NextActionReactivate)
		}
		return err
	}

	req := larkeventv1.NewReactivateSubscriptionReqBuilder().SubscriptionId(le.RemoteSubscriptionID).Build()
	_, callErr := client.Reactivate(ctx, req)
	markActionResult(res.conns, "reactivate", callErr)
	if callErr != nil {
		for _, c := range res.conns {
			c.SetDegraded(ReasonRemoteSubscriptionSuspended)
			c.SetNextAction(NextActionReactivate)
		}
		return callErr
	}

	for _, c := range res.conns {
		if c.OwnerUserOpenID() == "" {
			c.ClearActionDegraded()
			continue
		}
		if bindErr := a.gate.BindConsumer(ctx, c); bindErr != nil {
			c.SetNextAction(NextActionRebind)
			continue
		}
		c.ClearActionDegraded()
	}
	return nil
}

// --- expiration_reminder -----------------------------------------------------

// handleExpirationReminder implements the expiration_reminder_v1 row: a SINGLE
// Renew on a hit+eligible match; success clears any prior degraded state (the
// remote expire_time itself is refreshed server-side — no local field caches
// it; protocol.RemoteSubscriptionInfo.ExpireTime is a status.go-only,
// separately-fetched concept).
func (a *SubscriptionAction) handleExpirationReminder(ctx context.Context, le LifecycleEvent, conns []Conn) error {
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
			c.SetDegraded(ReasonRemoteSubscriptionExpiringSoon)
			c.SetNextAction(NextActionRenew)
		}
		return err
	}

	req := larkeventv1.NewRenewSubscriptionReqBuilder().SubscriptionId(le.RemoteSubscriptionID).Build()
	_, callErr := client.Renew(ctx, req)
	markActionResult(res.conns, "renew", callErr)
	if callErr != nil {
		for _, c := range res.conns {
			c.SetDegraded(ReasonRemoteSubscriptionExpiringSoon)
			c.SetNextAction(NextActionRenew)
		}
		return callErr
	}
	for _, c := range res.conns {
		c.ClearActionDegraded()
	}
	return nil
}

// --- expired / deleted: local bookkeeping only, NEVER a remote call --------

// handleExpired implements the expired_v1 row: degraded, guide rebuild, and —
// unconditionally, regardless of eligibility — NO Renew, NO auto-Reactivate.
// Applying this to every matched conn (even one whose owner != current) is
// safe: it is pure local bookkeeping, never a remote call or BindUser, so it
// isn't the kind of "action" the security gate covers.
func (a *SubscriptionAction) handleExpired(conns []Conn) error {
	for _, c := range conns {
		c.SetDegraded(ReasonRemoteSubscriptionExpired)
		c.SetNextAction(NextActionRebuild)
	}
	return nil
}

// handleDeleted implements the deleted_v1 row: delete the active snapshot
// (Handle already synthesized remoteState="deleted"), degraded, NO rebuild —
// and tombstones remoteSubID for BOTH hit and miss (both table columns keep a
// TTL in-memory tombstone), so a late/out-of-order activated_v1/updated_v1 for
// the same id cannot resurrect it.
func (a *SubscriptionAction) handleDeleted(le LifecycleEvent, conns []Conn) error {
	a.tombstone.mark(le.RemoteSubscriptionID)
	// The subscription is gone — release its cached encrypt_key from the bus
	// provider so the key does not outlive the subscription in memory.
	// Best-effort, idempotent, and never fails the event: a nil remover (no
	// provider wired) or an unknown id is a no-op.
	if a.encryptKeyRemover != nil && le.RemoteSubscriptionID != "" {
		a.encryptKeyRemover(le.RemoteSubscriptionID)
	}
	for _, c := range conns {
		c.SetDegraded(ReasonRemoteSubscriptionDeleted)
		c.SetNextAction(NextActionRebuild)
	}
	return nil
}

// --- reconcile (single Get) --------------------------------------------------

// reconcileWithGet issues the SINGLE Get for when order/compatibility is
// unclear (updated_v1) or the suspension.code isn't the one confirmed stable
// value (suspended_v1's default branch) — "state source of truth = Get/List,
// never second-level update_time". The fetched state (not the triggering
// event's own, possibly-stale State) refreshes the summary and, for the states
// this SDK's suspension model actually documents (mirrors status.go's
// remoteDegradedAdvisory: only "suspended"/"expired" are special-cased;
// everything else, including "active" or any future open-vocabulary value, is
// left alone rather than guessed at).
//
// An "active" result is NOT, by itself, proof this consumer's own local
// listening intent is still honored: the remote Subscription could have been
// updated (target_resource/authority/include_resource_data) without ever
// producing an observed updated_v1 (e.g. this bus was offline when it fired).
// So "active" additionally projects the fetched Subscription into the same
// 3-dimension shape classifyUpdateCompatibility already compares an
// updated_v1's After snapshot through, and reuses that identical compare
// against lead's stored intent — degraded is cleared ONLY when active AND
// compatible; active-but-incompatible degrades exactly like updated_v1's own
// incompatible row (same reason, same next_action), never silently clearing
// a real conflict just because the state happens to read "active".
func (a *SubscriptionAction) reconcileWithGet(ctx context.Context, le LifecycleEvent, res eligibilityResult) error {
	if len(res.conns) == 0 {
		return nil
	}
	lead := res.conns[0]
	client, err := a.buildClientForConn(ctx, lead, res.cur)
	if err != nil {
		markActionResult(res.conns, "get", err)
		for _, c := range res.conns {
			c.SetNextAction(NextActionGet)
		}
		return err
	}

	req := larkeventv1.NewGetSubscriptionReqBuilder().SubscriptionId(le.RemoteSubscriptionID).Build()
	resp, callErr := client.Get(ctx, req)
	markActionResult(res.conns, "get", callErr)
	if callErr != nil {
		for _, c := range res.conns {
			c.SetDegraded(reasonRemoteStateUnreconciled)
			c.SetNextAction(NextActionGet)
		}
		return callErr
	}

	var state, suspensionCode string
	var subscription *larkeventv1.SubscriptionDetail
	if resp != nil && resp.Data != nil && resp.Data.Subscription != nil {
		subscription = resp.Data.Subscription
		if subscription.State != nil {
			state = *subscription.State
		}
		if subscription.Suspension != nil && subscription.Suspension.Code != nil {
			suspensionCode = *subscription.Suspension.Code
		}
	}
	// Only consulted by the "active" branch below — computed once against
	// lead (mirrors handleUpdated's own "classify once, apply to every eligible
	// conn" pattern, since every conn sharing one remote_subscription_id is
	// expected to share the same local listening intent).
	compatible := classifyUpdateCompatibility(projectSubscriptionCompatibility(subscription), lead) == updateCompatible

	for _, c := range res.conns {
		c.SetLifecycleSummary(le.EventType, le.EventID, state)
		switch state {
		case "active":
			c.SetSuspensionReason("")
			if compatible {
				c.ClearActionDegraded()
			} else {
				c.SetDegraded(ReasonRemoteSubscriptionConflict)
				c.SetNextAction(NextActionGet)
			}
		case "suspended":
			c.SetSuspensionReason(suspensionCode)
			c.SetDegraded(ReasonRemoteSubscriptionSuspended)
			c.SetNextAction(NextActionReactivate)
		case "expired":
			c.SetDegraded(ReasonRemoteSubscriptionExpired)
			c.SetNextAction(NextActionRebuild)
		default:
			// Open vocabulary: don't guess (mirrors status.go's
			// remoteDegradedAdvisory).
		}
	}
	return nil
}

// projectSubscriptionCompatibility turns a Get response's Subscription
// snapshot into the same {Authority,TargetResource,IncludeResourceData,
// PayloadOptionsPresent} shape classifyUpdateCompatibility already compares
// an updated_v1 event's After snapshot through — using the SAME authority
// normalization (source.FormatLifecycleAuthority) an actual lifecycle event
// would carry — so reconcileWithGet's "active" branch can reuse that
// identical 3-dimension compare rather than re-deriving it or trusting
// state=="active" alone. d==nil (a malformed/empty Get response) projects to
// the zero value: every dimension reads as "absent", which
// classifyUpdateCompatibility already treats as "unclear" rather than a
// confirmed match — never silently "compatible".
func projectSubscriptionCompatibility(d *larkeventv1.SubscriptionDetail) LifecycleEvent {
	if d == nil {
		return LifecycleEvent{}
	}
	le := LifecycleEvent{Authority: source.FormatLifecycleAuthority(d.Authority)}
	if d.TargetResource != nil {
		le.TargetResource = *d.TargetResource
	}
	if d.PayloadOptions != nil && d.PayloadOptions.IncludeResourceData != nil {
		le.PayloadOptionsPresent = true
		le.IncludeResourceData = *d.PayloadOptions.IncludeResourceData
	}
	return le
}
