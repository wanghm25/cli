// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package lifecycle

import (
	"github.com/larksuite/cli/internal/event"
)

// Phase is the reducer's per-remote_subscription_id state — what the ordered
// fold of lifecycle events has decided the subscription's status is. Two phases
// are TERMINAL (Expired, Deleted): once an id reaches one, a late/out-of-order
// activated_v1 or updated_v1 for the same id must NOT revive it (the tombstone
// invariant). Every other phase is live and freely transitionable.
type Phase int

const (
	PhaseUnknown Phase = iota
	PhaseActive
	PhaseSuspended
	PhaseExpiringSoon
	PhaseExpired
	PhaseDeleted
)

// Terminal reports whether this phase is a tombstone that a resurrection event
// must not undo.
func (p Phase) Terminal() bool { return p == PhaseExpired || p == PhaseDeleted }

// Intent is the eligible consumers' shared local listening intent — the ONLY
// per-consumer data the pure reducer needs (for the updated_v1 4-dimension
// compatibility compare). Built once from the lead eligible conn.
type Intent struct {
	OwnerUserOpenID     string
	TargetResource      string
	IncludeResourceData bool
	Filter              *event.Filter
}

// EffectKind enumerates the side effects the reducer RETURNS for the executor's
// caller (SubscriptionAction.Handle) to run — the reducer itself performs no
// remote call and mutates no Conn. The single remote call each effect issues
// (at most one per event, never a retry) and its result-dependent health are
// applied by the effect runner, keeping the reducer pure.
type EffectKind int

const (
	EffectNone EffectKind = iota
	// EffectReconcileGet: fetch the current remote state (single Get) and
	// classify it — this is how updated_v1's "unclear" 4-dimension compat, and
	// suspended_v1's non-authority_revoked default branch, reconcile.
	EffectReconcileGet
	// EffectReactivate: a single Reactivate, then (for a user) a BindUser.
	EffectReactivate
	// EffectRenew: a single Renew.
	EffectRenew
	// EffectReleaseEncryptKey: drop the deleted subscription's cached key.
	EffectReleaseEncryptKey
)

// subHealthOp is how a reduction touches the SUBSCRIPTION health dimension.
type subHealthOp int

const (
	subLeave            subHealthOp = iota // leave the subscription fact as-is
	subSet                                 // set reason (+ next_action)
	subClearIfSuspended                    // clear only if currently remote_subscription_suspended
	subClearIfConflict                     // clear only if currently remote_subscription_conflict
)

// subDecision is the reduction's subscription-dimension health decision.
type subDecision struct {
	op     subHealthOp
	reason string // for subSet
	next   string // for subSet (next_action)
}

// subConflictGetDecision is the ONE "the remote subscription no longer matches
// this consumer's local intent — go re-inspect it" health decision:
// remote_subscription_conflict + next_action=get. Defined once so the updated_v1
// INCOMPATIBLE branch here and the proactive operator-update signal
// (SubscriptionAction.MarkLocalUpdate) apply the byte-identical decision and can
// never drift — the whole point of "the local-update signal mirrors updated_v1
// exactly".
func subConflictGetDecision() subDecision {
	return subDecision{op: subSet, reason: ReasonRemoteSubscriptionConflict, next: NextActionGet}
}

// reduceScope is which conns a reduction's subscription decision applies to:
// only the ELIGIBLE (owner==current) conns, or ALL matched conns (expired/
// deleted are pure local bookkeeping, applied even to an ineligible conn).
type reduceScope int

const (
	scopeEligible reduceScope = iota
	scopeAll
)

// Reduction is reduce()'s pure output: the new phase, an optional suspension.code
// to record on ALL matched conns, the subscription-dimension health decision
// (applied to scope conns), and the single effect the caller must run. It
// carries NO Conn and issues NO remote call.
type Reduction struct {
	Phase   Phase
	dropped bool // terminal-tombstone hit: a resurrection to be dropped

	suspension *string // non-nil: SetSuspensionReason(*suspension) on ALL matched conns
	sub        subDecision
	scope      reduceScope
	effect     EffectKind
}

// isResurrection reports whether an event type could revive a terminal
// subscription — only activated/updated. suspended/expiration/expired/deleted
// arriving after a terminal are not "revivals" and are handled normally (an
// expired after a deleted just re-confirms terminal).
func isResurrection(eventType string) bool {
	return eventType == lifecycleEventTypeActivated || eventType == lifecycleEventTypeUpdated
}

// reduce is the pure per-remote_subscription_id lifecycle reducer:
// (currentPhase, event, eligible-intent) -> (newPhase, health decision,
// effect). It performs no remote call and mutates no Conn. Terminal-state
// priority is first-class here: a resurrection (activated/updated) against a
// terminal current phase yields a dropped reduction (the tombstone is kept).
func reduce(cur Phase, le LifecycleEvent, intent Intent) Reduction {
	if cur.Terminal() && isResurrection(le.EventType) {
		return Reduction{Phase: cur, dropped: true}
	}

	switch le.EventType {
	case lifecycleEventTypeActivated:
		// No longer suspended: clear suspension bookkeeping on all, and clear a
		// prior suspension-degraded fact on the eligible conns. NEVER binds.
		return Reduction{
			Phase:      PhaseActive,
			suspension: strPtr(""),
			sub:        subDecision{op: subClearIfSuspended},
			scope:      scopeEligible,
		}

	case lifecycleEventTypeUpdated:
		switch classifyUpdateCompatibility(le, intent) {
		case updateCompatible:
			return Reduction{Phase: PhaseActive, sub: subDecision{op: subClearIfConflict}, scope: scopeEligible}
		case updateIncompatible:
			return Reduction{Phase: PhaseActive, sub: subConflictGetDecision(), scope: scopeEligible}
		default: // unclear: reconcile with a single Get
			return Reduction{Phase: PhaseActive, scope: scopeEligible, effect: EffectReconcileGet}
		}

	case lifecycleEventTypeSuspended:
		// suspension.code recorded verbatim on ALL matched conns (bookkeeping);
		// only the ONE confirmed-stable code auto-Reactivates, any other value
		// reconciles via a single Get (never a guessed action).
		r := Reduction{Phase: PhaseSuspended, suspension: strPtr(le.SuspensionCode), scope: scopeEligible}
		if le.SuspensionCode == suspensionCodeAuthorityRevoked {
			r.effect = EffectReactivate
		} else {
			r.effect = EffectReconcileGet
		}
		return r

	case lifecycleEventTypeExpirationReminder:
		return Reduction{Phase: PhaseExpiringSoon, scope: scopeEligible, effect: EffectRenew}

	case lifecycleEventTypeExpired:
		// Terminal, pure local bookkeeping, NEVER a remote call — applied to
		// every matched conn regardless of eligibility.
		return Reduction{Phase: PhaseExpired, sub: subDecision{op: subSet, reason: ReasonRemoteSubscriptionExpired, next: NextActionRebuild}, scope: scopeAll}

	case LifecycleEventTypeDeleted:
		// Terminal + release the cached encrypt_key; NEVER a rebuild.
		return Reduction{Phase: PhaseDeleted, sub: subDecision{op: subSet, reason: ReasonRemoteSubscriptionDeleted, next: NextActionRebuild}, scope: scopeAll, effect: EffectReleaseEncryptKey}

	default:
		// Unrecognized: the summary was already recorded by Handle; no change.
		return Reduction{Phase: cur}
	}
}

// phaseFromState maps a Get's/event's remote state string to a Phase (ok=false
// for an open-vocabulary value the reducer does not special-case).
func phaseFromState(state string) (Phase, bool) {
	switch state {
	case "active":
		return PhaseActive, true
	case "suspended":
		return PhaseSuspended, true
	case "expired":
		return PhaseExpired, true
	default:
		return PhaseUnknown, false
	}
}

func strPtr(s string) *string { return &s }

// Terminal-state priority no longer needs an executor-level supersede/merge
// veto: the per-id FIFO executor processes events for one remote_subscription_id
// in ARRIVAL order, and the pure reducer's phase store (reduce's terminal check
// against the recorded Phase) drops a resurrection that follows a terminal in
// that order. So a pending terminal can never be "overwritten" — there is no
// latest-wins merge to veto — and the tombstone invariant is owned in ONE place,
// the reducer, rather than being duplicated as a merge-window policy here.
