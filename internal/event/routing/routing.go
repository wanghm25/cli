// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

// Package routing owns the PURE routing decision for the event bus: given an
// immutable snapshot of the registered consumers plus one RawEvent, it produces
// a RoutePlan naming the eligible destinations and the decisions the host must
// observe as side effects. It performs NO fan-out, NO queueing, NO state
// writes, and imports NO SDK — it depends only on internal/event/model (value
// objects) and internal/event/session (the owner/current gate policy), plus
// stdlib. This is what lets the bus's Publish become a thin
// snapshot -> Plan -> deliver -> commit pipeline while the "who is eligible"
// logic lives here, testable in isolation.
//
// The routing decision folds together three concerns that used to be inlined in
// the hub:
//
//   - Dual-index matching. A REFINED consumer (RemoteSubscriptionID != "") is
//     matched ONLY by remote_subscription_id equality, and — the §七 fix — an
//     event with an EMPTY remote_subscription_id can never build a refined
//     route (RouteKind is explicit, never inferred from an empty id). A LEGACY
//     consumer (RemoteSubscriptionID == "") is matched by event_type,
//     unconditionally, including for refined-native events.
//   - The refined cross-check (fail-closed defense-in-depth): a consumer matched
//     by remote_subscription_id must still agree with the event on
//     event_type / target_resource / authority, or it is dropped rather than
//     delivered blind.
//   - The owner/current identity gate (session.Gate): a user consumer whose
//     owner no longer matches the live current identity (or whose current
//     identity cannot be resolved) is not an eligible destination. The
//     fail-closed POLICY is session's and is unchanged here; routing only
//     decides eligibility from it.
package routing

import (
	"github.com/larksuite/cli/internal/event/model"
	"github.com/larksuite/cli/internal/event/session"
)

// RouteKind classifies how a destination was selected. It is explicit — never
// inferred from whether an id happens to be empty — so an empty
// remote_subscription_id can never masquerade as a refined route.
type RouteKind int

const (
	// RouteLegacy: selected by event_type. Its dedup domain is event_id.
	RouteLegacy RouteKind = iota
	// RouteRefined: selected by remote_subscription_id equality (which REQUIRES
	// a non-empty id on both the consumer and the event). Its dedup domain is
	// keyed by subscription_event_id (globally unique for a refined event).
	RouteRefined
)

// Consumer is the immutable, routing-relevant projection of one registered
// consumer. The host builds these under its own lock and hands routing a
// snapshot; routing never mutates a Consumer.
type Consumer struct {
	// Ref is an opaque handle back to the real delivery destination. Routing
	// carries it through into the RoutePlan untouched — it never inspects it —
	// so the host can pack whatever endpoint value it needs (e.g. the bus
	// Subscriber) and read it back out when applying the plan.
	Ref any
	// EventTypes is the consumer's registered event-type set (legacy matching
	// and the cross-check's event_type dimension).
	EventTypes []string
	// RemoteSubscriptionID is the remote Subscription this consumer is bound to;
	// "" marks a legacy consumer.
	RemoteSubscriptionID string
	// Owner is the consumer's owner identity fixed at registration. Only AppID
	// and UserOpenID participate in the gate; UserOpenID == "" marks a
	// bot/legacy consumer that is NEVER identity-gated.
	Owner model.OwnerRef
	// TargetResource is the consumer's own resolved listening intent, used by
	// the cross-check's target_resource dimension. "" means "no stored intent"
	// (a legacy consumer, or a destination that carries no target_resource
	// concept) — the cross-check then only presence-checks the event's own
	// target_resource, never comparing it against a consumer intent.
	TargetResource string
}

// IdentityResolver is the read-only port routing consults for the owner/current
// gate. A nil resolver disables identity gating entirely (the default for every
// non-gated bus). Routing owns no I/O of its own: the host injects this port and
// owns whatever config read it performs. Plan calls it AT MOST ONCE per plan,
// lazily, and only when a matched user consumer actually needs it.
type IdentityResolver func() (session.CurrentIdentity, error)

// Snapshot is the immutable input to Plan: the point-in-time consumer set plus
// the identity port.
type Snapshot struct {
	Consumers []Consumer
	// Identity, when non-nil, enables the owner/current fail-closed gate.
	Identity IdentityResolver
}

// DropReason classifies why routing excluded a matched consumer. Each maps to a
// host-applied side effect: a counter + WARN log (cross-check) or a per-consumer
// state mark (identity gate).
type DropReason int

const (
	// DropCrossCheck: a refined consumer matched by remote_subscription_id but
	// disagreed with the event on the cross-check (Detail names the dimension).
	DropCrossCheck DropReason = iota
	// DropStaleIdentity: a user consumer whose owner != the live current
	// identity (session.AdmitStale). The host marks it stale_identity.
	DropStaleIdentity
	// DropUnresolvedIdentity: the current identity could not be resolved
	// (session.AdmitUnresolved). The host marks the consumer identity-degraded.
	DropUnresolvedIdentity
)

// Destination is one eligible fan-out target: the opaque Ref the host packed,
// tagged with the RouteKind that selected it (which the host uses to pick the
// dedup domain).
type Destination struct {
	Ref  any
	Kind RouteKind
}

// Drop is one excluded consumer plus why. The host applies the corresponding
// side effect; routing itself writes no state.
type Drop struct {
	Ref    any
	Reason DropReason
	// Detail carries the cross-check mismatch token ("event_type" /
	// "target_resource" / "authority" / "target_resource_missing" /
	// "authority_missing") when Reason == DropCrossCheck; "" otherwise.
	Detail string
}

// RoutePlan is Plan's output: the eligible destinations and the excluded
// consumers with their reasons.
type RoutePlan struct {
	Deliveries []Destination
	Drops      []Drop
}

// Router produces RoutePlans. It is stateless; the zero value is ready to use.
// It is a struct rather than a bare function so the routing policy can be
// extended (e.g. injected configuration) without changing every call site.
type Router struct{}

// Plan is the pure routing decision. Given the snapshot and one event it returns
// the eligible destinations (Deliveries) and the consumers it excluded (Drops)
// with reasons, so the host can apply the side effects. It writes no state and
// performs no I/O beyond consulting the injected identity port at most once.
func (Router) Plan(snap Snapshot, raw *model.RawEvent) RoutePlan {
	var plan RoutePlan

	// Identity resolution is memoized within this single Plan call: it happens
	// at most once, and only when a matched user consumer first needs the gate.
	var (
		identityResolved bool
		cur              session.CurrentIdentity
		curErr           error
	)
	resolveIdentity := func() (session.CurrentIdentity, error) {
		if !identityResolved {
			cur, curErr = snap.Identity()
			identityResolved = true
		}
		return cur, curErr
	}

	for _, c := range snap.Consumers {
		// Dual-index matching. A refined consumer matches ONLY by
		// remote_subscription_id equality, which by construction requires a
		// non-empty id on BOTH sides — an empty event remote id can never build
		// a refined route. A legacy consumer matches by event_type.
		var kind RouteKind
		if c.RemoteSubscriptionID != "" {
			if raw.RemoteSubscriptionID == "" || c.RemoteSubscriptionID != raw.RemoteSubscriptionID {
				continue
			}
			kind = RouteRefined
		} else {
			if !eventTypeMatch(c.EventTypes, raw.EventType) {
				continue
			}
			kind = RouteLegacy
		}

		// Refined cross-check: fail-closed defense-in-depth on top of the
		// remote_subscription_id match, evaluated per consumer.
		if kind == RouteRefined {
			if reason := crossCheckReason(raw, c); reason != "" {
				plan.Drops = append(plan.Drops, Drop{Ref: c.Ref, Reason: DropCrossCheck, Detail: reason})
				continue
			}
		}

		// Owner/current identity gate: user consumers only; a bot/legacy owner
		// (UserOpenID == "") bypasses it entirely via session.Gate.
		if snap.Identity != nil && c.Owner.UserOpenID != "" {
			gateCur, gateErr := resolveIdentity()
			switch session.Gate(c.Owner, gateCur, gateErr) {
			case session.AdmitUnresolved:
				plan.Drops = append(plan.Drops, Drop{Ref: c.Ref, Reason: DropUnresolvedIdentity})
				continue
			case session.AdmitStale:
				plan.Drops = append(plan.Drops, Drop{Ref: c.Ref, Reason: DropStaleIdentity})
				continue
			}
		}

		plan.Deliveries = append(plan.Deliveries, Destination{Ref: c.Ref, Kind: kind})
	}

	return plan
}

// eventTypeMatch reports whether raw's event_type is in the consumer's
// registered set.
func eventTypeMatch(eventTypes []string, eventType string) bool {
	for _, et := range eventTypes {
		if et == eventType {
			return true
		}
	}
	return false
}

// crossCheckReason implements the fail-closed cross-check for a refined consumer
// already matched by remote_subscription_id equality. A legit refined event
// ALWAYS carries its full context (event_type + target_resource + authority), so
// a MISSING dimension is treated as an anomaly to drop, not a reason to deliver
// blind. It returns "" (deliver) or a short fixed token naming what went wrong,
// with distinct tokens for "absent context" (*_missing) versus
// "present-but-disagrees".
//
//   - event_type is always checkable and a real remote Subscription is bound to
//     one event_type at Create time, so a disagreement under an already-matched
//     remote_subscription_id is a genuine anomaly.
//   - target_resource must be present; when the consumer carries its own resolved
//     intent (TargetResource != "") it is compared NORMALIZED via
//     model.TargetResourceEqual so escaping/selector-ordering never false-drops.
//     A consumer with no stored intent is held only to the presence check.
//   - authority must be present and must name THIS consumer's own owner —
//     "user:<open_id>" for a user, "app" for a bot — via model.AuthorityMatchesOwner,
//     the same comparison the lifecycle update-compatibility check uses.
func crossCheckReason(raw *model.RawEvent, c Consumer) string {
	if !eventTypeMatch(c.EventTypes, raw.EventType) {
		return "event_type"
	}

	if raw.Resource == "" {
		return "target_resource_missing"
	}
	if c.TargetResource != "" && !model.TargetResourceEqual(raw.Resource, c.TargetResource) {
		return "target_resource"
	}

	if raw.Authority == "" {
		return "authority_missing"
	}
	if !model.AuthorityMatchesOwner(raw.Authority, c.Owner.UserOpenID) {
		return "authority"
	}

	return ""
}
