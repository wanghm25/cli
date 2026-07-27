// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package routing

import (
	"errors"
	"testing"

	"github.com/larksuite/cli/internal/event/model"
	"github.com/larksuite/cli/internal/event/session"
)

const et = "im.message.receive_v1"

// legacyConsumer / refinedConsumer are terse builders for the snapshot Consumer
// value objects the tests route against. Ref is a string label so a test can
// identify which consumer a Destination/Drop points back to.
func legacyConsumer(ref string, eventTypes ...string) Consumer {
	return Consumer{Ref: ref, EventTypes: eventTypes}
}

func refinedConsumer(ref, remoteSubID string, eventTypes ...string) Consumer {
	return Consumer{Ref: ref, RemoteSubscriptionID: remoteSubID, EventTypes: eventTypes}
}

// planFor is a one-liner for Plan against a gate-less snapshot.
func planFor(raw *model.RawEvent, consumers ...Consumer) RoutePlan {
	return Router{}.Plan(Snapshot{Consumers: consumers}, raw)
}

// deliveredKind returns the RouteKind a given Ref was delivered under, or -1 if
// it was not in Deliveries.
func deliveredKind(plan RoutePlan, ref string) RouteKind {
	for _, d := range plan.Deliveries {
		if d.Ref == ref {
			return d.Kind
		}
	}
	return -1
}

func dropReason(plan RoutePlan, ref string) (DropReason, string, bool) {
	for _, d := range plan.Drops {
		if d.Ref == ref {
			return d.Reason, d.Detail, true
		}
	}
	return 0, "", false
}

// The four-quadrant matrix: {with, without} remote_subscription_id ×
// {legacy, refined} consumer. Refined consumers match ONLY by
// remote_subscription_id equality; legacy consumers match by event_type
// regardless of whether the event carries a remote_subscription_id.
func TestPlan_FourQuadrantMatching(t *testing.T) {
	legacy := legacyConsumer("legacy", et)
	refinedR1 := refinedConsumer("R1", "R1", et)
	refinedR2 := refinedConsumer("R2", "R2", et)

	// Quadrant A: event carries remote_subscription_id=R1 with full valid
	// context. R1 gets it (Refined), R2 does not, legacy gets it (Legacy).
	planA := planFor(&model.RawEvent{
		EventType:            et,
		RemoteSubscriptionID: "R1",
		Resource:             "im.message?chat_id=oc_1",
		Authority:            "app",
	}, legacy, refinedR1, refinedR2)

	if got := deliveredKind(planA, "legacy"); got != RouteLegacy {
		t.Errorf("legacy delivered kind = %v, want RouteLegacy", got)
	}
	if got := deliveredKind(planA, "R1"); got != RouteRefined {
		t.Errorf("R1 delivered kind = %v, want RouteRefined", got)
	}
	if got := deliveredKind(planA, "R2"); got != -1 {
		t.Errorf("R2 must NOT be delivered (non-matching remote_subscription_id), got kind %v", got)
	}

	// Quadrant B: event carries NO remote_subscription_id. Legacy matches by
	// event_type; NEITHER refined consumer matches (never guess a resource).
	planB := planFor(&model.RawEvent{EventType: et}, legacy, refinedR1, refinedR2)
	if got := deliveredKind(planB, "legacy"); got != RouteLegacy {
		t.Errorf("legacy delivered kind (no remote id) = %v, want RouteLegacy", got)
	}
	if got := deliveredKind(planB, "R1"); got != -1 {
		t.Error("R1 must NOT be delivered for an event with no remote_subscription_id")
	}
	if got := deliveredKind(planB, "R2"); got != -1 {
		t.Error("R2 must NOT be delivered for an event with no remote_subscription_id")
	}
}

// The §七 fix: an empty remote_subscription_id can NEVER build a refined route,
// even when the event_type would otherwise match the refined consumer's set.
// A refined consumer is matched ONLY by a non-empty remote_subscription_id
// equality — not inferred from an empty id.
func TestPlan_EmptyRemoteSubscriptionIDCannotBuildRefinedRoute(t *testing.T) {
	refined := refinedConsumer("R1", "R1", et)

	// Event with a matching event_type but NO remote_subscription_id.
	plan := planFor(&model.RawEvent{EventType: et}, refined)

	if len(plan.Deliveries) != 0 {
		t.Fatalf("expected no refined route for an empty-remote-id event, got %d deliveries", len(plan.Deliveries))
	}
	if len(plan.Drops) != 0 {
		t.Fatalf("an unmatched refined consumer must not appear as a drop either, got %d", len(plan.Drops))
	}
}

// A refined consumer whose id does not equal the event's non-empty id is simply
// not matched (not a cross-check drop) — the match must precede the cross-check.
func TestPlan_RefinedNonMatchingIDIsNotEvenADrop(t *testing.T) {
	refined := refinedConsumer("R1", "R1", et)
	plan := planFor(&model.RawEvent{
		EventType:            et,
		RemoteSubscriptionID: "R2",
		Resource:             "im.message?chat_id=oc_1",
		Authority:            "app",
	}, refined)
	if len(plan.Deliveries) != 0 || len(plan.Drops) != 0 {
		t.Errorf("non-matching remote id must be neither delivery nor drop; got %d deliveries, %d drops",
			len(plan.Deliveries), len(plan.Drops))
	}
}

// The refined cross-check reason table, exercised through Plan (the real seam):
// a "" want means the consumer is delivered (Refined) with no drop; a non-""
// want means it is dropped with Reason=DropCrossCheck and that exact token.
func TestPlan_RefinedCrossCheckReasons(t *testing.T) {
	tests := []struct {
		name           string
		ownerOpenID    string // consumer owner ("" = bot)
		targetResource string // consumer intent ("" = none stored)
		raw            *model.RawEvent
		want           string // "" = delivered
	}{
		{
			name: "event_type_mismatch",
			raw:  &model.RawEvent{EventType: "im.message.OTHER_v1", RemoteSubscriptionID: "R1", Resource: "im.message?chat_id=oc_1", Authority: "app"},
			want: "event_type",
		},
		{
			name:           "target_resource_missing",
			ownerOpenID:    "ou_alice",
			targetResource: "im.message?chat_id=oc_1",
			raw:            &model.RawEvent{EventType: et, RemoteSubscriptionID: "R1", Authority: "user:ou_alice"},
			want:           "target_resource_missing",
		},
		{
			name:           "target_resource_mismatch",
			ownerOpenID:    "ou_alice",
			targetResource: "im.message?chat_id=oc_1",
			raw:            &model.RawEvent{EventType: et, RemoteSubscriptionID: "R1", Resource: "im.message?chat_id=oc_2", Authority: "user:ou_alice"},
			want:           "target_resource",
		},
		{
			name:           "target_resource_escaping_equivalent_ok",
			ownerOpenID:    "ou_alice",
			targetResource: "im.message?chat_id=oc+1", // local url.QueryEscape form
			raw:            &model.RawEvent{EventType: et, RemoteSubscriptionID: "R1", Resource: "im.message?chat_id=oc%201", Authority: "user:ou_alice"},
			want:           "",
		},
		{
			name:           "authority_missing",
			ownerOpenID:    "ou_alice",
			targetResource: "im.message?chat_id=oc_1",
			raw:            &model.RawEvent{EventType: et, RemoteSubscriptionID: "R1", Resource: "im.message?chat_id=oc_1"},
			want:           "authority_missing",
		},
		{
			name:           "authority_mismatch",
			ownerOpenID:    "ou_alice",
			targetResource: "im.message?chat_id=oc_1",
			raw:            &model.RawEvent{EventType: et, RemoteSubscriptionID: "R1", Resource: "im.message?chat_id=oc_1", Authority: "user:ou_bob"},
			want:           "authority",
		},
		{
			name:           "user_push_all_match",
			ownerOpenID:    "ou_alice",
			targetResource: "im.message?chat_id=oc_1",
			raw:            &model.RawEvent{EventType: et, RemoteSubscriptionID: "R1", Resource: "im.message?chat_id=oc_1", Authority: "user:ou_alice"},
			want:           "",
		},
		{
			name:           "bot_push_all_match",
			ownerOpenID:    "", // bot
			targetResource: "im.message?chat_id=oc_1",
			raw:            &model.RawEvent{EventType: et, RemoteSubscriptionID: "R1", Resource: "im.message?chat_id=oc_1", Authority: "app"},
			want:           "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Consumer{
				Ref:                  "c",
				EventTypes:           []string{et},
				RemoteSubscriptionID: "R1",
				Owner:                model.OwnerRef{AppID: "cli_app", UserOpenID: tt.ownerOpenID},
				TargetResource:       tt.targetResource,
			}
			plan := planFor(tt.raw, c)
			if tt.want == "" {
				if deliveredKind(plan, "c") != RouteRefined {
					t.Fatalf("want delivered (RouteRefined), got deliveries=%d drops=%d", len(plan.Deliveries), len(plan.Drops))
				}
				if len(plan.Drops) != 0 {
					t.Fatalf("want no drop, got %+v", plan.Drops)
				}
				return
			}
			reason, detail, ok := dropReason(plan, "c")
			if !ok {
				t.Fatalf("want a cross-check drop with token %q, got deliveries=%d drops=%d", tt.want, len(plan.Deliveries), len(plan.Drops))
			}
			if reason != DropCrossCheck {
				t.Errorf("drop reason = %v, want DropCrossCheck", reason)
			}
			if detail != tt.want {
				t.Errorf("cross-check token = %q, want %q", detail, tt.want)
			}
		})
	}
}

// The cross-check is per-consumer: two consumers sharing a remote_subscription_id
// differing only on owner are judged independently — one dropped, one delivered.
func TestPlan_RefinedCrossCheckIsPerConsumer(t *testing.T) {
	good := Consumer{Ref: "good", EventTypes: []string{et}, RemoteSubscriptionID: "R1", Owner: model.OwnerRef{UserOpenID: "ou_alice"}}
	bad := Consumer{Ref: "bad", EventTypes: []string{et}, RemoteSubscriptionID: "R1", Owner: model.OwnerRef{UserOpenID: "ou_bob"}}

	plan := planFor(&model.RawEvent{
		EventType:            et,
		RemoteSubscriptionID: "R1",
		Resource:             "im.message?chat_id=oc_1",
		Authority:            "user:ou_alice",
	}, good, bad)

	if deliveredKind(plan, "good") != RouteRefined {
		t.Error("good consumer (owner matches authority) must be delivered")
	}
	if reason, detail, ok := dropReason(plan, "bad"); !ok || reason != DropCrossCheck || detail != "authority" {
		t.Errorf("bad consumer must drop with authority mismatch; got ok=%v reason=%v detail=%q", ok, reason, detail)
	}
}

// --- identity gate eligibility (session.Gate folded in) ---

func gatedSnapshot(resolver IdentityResolver, consumers ...Consumer) Snapshot {
	return Snapshot{Consumers: consumers, Identity: resolver}
}

func userConsumer(ref, appID, userOpenID string) Consumer {
	return Consumer{Ref: ref, EventTypes: []string{et}, Owner: model.OwnerRef{AppID: appID, UserOpenID: userOpenID}}
}

// owner == current: delivered, no drop.
func TestPlan_IdentityGate_AllowsMatchingOwner(t *testing.T) {
	snap := gatedSnapshot(func() (session.CurrentIdentity, error) {
		return session.CurrentIdentity{AppID: "app1", UserOpenID: "ou_alice"}, nil
	}, userConsumer("u", "app1", "ou_alice"))

	plan := Router{}.Plan(snap, &model.RawEvent{EventType: et})
	if deliveredKind(plan, "u") != RouteLegacy {
		t.Error("owner==current must be an eligible destination")
	}
	if len(plan.Drops) != 0 {
		t.Errorf("owner==current must not drop, got %+v", plan.Drops)
	}
}

// owner != current: excluded, DropStaleIdentity.
func TestPlan_IdentityGate_DeniesMismatchedOwner(t *testing.T) {
	snap := gatedSnapshot(func() (session.CurrentIdentity, error) {
		return session.CurrentIdentity{AppID: "app1", UserOpenID: "ou_bob"}, nil
	}, userConsumer("u", "app1", "ou_alice"))

	plan := Router{}.Plan(snap, &model.RawEvent{EventType: et})
	if deliveredKind(plan, "u") != -1 {
		t.Error("owner!=current must not be delivered")
	}
	if reason, _, ok := dropReason(plan, "u"); !ok || reason != DropStaleIdentity {
		t.Errorf("owner!=current must drop as DropStaleIdentity; got ok=%v reason=%v", ok, reason)
	}
}

// resolver error: user consumer excluded as DropUnresolvedIdentity, bot unaffected.
func TestPlan_IdentityGate_ResolveErrorFailsClosedForUsersOnly(t *testing.T) {
	snap := gatedSnapshot(func() (session.CurrentIdentity, error) {
		return session.CurrentIdentity{}, errors.New("config unreadable")
	},
		userConsumer("user", "app1", "ou_alice"),
		Consumer{Ref: "bot", EventTypes: []string{et}, Owner: model.OwnerRef{AppID: "app1"}}, // bot: no owner user
	)

	plan := Router{}.Plan(snap, &model.RawEvent{EventType: et})
	if reason, _, ok := dropReason(plan, "user"); !ok || reason != DropUnresolvedIdentity {
		t.Errorf("user must fail closed as DropUnresolvedIdentity; got ok=%v reason=%v", ok, reason)
	}
	if deliveredKind(plan, "bot") != RouteLegacy {
		t.Error("bot consumer must be unaffected by a resolve error")
	}
}

// nil resolver = no gating: a user consumer is delivered exactly like legacy.
func TestPlan_IdentityGate_NilResolverIsLegacyBehavior(t *testing.T) {
	plan := planFor(&model.RawEvent{EventType: et}, userConsumer("u", "app1", "ou_alice"))
	if deliveredKind(plan, "u") != RouteLegacy {
		t.Error("nil resolver must mean no gating (delivered)")
	}
	if len(plan.Drops) != 0 {
		t.Error("nil resolver must not drop")
	}
}

// A bot consumer bypasses the gate even when a resolver is configured and would
// mismatch a user owner.
func TestPlan_IdentityGate_BotBypasses(t *testing.T) {
	snap := gatedSnapshot(func() (session.CurrentIdentity, error) {
		return session.CurrentIdentity{AppID: "app1", UserOpenID: "ou_alice"}, nil
	}, Consumer{Ref: "bot", EventTypes: []string{et}, Owner: model.OwnerRef{AppID: "app1"}})

	plan := Router{}.Plan(snap, &model.RawEvent{EventType: et})
	if deliveredKind(plan, "bot") != RouteLegacy {
		t.Error("bot consumer must always be eligible — never identity-gated")
	}
}

// The identity port is consulted AT MOST ONCE per Plan even when several user
// consumers match — memoized by construction, not by luck.
func TestPlan_IdentityGate_ResolvedOncePerPlan(t *testing.T) {
	var calls int
	snap := gatedSnapshot(func() (session.CurrentIdentity, error) {
		calls++
		return session.CurrentIdentity{AppID: "app1", UserOpenID: "ou_alice"}, nil
	},
		userConsumer("u1", "app1", "ou_alice"),
		userConsumer("u2", "app1", "ou_alice"),
	)

	plan := Router{}.Plan(snap, &model.RawEvent{EventType: et})
	if len(plan.Deliveries) != 2 {
		t.Fatalf("both user consumers should be eligible, got %d", len(plan.Deliveries))
	}
	if calls != 1 {
		t.Errorf("identity port called %d times for one Plan with 2 user consumers, want 1", calls)
	}
}

// A bot-only match must NOT consult the identity port at all (lazy): the gate is
// never reached for a consumer that bypasses it.
func TestPlan_IdentityGate_NotResolvedWithoutUserMatch(t *testing.T) {
	var calls int
	snap := gatedSnapshot(func() (session.CurrentIdentity, error) {
		calls++
		return session.CurrentIdentity{}, nil
	}, Consumer{Ref: "bot", EventTypes: []string{et}, Owner: model.OwnerRef{AppID: "app1"}})

	Router{}.Plan(snap, &model.RawEvent{EventType: et})
	if calls != 0 {
		t.Errorf("identity port called %d times for a bot-only match, want 0 (lazy)", calls)
	}
}

// The cross-check precedes the identity gate: a refined user consumer with BOTH
// a cross-check mismatch and an identity mismatch is dropped by the cross-check,
// so the gate is not even consulted.
func TestPlan_CrossCheckPrecedesIdentityGate(t *testing.T) {
	var calls int
	snap := gatedSnapshot(func() (session.CurrentIdentity, error) {
		calls++
		return session.CurrentIdentity{AppID: "app1", UserOpenID: "ou_bob"}, nil // would be stale for ou_alice
	}, Consumer{
		Ref:                  "c",
		EventTypes:           []string{et},
		RemoteSubscriptionID: "R1",
		Owner:                model.OwnerRef{AppID: "app1", UserOpenID: "ou_alice"},
		TargetResource:       "im.message?chat_id=oc_1",
	})

	// event_type disagrees -> cross-check drop before the gate.
	plan := Router{}.Plan(snap, &model.RawEvent{
		EventType:            "im.message.OTHER_v1",
		RemoteSubscriptionID: "R1",
		Resource:             "im.message?chat_id=oc_1",
		Authority:            "user:ou_alice",
	})
	if reason, detail, ok := dropReason(plan, "c"); !ok || reason != DropCrossCheck || detail != "event_type" {
		t.Errorf("want cross-check event_type drop; got ok=%v reason=%v detail=%q", ok, reason, detail)
	}
	if calls != 0 {
		t.Errorf("identity port must not be consulted when the cross-check already dropped the consumer, got %d calls", calls)
	}
}
