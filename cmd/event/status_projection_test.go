// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/internal/event/model"
	larkgw "github.com/larksuite/cli/internal/event/platform/lark"
	subown "github.com/larksuite/cli/internal/event/subscription"
	"github.com/larksuite/cli/internal/event/protocol"
)

// The real catalog (im.message.created_v1 refined base + chat-id template, etc.)
// is registered for this whole test binary by golden_test.go's blank
// `_ "github.com/larksuite/cli/events"` import, so ReverseResolve reconstructs
// executable event_keys here without a per-test fixture.

// --- bounded remote refresh: timeout ⇒ local-only fallback ------------------

// blockingGetter is a refinedSubscriptionGetter whose Get/WalkSubscriptions
// block until the supplied context is cancelled, then return its error — a slow
// or unreachable remote. It lets the timeout test prove runBoundedSupplement
// applies its total budget to the context the getter actually sees (never a
// non-cancelling context that would hang forever) with no Factory/network.
type blockingGetter struct{ walkStarted chan struct{} }

func (g blockingGetter) Get(ctx context.Context, _ string) (*model.RemoteSubscription, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (g blockingGetter) WalkSubscriptions(ctx context.Context, _ subown.ListParams, _ func(model.RemoteSubscription) bool) (bool, error) {
	if g.walkStarted != nil {
		close(g.walkStarted)
	}
	<-ctx.Done()
	return false, ctx.Err()
}

// TestRunBoundedSupplement_RemoteTimesOut_DegradesToLocalOnly is the
// bounded-refresh guard: when the remote is unreachable/slow, the whole
// supplement is bounded by its total time budget and degrades to local-only —
// it never hangs, and the unverified consumer projects scope=unknown (the
// remote is marked unavailable/unknown, never falsely verified).
func TestRunBoundedSupplement_RemoteTimesOut_DegradesToLocalOnly(t *testing.T) {
	c := refinedConsumer() // one refined consumer, RemoteSubscription nil
	statuses := []appStatus{{
		AppID: "cli_a", State: stateRunning, Active: 1,
		Consumers: []protocol.ConsumerInfo{c},
	}}

	done := make(chan bool, 1)
	go func() {
		// Tiny budget; the getter blocks until the bounded context cancels.
		runBoundedSupplement(context.Background(), 20*time.Millisecond, statuses, "cli_a", func(ctx context.Context) refinedSubscriptionGetter {
			return blockingGetter{}
		})
		done <- true
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("runBoundedSupplement did not return within the total budget — the remote supplement is not bounded (it would hang `event status`)")
	}

	got := statuses[0].Consumers[0]
	if got.RemoteSubscription != nil || got.RemoteState != "" {
		t.Errorf("consumer must degrade to local-only on a remote timeout, got: %+v", got)
	}
	if scope := (StatusProjector{}).consumerScope(statuses[0], got); scope != scopeUnknown {
		t.Errorf("scope = %q, want %q after the bounded-refresh timeout — a genuinely-unverified remote is unknown, never verified", scope, scopeUnknown)
	}
}

// --- scope tri-state: verified | missing | unknown --------------------------

// TestConsumerScope_TriState locks the three scope states and the two safety
// invariants: a genuinely-unverified refined consumer is unknown (never
// verified), and a legacy consumer (no remote subscription) has no scope at all.
func TestConsumerScope_TriState(t *testing.T) {
	base := appStatus{AppID: "cli_a", State: stateRunning}

	// verified: the bounded supplement filled the remote snapshot.
	verified := refinedConsumer()
	verified.RemoteSubscription = &protocol.RemoteSubscriptionInfo{State: "active"}
	if got := (StatusProjector{}).consumerScope(base, verified); got != scopeVerified {
		t.Errorf("verified: scope = %q, want %q", got, scopeVerified)
	}

	// missing: a complete remote enumeration authoritatively found the id absent.
	missingApp := appStatus{AppID: "cli_a", State: stateRunning, RemoteMissing: map[string]bool{"sub_abc123": true}}
	if got := (StatusProjector{}).consumerScope(missingApp, refinedConsumer()); got != scopeMissing {
		t.Errorf("missing: scope = %q, want %q", got, scopeMissing)
	}

	// unknown: refined consumer, no snapshot, not authoritatively absent — must
	// be unknown, NEVER verified.
	if got := (StatusProjector{}).consumerScope(base, refinedConsumer()); got != scopeUnknown {
		t.Errorf("unknown: scope = %q, want %q (genuinely-unknown must not be verified)", got, scopeUnknown)
	}

	// legacy consumer: no remote subscription -> scope omitted entirely.
	legacy := protocol.ConsumerInfo{PID: 1, EventKey: "mail.x"}
	if got := (StatusProjector{}).consumerScope(base, legacy); got != "" {
		t.Errorf("legacy: scope = %q, want empty (omitted) — a legacy consumer has no remote subscription to verify", got)
	}
}

// TestWriteStatusJSON_Scope_UnknownRenderedExplicitly locks that scope reaches
// the JSON as the explicit string "unknown" (not true, not omitted, not a bool)
// for an unverified refined consumer, and is omitted for a legacy consumer.
func TestWriteStatusJSON_Scope_UnknownRenderedExplicitly(t *testing.T) {
	statuses := []appStatus{{
		AppID: "cli_a", State: stateRunning,
		Consumers: []protocol.ConsumerInfo{refinedConsumer()},
	}}
	cv := consumerJSONFromStatuses(t, statuses)
	if cv["scope"] != "unknown" {
		t.Errorf("scope = %#v, want the explicit string \"unknown\" (never true/verified/omitted for an unverified refined consumer)", cv["scope"])
	}
	if cv["scope"] == true {
		t.Error("scope must never render as boolean true")
	}

	legacy := []appStatus{{
		AppID: "cli_a", State: stateRunning,
		Consumers: []protocol.ConsumerInfo{{PID: 1, EventKey: "mail.x", Received: 1}},
	}}
	lv := consumerJSONFromStatuses(t, legacy)
	if _, present := lv["scope"]; present {
		t.Errorf("scope must be omitted for a legacy consumer, got %v", lv["scope"])
	}
}

// TestWriteStatusJSON_Scope_VerifiedWhenSupplemented locks scope=verified once
// the remote snapshot is present.
func TestWriteStatusJSON_Scope_VerifiedWhenSupplemented(t *testing.T) {
	c := refinedConsumer()
	c.RemoteSubscription = &protocol.RemoteSubscriptionInfo{State: "active"}
	c.RemoteState = "active"
	statuses := []appStatus{{AppID: "cli_a", State: stateRunning, Consumers: []protocol.ConsumerInfo{c}}}
	cv := consumerJSONFromStatuses(t, statuses)
	if cv["scope"] != "verified" {
		t.Errorf("scope = %v, want verified once the remote supplement filled the snapshot", cv["scope"])
	}
}

// TestSupplementRefinedConsumers_CompleteScanMissingID_MarksMissing proves the
// end-to-end scope=missing path: a reachable, COMPLETE (non-capped) List scan
// that never yields the wanted ids reports them as authoritatively missing, and
// the projector renders scope=missing for those consumers.
func TestSupplementRefinedConsumers_CompleteScanMissingID_MarksMissing(t *testing.T) {
	n := remoteSupplementListThreshold + 1
	consumers := make([]protocol.ConsumerInfo, 0, n)
	for i := 0; i < n; i++ {
		c := refinedConsumer()
		c.PID = 100 + i
		c.RemoteSubscriptionID = fmt.Sprintf("sub_gone_%d", i)
		consumers = append(consumers, c)
	}
	// A COMPLETE scan (walkCapped=false) that only ever yields unrelated ids.
	getter := &fakeRefinedGetter{
		walkItems:  []model.RemoteSubscription{remoteSubID("unrelated_1", "active")},
		walkCapped: false,
	}

	capped, missing := supplementRefinedConsumers(context.Background(), getter, consumers)
	if capped {
		t.Error("capped = true, want false (the scan ran to completion)")
	}
	s := appStatus{AppID: "cli_a", State: stateRunning, RemoteMissing: missing, Consumers: consumers}
	for i, c := range consumers {
		if !missing[c.RemoteSubscriptionID] {
			t.Errorf("consumers[%d] id=%s not marked missing after a complete enumeration", i, c.RemoteSubscriptionID)
		}
		if got := (StatusProjector{}).consumerScope(s, c); got != scopeMissing {
			t.Errorf("consumers[%d] scope = %q, want %q", i, got, scopeMissing)
		}
	}
}

// --- event_key on the remote_subscription snapshot --------------------------

// TestMapRemoteSubscriptionInfo_EventKey_ReversedOrUnavailable proves status's
// remote supplement surfaces the executable, reversed event_key — or the
// explicit unavailable marker — never a raw event_type.
func TestMapRemoteSubscriptionInfo_EventKey_ReversedOrUnavailable(t *testing.T) {
	resolvable := mapRemoteSubscriptionInfo(larkgw.ProjectSubscription(&larkeventv1.SubscriptionDetail{
		EventType:      strPtr("im.message.created_v1"),
		TargetResource: strPtr("im.message?chat_id=oc_xxx"),
		State:          strPtr("active"),
	}))
	if resolvable.EventKey != "im.message.created_v1/chat-id/oc_xxx" {
		t.Errorf("EventKey = %q, want the reversed materialized key im.message.created_v1/chat-id/oc_xxx", resolvable.EventKey)
	}

	unreversible := mapRemoteSubscriptionInfo(larkgw.ProjectSubscription(&larkeventv1.SubscriptionDetail{
		EventType:      strPtr("does.not.exist_v1"),
		TargetResource: strPtr("im.message?chat_id=oc_zzz"),
		State:          strPtr("active"),
	}))
	if unreversible.EventKey != statusEventKeyUnavailable {
		t.Errorf("EventKey = %q, want the explicit %q marker for an unreversible pair", unreversible.EventKey, statusEventKeyUnavailable)
	}
	if unreversible.EventKey == "does.not.exist_v1" {
		t.Error("event_key must never be the raw event_type when unreversible")
	}
}

// --- structured next_action {command,args,reason} + text consistency --------

// TestNextAction_BusReactivateToken_StructuredAndConsistentWithText locks that
// the bus lifecycle "reactivate" token projects into a structured, directly
// executable {command,args,reason} in JSON, and that the status text carries the
// SAME reason and the runnable command — so the AI form and the human text agree.
func TestNextAction_BusReactivateToken_StructuredAndConsistentWithText(t *testing.T) {
	c := refinedConsumer()
	c.NextAction = "reactivate" // the bus's frozen lifecycle recovery token
	statuses := []appStatus{{
		AppID: "cli_a", State: stateRunning, Active: 1,
		CurrentIdentityKnown: true, CurrentAppID: "cli_a", CurrentUserOpenID: "ou_1",
		Consumers: []protocol.ConsumerInfo{c},
	}}

	cv := consumerJSONFromStatuses(t, statuses)
	na, ok := cv["next_action"].(map[string]interface{})
	if !ok {
		t.Fatalf("next_action = %v (%T), want a structured {command,args,reason} object", cv["next_action"], cv["next_action"])
	}
	if na["command"] != "lark-cli event subscription reactivate" {
		t.Errorf("next_action.command = %v, want lark-cli event subscription reactivate", na["command"])
	}
	args, _ := na["args"].([]interface{})
	if len(args) != 1 || args[0] != "sub_abc123" {
		t.Errorf("next_action.args = %v, want [sub_abc123] (the remote_subscription_id)", na["args"])
	}
	reason, _ := na["reason"].(string)
	if reason == "" {
		t.Fatal("next_action.reason must be non-empty")
	}

	var buf bytes.Buffer
	writeStatusText(&buf, statuses)
	out := buf.String()
	if !strings.Contains(out, "next_action:") {
		t.Errorf("text output missing a next_action line; full output:\n%s", out)
	}
	if !strings.Contains(out, reason) {
		t.Errorf("text next_action must carry the SAME reason as the JSON (%q); full output:\n%s", reason, out)
	}
	if !strings.Contains(out, "lark-cli event subscription reactivate sub_abc123") {
		t.Errorf("text next_action must name the runnable command; full output:\n%s", out)
	}
}

// TestNextAction_ScopeMissing_RecommendsRecreate locks that an authoritatively
// missing remote subscription yields a structured `subscription create
// <event_key>` recovery (the reversed, executable key), so an AI can rebuild it.
func TestNextAction_ScopeMissing_RecommendsRecreate(t *testing.T) {
	c := refinedConsumer() // EventKey im.message.created_v1/chat-id/oc_xxx, id sub_abc123
	s := appStatus{AppID: "cli_a", State: stateRunning, RemoteMissing: map[string]bool{"sub_abc123": true}}
	na := (StatusProjector{}).nextAction(s, c, scopeMissing)
	if na == nil {
		t.Fatal("expected a structured next_action for a missing remote subscription, got nil")
	}
	if na.Command != "lark-cli event subscription create" {
		t.Errorf("command = %q, want lark-cli event subscription create", na.Command)
	}
	if len(na.Args) != 1 || na.Args[0] != c.EventKey {
		t.Errorf("args = %v, want [%s] (the executable event_key)", na.Args, c.EventKey)
	}
	if na.Reason == "" {
		t.Error("reason must be non-empty")
	}
}

// TestNextAction_HealthyVerified_NoRecommendation locks that a healthy, verified,
// owner-matching consumer surfaces no next_action at all (nil -> omitted).
func TestNextAction_HealthyVerified_NoRecommendation(t *testing.T) {
	c := refinedConsumer()
	c.RemoteSubscription = &protocol.RemoteSubscriptionInfo{State: "active"}
	s := appStatus{
		AppID: "cli_a", State: stateRunning,
		CurrentIdentityKnown: true, CurrentAppID: "cli_a", CurrentUserOpenID: "ou_1",
	}
	if na := (StatusProjector{}).nextAction(s, c, scopeVerified); na != nil {
		t.Errorf("healthy verified owner-matching consumer must have no next_action, got %+v", na)
	}
}
