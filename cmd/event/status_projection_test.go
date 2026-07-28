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

	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/event/model"
	larkgw "github.com/larksuite/cli/internal/event/platform/lark"
	"github.com/larksuite/cli/internal/event/protocol"
	subown "github.com/larksuite/cli/internal/event/subscription"
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

	oc := resolvingOwnerCtx() // owner cli_a / user / ou_1 resolves -> executable
	cv := consumerJSONWithOwner(t, statuses, oc)
	na, ok := cv["next_action"].(map[string]interface{})
	if !ok {
		t.Fatalf("next_action = %v (%T), want a structured {command,args,reason} object", cv["next_action"], cv["next_action"])
	}
	if na["command"] != "lark-cli event subscription reactivate" {
		t.Errorf("next_action.command = %v, want lark-cli event subscription reactivate", na["command"])
	}
	args, _ := na["args"].([]interface{})
	// args carry the positional id PLUS the OWNING --profile/--as (owner-first
	// composability): the recovery command runs against the subscription's owner.
	if len(args) != 5 || args[0] != "sub_abc123" || args[1] != "--profile" || args[2] != "cli_a" || args[3] != "--as" || args[4] != "user" {
		t.Errorf("next_action.args = %v, want [sub_abc123 --profile cli_a --as user]", na["args"])
	}
	reason, _ := na["reason"].(string)
	if reason == "" {
		t.Fatal("next_action.reason must be non-empty")
	}

	var buf bytes.Buffer
	writeStatusText(&buf, statuses, oc)
	out := buf.String()
	if !strings.Contains(out, "next_action:") {
		t.Errorf("text output missing a next_action line; full output:\n%s", out)
	}
	if !strings.Contains(out, reason) {
		t.Errorf("text next_action must carry the SAME reason as the JSON (%q); full output:\n%s", reason, out)
	}
	if !strings.Contains(out, "lark-cli event subscription reactivate sub_abc123 --profile cli_a --as user") {
		t.Errorf("text next_action must name the runnable command with the owning --profile/--as; full output:\n%s", out)
	}
}

// TestNextAction_ScopeMissing_RecommendsRecreate locks that an authoritatively
// missing remote subscription yields a structured `subscription create
// <event_key>` recovery (the reversed, executable key), so an AI can rebuild it.
func TestNextAction_ScopeMissing_RecommendsRecreate(t *testing.T) {
	c := refinedConsumer() // EventKey im.message.created_v1/chat-id/oc_xxx, id sub_abc123
	s := appStatus{AppID: "cli_a", State: stateRunning, RemoteMissing: map[string]bool{"sub_abc123": true}}
	na := (StatusProjector{}).nextAction(s, c, scopeMissing, resolvingOwnerCtx())
	if na == nil {
		t.Fatal("expected a structured next_action for a missing remote subscription, got nil")
	}
	if na.Command != "lark-cli event subscription create" {
		t.Errorf("command = %q, want lark-cli event subscription create", na.Command)
	}
	// The create carries the executable key PLUS the OWNING --profile/--as.
	want := []string{c.EventKey, "--profile", "cli_a", "--as", "user"}
	if len(na.Args) != len(want) {
		t.Fatalf("args = %v, want %v", na.Args, want)
	}
	for i, w := range want {
		if na.Args[i] != w {
			t.Errorf("args[%d] = %q, want %q (full: %v)", i, na.Args[i], w, na.Args)
		}
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
	if na := (StatusProjector{}).nextAction(s, c, scopeVerified, resolvingOwnerCtx()); na != nil {
		t.Errorf("healthy verified owner-matching consumer must have no next_action, got %+v", na)
	}
}

// --- owner-first recovery-command gating (LoadMultiAppConfig owner context) ---

// TestBuildOwnerContext_AmbiguousAppIDDropped locks that an app_id appearing for
// more than one AppConfig is DROPPED (reason-only), never a guessed profile.
func TestBuildOwnerContext_AmbiguousAppIDDropped(t *testing.T) {
	cfg := &core.MultiAppConfig{Apps: []core.AppConfig{
		{AppId: "cli_a", Name: "A", Users: []core.AppUser{{UserOpenId: "ou_1"}}},
		{AppId: "cli_dup"}, {AppId: "cli_dup"}, // ambiguous
	}}
	oc := buildOwnerContext(cfg)
	if got, ok := oc["cli_a"]; !ok || got.Profile != "A" || got.UserOpenID != "ou_1" {
		t.Errorf("cli_a = %+v (ok=%v), want {Profile:A UserOpenID:ou_1}", got, ok)
	}
	if _, ok := oc["cli_dup"]; ok {
		t.Error("ambiguous cli_dup must be dropped -> reason-only")
	}
}

// TestRecoveryCommandContext_OwnerFirstRules locks the exact owner-first policy:
// bot needs only a profile hit; user needs a profile hit AND its active-user
// open_id == OwnerUserOpenID; a foreign app, an ambiguous/absent mapping, no
// active user, a user mismatch, or an unknown identity all yield ok=false
// (reason-only). It NEVER falls back to any other profile.
func TestRecoveryCommandContext_OwnerFirstRules(t *testing.T) {
	oc := ownerContext{
		"app_bot":    {Profile: "P_bot"},
		"app_user":   {Profile: "P_user", UserOpenID: "ou_own"},
		"app_nouser": {Profile: "P_nu"}, // no active user
	}
	cases := []struct {
		name                string
		c                   protocol.ConsumerInfo
		wantOK              bool
		wantProfile, wantID string
	}{
		{"bot hit", protocol.ConsumerInfo{OwnerIdentity: "bot", OwnerAppID: "app_bot"}, true, "P_bot", "bot"},
		{"user hit + match", protocol.ConsumerInfo{OwnerIdentity: "user", OwnerAppID: "app_user", OwnerUserOpenID: "ou_own"}, true, "P_user", "user"},
		{"user mismatch", protocol.ConsumerInfo{OwnerIdentity: "user", OwnerAppID: "app_user", OwnerUserOpenID: "ou_other"}, false, "", ""},
		{"user no active user", protocol.ConsumerInfo{OwnerIdentity: "user", OwnerAppID: "app_nouser", OwnerUserOpenID: "ou_own"}, false, "", ""},
		{"foreign app", protocol.ConsumerInfo{OwnerIdentity: "bot", OwnerAppID: "app_missing"}, false, "", ""},
		{"unknown identity", protocol.ConsumerInfo{OwnerIdentity: "weird", OwnerAppID: "app_bot"}, false, "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, ok := recoveryCommandContext(oc, tc.c)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (ctx=%+v)", ok, tc.wantOK, ctx)
			}
			if ok && (ctx.Profile != tc.wantProfile || string(ctx.Identity) != tc.wantID) {
				t.Errorf("ctx = %+v, want {Profile:%s Identity:%s}", ctx, tc.wantProfile, tc.wantID)
			}
		})
	}
}

// TestNextAction_ForeignOwner_ReasonOnly locks that an unresolvable owner yields
// a reason-only recovery: NO executable Command/Args and NO --profile leaked —
// never a fallback to the status-invocation profile.
func TestNextAction_ForeignOwner_ReasonOnly(t *testing.T) {
	c := refinedConsumer()
	c.NextAction = "reactivate"
	s := appStatus{AppID: "cli_a", State: stateRunning}
	na := (StatusProjector{}).nextAction(s, c, "", ownerContext{}) // empty map: foreign for all
	if na == nil {
		t.Fatal("expected a reason-only next_action, got nil")
	}
	if na.Command != "" || len(na.Args) != 0 {
		t.Errorf("foreign owner must be reason-only, got Command=%q Args=%v", na.Command, na.Args)
	}
	if na.Reason == "" {
		t.Error("reason must be non-empty")
	}
	if strings.Contains(na.Reason, "--profile") {
		t.Errorf("reason must not carry an executable --profile flag: %q", na.Reason)
	}
}

// TestNextAction_UserMismatch_ReasonOnly locks the user open_id gate.
func TestNextAction_UserMismatch_ReasonOnly(t *testing.T) {
	c := refinedConsumer() // user / cli_a / ou_1
	c.NextAction = "reactivate"
	s := appStatus{AppID: "cli_a", State: stateRunning}
	oc := ownerContext{"cli_a": {Profile: "cli_a", UserOpenID: "ou_DIFFERENT"}}
	na := (StatusProjector{}).nextAction(s, c, "", oc)
	if na == nil || na.Command != "" {
		t.Fatalf("user open_id mismatch must be reason-only, got %+v", na)
	}
}

// TestNextAction_BotOwner_ExecutableWithBotFlag locks that a bot owner is
// executable on a bare profile hit and carries --as bot (no open_id check).
func TestNextAction_BotOwner_ExecutableWithBotFlag(t *testing.T) {
	c := refinedConsumer()
	c.OwnerIdentity = "bot"
	c.OwnerUserOpenID = ""
	c.NextAction = "reactivate"
	s := appStatus{AppID: "cli_a", State: stateRunning}
	oc := ownerContext{"cli_a": {Profile: "cli_a"}}
	na := (StatusProjector{}).nextAction(s, c, "", oc)
	if na == nil || na.Command != "lark-cli event subscription reactivate" {
		t.Fatalf("bot owner should be executable, got %+v", na)
	}
	want := []string{"sub_abc123", "--profile", "cli_a", "--as", "bot"}
	if len(na.Args) != len(want) {
		t.Fatalf("args = %v, want %v", na.Args, want)
	}
	for i, w := range want {
		if na.Args[i] != w {
			t.Errorf("args[%d] = %q, want %q", i, na.Args[i], w)
		}
	}
}
