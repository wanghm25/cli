// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"
	"github.com/spf13/cobra"

	"github.com/larksuite/cli/internal/auth"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/event/protocol"
)

// fakeRefinedGetter is a network-free stand-in for
// *eventlib.SubscriptionClient's Get method (the refinedSubscriptionGetter
// test seam) — mirrors cmd/event/subscription/get_test.go's fakeGetAPI.
type fakeRefinedGetter struct {
	resp  *larkeventv1.GetSubscriptionResp
	err   error
	calls int
}

func (f *fakeRefinedGetter) Get(_ context.Context, _ *larkeventv1.GetSubscriptionReq) (*larkeventv1.GetSubscriptionResp, error) {
	f.calls++
	return f.resp, f.err
}

var errBoom = errors.New("boom: unreachable")

func okGetSubscriptionResp(state string, expireTime int, includeResourceData bool) *larkeventv1.GetSubscriptionResp {
	return &larkeventv1.GetSubscriptionResp{
		ApiResp: &larkcore.ApiResp{RawBody: []byte(`{"code":0}`)},
		Data: &larkeventv1.GetSubscriptionRespData{
			Subscription: &larkeventv1.SubscriptionDetail{
				State:          &state,
				ExpireTime:     &expireTime,
				PayloadOptions: &larkeventv1.PayloadOptions{IncludeResourceData: &includeResourceData},
			},
		},
	}
}

// --- Task 16: local current_profile_match (spec §4.6, no scope, always
// available) --------------------------------------------------------------
//
// consumerProfileMatch is a pure function: it takes an appStatus (already
// annotated with the freshly-resolved current identity — see
// annotateCurrentIdentity below) and one ConsumerInfo, with no Factory/disk/
// network access, so every branch is covered directly with plain literals.

func TestConsumerProfileMatch_TrueWhenOwnerEqualsCurrent(t *testing.T) {
	s := appStatus{
		AppID:                "cli_a",
		CurrentIdentityKnown: true,
		CurrentAppID:         "cli_a",
		CurrentUserOpenID:    "ou_1",
	}
	c := protocol.ConsumerInfo{OwnerAppID: "cli_a", OwnerUserOpenID: "ou_1"}

	match, applicable := consumerProfileMatch(s, c)
	if !applicable {
		t.Fatal("applicable = false, want true")
	}
	if !match {
		t.Error("match = false, want true (owner == current)")
	}
}

func TestConsumerProfileMatch_FalseWhenOwnerDiffers(t *testing.T) {
	s := appStatus{
		CurrentIdentityKnown: true,
		CurrentAppID:         "cli_a",
		CurrentUserOpenID:    "ou_new",
	}
	c := protocol.ConsumerInfo{OwnerAppID: "cli_a", OwnerUserOpenID: "ou_old"}

	match, applicable := consumerProfileMatch(s, c)
	if !applicable {
		t.Fatal("applicable = false, want true")
	}
	if match {
		t.Error("match = true, want false (different owner_user_open_id)")
	}
}

// TestConsumerProfileMatch_NotApplicableForBotOrLegacyConsumer is the
// critical safety guard: OwnerAppID is populated for EVERY consumer on a
// Phase-C bus, including bot/legacy ones (it's just the bus's own AppID,
// spec §4.4) — but OwnerUserOpenID=="" must NEVER be compared as a
// "mismatch" against a real current user, or every bot consumer would show
// up as falsely stale_identity. applicable must be false here regardless of
// whether OwnerAppID happens to equal the current AppID.
func TestConsumerProfileMatch_NotApplicableForBotOrLegacyConsumer(t *testing.T) {
	s := appStatus{
		CurrentIdentityKnown: true,
		CurrentAppID:         "cli_a",
		CurrentUserOpenID:    "ou_1",
	}
	c := protocol.ConsumerInfo{OwnerAppID: "cli_a", OwnerUserOpenID: ""}

	match, applicable := consumerProfileMatch(s, c)
	if applicable {
		t.Errorf("applicable = true, want false for a bot/legacy consumer (OwnerUserOpenID==\"\"); match=%v", match)
	}
	if match {
		t.Error("match must never be true when not applicable")
	}
}

func TestConsumerProfileMatch_NotApplicableWhenCurrentIdentityUnknown(t *testing.T) {
	s := appStatus{CurrentIdentityKnown: false}
	c := protocol.ConsumerInfo{OwnerAppID: "cli_a", OwnerUserOpenID: "ou_1"}

	_, applicable := consumerProfileMatch(s, c)
	if applicable {
		t.Error("applicable = true, want false when no current identity is resolvable")
	}
}

// --- annotateCurrentIdentity ------------------------------------------------

func TestAnnotateCurrentIdentity_OnlyStampsMatchingApp(t *testing.T) {
	statuses := []appStatus{
		{AppID: "cli_a"},
		{AppID: "cli_b"},
	}
	annotateCurrentIdentity(statuses, "cli_a", currentIdentityForMatch{appID: "cli_a", userOpenID: "ou_1"}, true)

	if !statuses[0].CurrentIdentityKnown || statuses[0].CurrentAppID != "cli_a" || statuses[0].CurrentUserOpenID != "ou_1" {
		t.Errorf("cli_a not annotated: %+v", statuses[0])
	}
	if statuses[1].CurrentIdentityKnown || statuses[1].CurrentAppID != "" || statuses[1].CurrentUserOpenID != "" {
		t.Errorf("cli_b (foreign app) must stay zero-valued: %+v", statuses[1])
	}
}

func TestAnnotateCurrentIdentity_UnresolvableCurrentLeavesKnownFalse(t *testing.T) {
	statuses := []appStatus{{AppID: "cli_a"}}
	annotateCurrentIdentity(statuses, "cli_a", currentIdentityForMatch{}, false)
	if statuses[0].CurrentIdentityKnown {
		t.Error("CurrentIdentityKnown = true, want false when curOK is false")
	}
}

// --- refinedNextAction -------------------------------------------------------

func TestRefinedNextAction_EmptyWhenNotApplicableOrMatching(t *testing.T) {
	if got := refinedNextAction(true, true); got != "" {
		t.Errorf("match+applicable: next_action = %q, want empty", got)
	}
	if got := refinedNextAction(false, false); got != "" {
		t.Errorf("not applicable: next_action = %q, want empty", got)
	}
}

func TestRefinedNextAction_NonEmptyOnRealMismatch(t *testing.T) {
	got := refinedNextAction(false, true)
	if got == "" {
		t.Error("expected a non-empty next_action string for a real owner/current mismatch")
	}
	if strings.Contains(strings.ToLower(got), "dead") || strings.Contains(strings.ToLower(got), "inactive") {
		t.Errorf("next_action must not describe the consumer as dead/inactive (advisory-only, spec §4.6): %q", got)
	}
}

// --- writeStatusText: refined sub-line (spec §4.6) --------------------------

func refinedConsumer() protocol.ConsumerInfo {
	return protocol.ConsumerInfo{
		PID:                  123,
		EventKey:             "im.message.created_v1/chat-id/oc_xxx",
		Received:             42,
		RefinedSubscription:  true,
		RemoteSubscriptionID: "sub_abc123",
		OwnerIdentity:        "user",
		OwnerAppID:           "cli_a",
		OwnerUserOpenID:      "ou_1",
	}
}

func TestWriteStatusText_RefinedConsumerSubLine_ShowsRemoteSubIDAndOwner(t *testing.T) {
	var buf bytes.Buffer
	statuses := []appStatus{{
		AppID: "cli_a", State: stateRunning, PID: 1, Active: 1,
		CurrentIdentityKnown: true, CurrentAppID: "cli_a", CurrentUserOpenID: "ou_1",
		Consumers: []protocol.ConsumerInfo{refinedConsumer()},
	}}
	writeStatusText(&buf, statuses)
	out := buf.String()

	for _, want := range []string{"remote_subscription_id=sub_abc123", "current_profile_match=true"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q; full output:\n%s", want, out)
		}
	}
	// Base row must still be present, unchanged in shape.
	if !strings.Contains(out, "pid=123") || !strings.Contains(out, "42") {
		t.Errorf("base consumer row missing/altered; full output:\n%s", out)
	}
}

func TestWriteStatusText_RefinedConsumerSubLine_MismatchShowsAdvisoryAndNextAction(t *testing.T) {
	var buf bytes.Buffer
	c := refinedConsumer()
	c.StaleIdentity = true
	statuses := []appStatus{{
		AppID: "cli_a", State: stateRunning, PID: 1, Active: 1,
		CurrentIdentityKnown: true, CurrentAppID: "cli_a", CurrentUserOpenID: "ou_DIFFERENT",
		Consumers: []protocol.ConsumerInfo{c},
	}}
	writeStatusText(&buf, statuses)
	out := buf.String()

	for _, want := range []string{"current_profile_match=false", "stale_identity", "next_action"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q; full output:\n%s", want, out)
		}
	}
	lower := strings.ToLower(out)
	if strings.Contains(lower, "dead") || strings.Contains(lower, "inactive") {
		t.Errorf("stale_identity must be advisory only, never described as dead/inactive; full output:\n%s", out)
	}
}

func TestWriteStatusText_DegradedReason_ShownAsAdvisory(t *testing.T) {
	var buf bytes.Buffer
	c := refinedConsumer()
	c.DegradedReason = "bind_failed: uat_unavailable"
	statuses := []appStatus{{
		AppID: "cli_a", State: stateRunning, PID: 1, Active: 1,
		Consumers: []protocol.ConsumerInfo{c},
	}}
	writeStatusText(&buf, statuses)
	out := buf.String()
	if !strings.Contains(out, "bind_failed: uat_unavailable") || !strings.Contains(out, "advisory") {
		t.Errorf("degraded_reason not shown as advisory; full output:\n%s", out)
	}
}

// TestWriteStatusText_LegacyConsumerRow_Unchanged is the additive-output
// regression lock: a plain (non-refined) consumer must get NO sub-line at
// all — the legacy table row is untouched by this change.
func TestWriteStatusText_LegacyConsumerRow_Unchanged(t *testing.T) {
	var buf bytes.Buffer
	statuses := []appStatus{{
		AppID: "cli_a", State: stateRunning, PID: 1, Active: 1,
		Consumers: []protocol.ConsumerInfo{{
			PID: 1, EventKey: "mail.x", SubscriptionID: "mail.x:alice", Received: 3, Dropped: 0,
		}},
	}}
	writeStatusText(&buf, statuses)
	out := buf.String()
	for _, unwanted := range []string{"remote_subscription_id", "owner=", "current_profile_match", "advisory"} {
		if strings.Contains(out, unwanted) {
			t.Errorf("legacy consumer output must stay unchanged, but found %q; full output:\n%s", unwanted, out)
		}
	}
}

// --- writeStatusJSON: current_profile_match / next_action overlay ----------

func TestWriteStatusJSON_RefinedConsumer_IncludesCurrentProfileMatch(t *testing.T) {
	var buf bytes.Buffer
	statuses := []appStatus{{
		AppID: "cli_a", State: stateRunning,
		CurrentIdentityKnown: true, CurrentAppID: "cli_a", CurrentUserOpenID: "ou_1",
		Consumers: []protocol.ConsumerInfo{refinedConsumer()},
	}}
	if err := writeStatusJSON(&buf, statuses); err != nil {
		t.Fatalf("writeStatusJSON: %v", err)
	}
	var payload struct {
		Apps []struct {
			Consumers []map[string]interface{} `json:"consumers"`
		} `json:"apps"`
	}
	if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal: %v\nraw: %s", err, buf.String())
	}
	if len(payload.Apps) != 1 || len(payload.Apps[0].Consumers) != 1 {
		t.Fatalf("unexpected shape: %+v", payload)
	}
	c := payload.Apps[0].Consumers[0]
	if c["remote_subscription_id"] != "sub_abc123" {
		t.Errorf("remote_subscription_id = %v, want sub_abc123", c["remote_subscription_id"])
	}
	if c["current_profile_match"] != true {
		t.Errorf("current_profile_match = %v, want true", c["current_profile_match"])
	}
	if c["owner_app_id"] != "cli_a" || c["owner_user_open_id"] != "ou_1" {
		t.Errorf("owner fields missing/wrong: %v", c)
	}
}

func TestWriteStatusJSON_RefinedConsumer_MismatchIncludesNextAction(t *testing.T) {
	var buf bytes.Buffer
	statuses := []appStatus{{
		AppID: "cli_a", State: stateRunning,
		CurrentIdentityKnown: true, CurrentAppID: "cli_a", CurrentUserOpenID: "ou_DIFFERENT",
		Consumers: []protocol.ConsumerInfo{refinedConsumer()},
	}}
	if err := writeStatusJSON(&buf, statuses); err != nil {
		t.Fatalf("writeStatusJSON: %v", err)
	}
	var payload struct {
		Apps []struct {
			Consumers []map[string]interface{} `json:"consumers"`
		} `json:"apps"`
	}
	if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal: %v\nraw: %s", err, buf.String())
	}
	c := payload.Apps[0].Consumers[0]
	if c["current_profile_match"] != false {
		t.Errorf("current_profile_match = %v, want false", c["current_profile_match"])
	}
	action, _ := c["next_action"].(string)
	if action == "" {
		t.Error("next_action missing/empty for a real owner/current mismatch")
	}
	lower := strings.ToLower(action)
	if strings.Contains(lower, "dead") || strings.Contains(lower, "inactive") {
		t.Errorf("next_action must not describe the consumer as dead/inactive: %q", action)
	}
}

func TestWriteStatusJSON_LegacyConsumer_OmitsCurrentProfileMatchKey(t *testing.T) {
	var buf bytes.Buffer
	statuses := []appStatus{{
		AppID: "cli_a", State: stateRunning,
		Consumers: []protocol.ConsumerInfo{{PID: 1, EventKey: "mail.x", Received: 1}},
	}}
	if err := writeStatusJSON(&buf, statuses); err != nil {
		t.Fatalf("writeStatusJSON: %v", err)
	}
	if strings.Contains(buf.String(), "current_profile_match") {
		t.Errorf("current_profile_match must be omitted entirely for a legacy consumer; got:\n%s", buf.String())
	}
}

// TestWriteStatusJSON_RunningOmitsOrphanFields (status_orphan_test.go) and
// friends already lock the legacy consumers==nil/omitted shape; no new test
// needed for that here since consumerView wraps ConsumerInfo via embedding
// (no per-field remapping), so an empty Consumers slice marshals exactly as
// before.

// --- pure remote-supplement helpers (spec §4.6) -----------------------------
//
// remoteSupplementUAT takes an already-resolved *auth.StoredUAToken value
// directly (never calling auth.GetStoredToken itself) — no keychain, no
// disk, no network — so every precondition branch (nil/expired token,
// missing scope, all pass) is exercised with plain literals. This is also
// the "no silent refresh" proof: this function has no way to refresh
// anything, it only reads fields off the struct it was handed.

func TestSupplementRefinedConsumers_FillsRemoteSubscriptionOnSuccess(t *testing.T) {
	consumers := []protocol.ConsumerInfo{refinedConsumer()}
	getter := &fakeRefinedGetter{resp: okGetSubscriptionResp("enabled", 1732000000, true)}

	supplementRefinedConsumers(context.Background(), getter, consumers)

	if consumers[0].RemoteState != "enabled" {
		t.Errorf("RemoteState = %q, want enabled", consumers[0].RemoteState)
	}
	if consumers[0].RemoteSubscription == nil {
		t.Fatal("RemoteSubscription is nil, want filled")
	}
	if consumers[0].RemoteSubscription.State != "enabled" || consumers[0].RemoteSubscription.ExpireTime != 1732000000 || !consumers[0].RemoteSubscription.IncludeResourceData {
		t.Errorf("RemoteSubscription = %+v, want {enabled 1732000000 true}", consumers[0].RemoteSubscription)
	}
	if getter.calls != 1 {
		t.Errorf("Get called %d times, want 1", getter.calls)
	}
}

func TestSupplementRefinedConsumers_SkipsLegacyConsumers(t *testing.T) {
	consumers := []protocol.ConsumerInfo{{PID: 1, EventKey: "mail.x"}} // RemoteSubscriptionID == ""
	getter := &fakeRefinedGetter{resp: okGetSubscriptionResp("enabled", 0, false)}

	supplementRefinedConsumers(context.Background(), getter, consumers)

	if getter.calls != 0 {
		t.Errorf("Get called %d times for a legacy consumer, want 0", getter.calls)
	}
	if consumers[0].RemoteSubscription != nil || consumers[0].RemoteState != "" {
		t.Errorf("legacy consumer must stay local-only: %+v", consumers[0])
	}
}

// TestSupplementRefinedConsumers_ErrorLeavesConsumerLocalOnly locks spec
// §4.6's "unreachable/error -> local-only, no fail" contract: a Get error
// must never propagate (supplementRefinedConsumers has no error return at
// all) and must never partially fill the consumer.
func TestSupplementRefinedConsumers_ErrorLeavesConsumerLocalOnly(t *testing.T) {
	consumers := []protocol.ConsumerInfo{refinedConsumer()}
	getter := &fakeRefinedGetter{err: errBoom}

	supplementRefinedConsumers(context.Background(), getter, consumers)

	if consumers[0].RemoteSubscription != nil || consumers[0].RemoteState != "" {
		t.Errorf("consumer must stay local-only after a Get error: %+v", consumers[0])
	}
}

func TestSupplementRefinedConsumers_NilGetterIsNoOp(t *testing.T) {
	consumers := []protocol.ConsumerInfo{refinedConsumer()}
	supplementRefinedConsumers(context.Background(), nil, consumers) // must not panic
	if consumers[0].RemoteSubscription != nil {
		t.Errorf("nil getter must be a no-op: %+v", consumers[0])
	}
}

// --- applyRefinedSupplement: orchestration, no Factory/network needed ------

func TestApplyRefinedSupplement_WrongApp_GetterNeverResolved(t *testing.T) {
	statuses := []appStatus{{
		AppID: "cli_OTHER", State: stateRunning,
		Consumers: []protocol.ConsumerInfo{refinedConsumer()},
	}}
	calls := 0
	applyRefinedSupplement(context.Background(), statuses, "cli_a", func() refinedSubscriptionGetter {
		calls++
		return &fakeRefinedGetter{resp: okGetSubscriptionResp("enabled", 0, false)}
	})
	if calls != 0 {
		t.Errorf("resolveGetter called %d times for a non-current app, want 0", calls)
	}
	if statuses[0].Consumers[0].RemoteSubscription != nil {
		t.Errorf("foreign app consumer must stay local-only: %+v", statuses[0].Consumers[0])
	}
}

func TestApplyRefinedSupplement_NoRefinedConsumers_GetterNeverResolved(t *testing.T) {
	statuses := []appStatus{{
		AppID: "cli_a", State: stateRunning,
		Consumers: []protocol.ConsumerInfo{{PID: 1, EventKey: "mail.x"}},
	}}
	calls := 0
	applyRefinedSupplement(context.Background(), statuses, "cli_a", func() refinedSubscriptionGetter {
		calls++
		return nil
	})
	if calls != 0 {
		t.Errorf("resolveGetter called %d times when there is nothing refined to supplement, want 0", calls)
	}
}

func TestApplyRefinedSupplement_GetterNil_StaysLocalOnly(t *testing.T) {
	statuses := []appStatus{{
		AppID: "cli_a", State: stateRunning,
		Consumers: []protocol.ConsumerInfo{refinedConsumer()},
	}}
	applyRefinedSupplement(context.Background(), statuses, "cli_a", func() refinedSubscriptionGetter { return nil })
	if statuses[0].Consumers[0].RemoteSubscription != nil {
		t.Errorf("nil getter (failed precondition) must stay local-only: %+v", statuses[0].Consumers[0])
	}
}

func TestApplyRefinedSupplement_Valid_FillsCurrentAppConsumers(t *testing.T) {
	statuses := []appStatus{
		{AppID: "cli_OTHER", State: stateRunning, Consumers: []protocol.ConsumerInfo{refinedConsumer()}},
		{AppID: "cli_a", State: stateRunning, Consumers: []protocol.ConsumerInfo{refinedConsumer()}},
	}
	getter := &fakeRefinedGetter{resp: okGetSubscriptionResp("enabled", 42, true)}
	applyRefinedSupplement(context.Background(), statuses, "cli_a", func() refinedSubscriptionGetter { return getter })

	if statuses[0].Consumers[0].RemoteSubscription != nil {
		t.Errorf("non-current app must stay local-only: %+v", statuses[0].Consumers[0])
	}
	if statuses[1].Consumers[0].RemoteSubscription == nil || statuses[1].Consumers[0].RemoteState != "enabled" {
		t.Errorf("current app consumer not filled: %+v", statuses[1].Consumers[0])
	}
}

// --- remoteSupplementUAT: pure precondition gate, no keychain/network -----
//
// stored is handed in directly (never via getStoredToken/auth.GetStoredToken)
// — this IS the no-refresh proof: the function has no code path that could
// call f.Credential.ResolveToken or anything else capable of refreshing a
// token; it only reads fields off the struct it was given.

func TestRemoteSupplementUAT_NilStored_NotOK(t *testing.T) {
	if _, ok := remoteSupplementUAT(nil, 1000); ok {
		t.Error("expected ok=false for a nil stored token")
	}
}

func TestRemoteSupplementUAT_Expired_NotOK(t *testing.T) {
	stored := &auth.StoredUAToken{AccessToken: "tok", ExpiresAt: 1000, Scope: "event:subscription:read"}
	if _, ok := remoteSupplementUAT(stored, 1000); ok {
		t.Error("expected ok=false when ExpiresAt <= now")
	}
}

func TestRemoteSupplementUAT_MissingScope_NotOK(t *testing.T) {
	stored := &auth.StoredUAToken{AccessToken: "tok", ExpiresAt: 999999999999, Scope: "im:message:send"}
	if _, ok := remoteSupplementUAT(stored, 1000); ok {
		t.Error("expected ok=false when event:subscription:read is not in stored scope")
	}
}

func TestRemoteSupplementUAT_ValidAndScoped_ReturnsToken(t *testing.T) {
	stored := &auth.StoredUAToken{AccessToken: "tok-xyz", ExpiresAt: 999999999999, Scope: "event:subscription:read im:message:send"}
	uat, ok := remoteSupplementUAT(stored, 1000)
	if !ok {
		t.Fatal("expected ok=true for a valid, scoped, unexpired token")
	}
	if uat != "tok-xyz" {
		t.Errorf("uat = %q, want %q", uat, "tok-xyz")
	}
}

// --- resolveRemoteSupplementGetter: Factory-touching glue ------------------
//
// A bare cmdutil.TestFactory config with no UserOpenId set deterministically
// auto-detects to core.AsBot (internal/credential's defaultTokenSource.
// ResolveIdentityHint short-circuits to AutoAs=bot whenever acct.UserOpenId
// is empty, with no keychain lookup at all) — so these two tests exercise
// the bot branch end-to-end (real f.ResolveAs/CheckStrictMode/CheckIdentity/
// f.LarkClient/eventlib.NewSubscriptionClient calls) without ever touching a
// real token store. The core.AsUser branch's own logic (GetStoredToken/
// expiry/scope) is covered directly by the remoteSupplementUAT tests above.

func TestResolveRemoteSupplementGetter_BotIdentity_ReturnsGetterWhenLarkClientAvailable(t *testing.T) {
	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "cli_a", AppSecret: "secret"})
	cmd := &cobra.Command{}

	getter := resolveRemoteSupplementGetter(context.Background(), cmd, f, "cli_a", "", time.Now())
	if getter == nil {
		t.Fatal("expected a non-nil getter for bot identity with a working LarkClient")
	}
}

func TestResolveRemoteSupplementGetter_LarkClientError_ReturnsNil(t *testing.T) {
	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "cli_a", AppSecret: "secret"})
	f.LarkClient = func() (*lark.Client, error) { return nil, errBoom }
	cmd := &cobra.Command{}

	getter := resolveRemoteSupplementGetter(context.Background(), cmd, f, "cli_a", "", time.Now())
	if getter != nil {
		t.Errorf("expected nil getter when f.LarkClient() errors, got %v", getter)
	}
}
