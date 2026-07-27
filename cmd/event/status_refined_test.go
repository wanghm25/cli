// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"
	"github.com/spf13/cobra"

	"github.com/larksuite/cli/internal/auth"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	eventlib "github.com/larksuite/cli/internal/event"
	larkgw "github.com/larksuite/cli/internal/event/platform/lark"
	"github.com/larksuite/cli/internal/event/protocol"
	"github.com/larksuite/cli/internal/event/session"
)

// fakeRefinedGetter is a network-free stand-in for the platform/lark gateway's
// Get + WalkSubscriptions (the refinedSubscriptionGetter test seam). The gateway
// already unwraps/validates/projects and owns the bounded pagination, so this
// fake just hands back a domain RemoteSubscription (Get) or replays a fixed set
// of items through visit (WalkSubscriptions). The scan's own page bound / cap /
// early-stop mechanics are covered at the gateway (platform/lark's
// TestGateway_WalkSubscriptions_*); here the fake reports capped directly so the
// supplement's dedup/threshold/fan-out/degrade logic is exercised in isolation.
type fakeRefinedGetter struct {
	sub   *larkgw.RemoteSubscription
	err   error
	calls int

	// walk seam: the items the scan yields, whether it reports capped, and any
	// error — plus a call counter so a test can assert the Get-vs-scan switch.
	walkItems  []larkgw.RemoteSubscription
	walkCapped bool
	walkErr    error
	walkCalls  int
}

func (f *fakeRefinedGetter) Get(_ context.Context, _ string) (*larkgw.RemoteSubscription, error) {
	f.calls++
	return f.sub, f.err
}

func (f *fakeRefinedGetter) WalkSubscriptions(_ context.Context, _ larkgw.ListParams, visit func(larkgw.RemoteSubscription) bool) (bool, error) {
	f.walkCalls++
	if f.walkErr != nil {
		return false, f.walkErr
	}
	for _, s := range f.walkItems {
		if !visit(s) {
			return false, nil // caller found everything it needed
		}
	}
	return f.walkCapped, nil
}

func strPtr(s string) *string { return &s }
func boolPtr(b bool) *bool    { return &b }

var errBoom = errors.New("boom: unreachable")

// remoteSub / subRef build the domain fixtures the migrated supplement fakes
// hand back, projected exactly as the gateway would from an SDK detail.
func remoteSub(state string, expireTime int, includeResourceData bool) larkgw.RemoteSubscription {
	return larkgw.ProjectSubscription(&larkeventv1.SubscriptionDetail{
		State:          &state,
		ExpireTime:     &expireTime,
		PayloadOptions: &larkeventv1.PayloadOptions{IncludeResourceData: &includeResourceData},
	})
}

func remoteSubID(id, state string) larkgw.RemoteSubscription {
	return larkgw.ProjectSubscription(&larkeventv1.SubscriptionDetail{SubscriptionId: &id, State: &state})
}

func subRef(s larkgw.RemoteSubscription) *larkgw.RemoteSubscription { return &s }

// --- local current_profile_match (no scope, always available) ---
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
// bus, including bot/legacy ones (it's just the bus's own AppID)
// — but OwnerUserOpenID=="" must NEVER be compared as a
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
	annotateCurrentIdentity(statuses, "cli_a", session.CurrentIdentity{AppID: "cli_a", UserOpenID: "ou_1"}, true)

	if !statuses[0].CurrentIdentityKnown || statuses[0].CurrentAppID != "cli_a" || statuses[0].CurrentUserOpenID != "ou_1" {
		t.Errorf("cli_a not annotated: %+v", statuses[0])
	}
	if statuses[1].CurrentIdentityKnown || statuses[1].CurrentAppID != "" || statuses[1].CurrentUserOpenID != "" {
		t.Errorf("cli_b (foreign app) must stay zero-valued: %+v", statuses[1])
	}
}

func TestAnnotateCurrentIdentity_UnresolvableCurrentLeavesKnownFalse(t *testing.T) {
	statuses := []appStatus{{AppID: "cli_a"}}
	annotateCurrentIdentity(statuses, "cli_a", session.CurrentIdentity{}, false)
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
		t.Errorf("next_action must not describe the consumer as dead/inactive (advisory-only): %q", got)
	}
}

// --- writeStatusText: refined sub-line ---

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

// --- remote filter supplement ---

// TestMapRemoteSubscriptionInfo_WithFilter_SurfacesCanonicalJSON proves the
// remote supplement surfaces a subscription's server-side filter as canonical
// JSON on RemoteSubscriptionInfo.
func TestMapRemoteSubscriptionInfo_WithFilter_SurfacesCanonicalJSON(t *testing.T) {
	f, err := eventlib.ParseAndValidateFilter(
		`{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"message_type","op":"in","list_value":["text"]}}]}}`,
		eventlib.FilterMetaFor("im.message.created_v1"))
	if err != nil {
		t.Fatalf("build filter: %v", err)
	}
	info := mapRemoteSubscriptionInfo(larkgw.ProjectSubscription(&larkeventv1.SubscriptionDetail{State: strPtr("enabled"), Filter: eventlib.FilterToSDK(f)}))
	if len(info.Filter) == 0 {
		t.Fatal("info.Filter empty, want canonical JSON")
	}
	want, err := f.Canonicalize()
	if err != nil {
		t.Fatalf("canonicalize: %v", err)
	}
	if !bytes.Equal(info.Filter, want) {
		t.Errorf("info.Filter = %s, want %s", info.Filter, want)
	}
}

func TestMapRemoteSubscriptionInfo_NoFilter_OmitsFilter(t *testing.T) {
	info := mapRemoteSubscriptionInfo(larkgw.ProjectSubscription(&larkeventv1.SubscriptionDetail{State: strPtr("enabled")}))
	if info.Filter != nil {
		t.Errorf("info.Filter = %s, want nil/omitted when the SDK omitted it", info.Filter)
	}
}

// TestWriteStatusText_RefinedConsumerSubLine_ShowsRemoteFilterWhenPresent locks
// that a supplemented remote filter is rendered on its own indented line.
func TestWriteStatusText_RefinedConsumerSubLine_ShowsRemoteFilterWhenPresent(t *testing.T) {
	var buf bytes.Buffer
	c := refinedConsumer()
	c.RemoteSubscription = &protocol.RemoteSubscriptionInfo{
		State:  "enabled",
		Filter: json.RawMessage(`{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"message_type","op":"in","list_value":["text"]}}]}}`),
	}
	statuses := []appStatus{{
		AppID: "cli_a", State: stateRunning, PID: 1, Active: 1,
		Consumers: []protocol.ConsumerInfo{c},
	}}
	writeStatusText(&buf, statuses)
	out := buf.String()
	if !strings.Contains(out, "filter=") || !strings.Contains(out, "message_type") {
		t.Errorf("output missing the remote filter line; full output:\n%s", out)
	}
}

// --- decrypt observability display ---

func TestDecryptAdvisory_PerState(t *testing.T) {
	// healthy / plaintext: no advisory.
	if adv, act := decryptAdvisory(protocol.ConsumerInfo{DecryptState: "decrypted"}); adv != "" || act != "" {
		t.Errorf("decrypted: got (%q,%q), want empty", adv, act)
	}
	if adv, _ := decryptAdvisory(protocol.ConsumerInfo{}); adv != "" {
		t.Errorf("plaintext: got advisory %q, want empty", adv)
	}
	// key unavailable: advisory + scope/identity next_action.
	adv, act := decryptAdvisory(protocol.ConsumerInfo{DecryptState: "decrypt_key_unavailable"})
	if !strings.Contains(adv, "decrypt_key_unavailable") {
		t.Errorf("key-unavailable advisory = %q", adv)
	}
	if !strings.Contains(act, "event:encrypt_key:read") {
		t.Errorf("key-unavailable next_action = %q, want it to mention the scope", act)
	}
	// decrypt failed: advisory carries the last_decrypt_error detail.
	adv, act = decryptAdvisory(protocol.ConsumerInfo{
		DecryptState:     "decrypt_failed",
		LastDecryptError: &protocol.DecryptError{Class: "decrypt_failed", Count: 5, Time: "2026-07-23T00:00:00Z"},
	})
	if !strings.Contains(adv, "count=5") {
		t.Errorf("decrypt_failed advisory = %q, want it to carry count=5", adv)
	}
	if act == "" {
		t.Errorf("decrypt_failed next_action must be non-empty")
	}
}

func TestWriteStatusText_DecryptKeyUnavailable_ShowsResourceDataAndAdvisory(t *testing.T) {
	var buf bytes.Buffer
	c := refinedConsumer()
	c.DecryptState = "decrypt_key_unavailable"
	c.ResourceData = "unavailable"
	statuses := []appStatus{{
		AppID: "cli_a", State: stateRunning, PID: 1, Active: 1,
		CurrentIdentityKnown: true, CurrentAppID: "cli_a", CurrentUserOpenID: "ou_1",
		Consumers: []protocol.ConsumerInfo{c},
	}}
	writeStatusText(&buf, statuses)
	out := buf.String()
	for _, want := range []string{"resource_data=unavailable", "decrypt_state=decrypt_key_unavailable", "advisory", "event:encrypt_key:read", "next_action"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q; full output:\n%s", want, out)
		}
	}
	// Advisory-only: never described as dead.
	if strings.Contains(strings.ToLower(out), "dead") {
		t.Errorf("decrypt advisory must be informational only; full output:\n%s", out)
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

// --- pure remote-supplement helpers ---
//
// remoteSupplementUAT takes an already-resolved *auth.StoredUAToken value
// directly (never calling auth.GetStoredToken itself) — no keychain, no
// disk, no network — so every precondition branch (nil/expired token,
// missing scope, all pass) is exercised with plain literals. This is also
// the "no silent refresh" proof: this function has no way to refresh
// anything, it only reads fields off the struct it was handed.

func TestSupplementRefinedConsumers_FillsRemoteSubscriptionOnSuccess(t *testing.T) {
	consumers := []protocol.ConsumerInfo{refinedConsumer()}
	getter := &fakeRefinedGetter{sub: subRef(remoteSub("enabled", 1732000000, true))}

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
	getter := &fakeRefinedGetter{sub: subRef(remoteSub("enabled", 0, false))}

	supplementRefinedConsumers(context.Background(), getter, consumers)

	if getter.calls != 0 {
		t.Errorf("Get called %d times for a legacy consumer, want 0", getter.calls)
	}
	if consumers[0].RemoteSubscription != nil || consumers[0].RemoteState != "" {
		t.Errorf("legacy consumer must stay local-only: %+v", consumers[0])
	}
}

// TestSupplementRefinedConsumers_ErrorLeavesConsumerLocalOnly locks the
// "unreachable/error -> local-only, no fail" contract: a Get error
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

// --- issue #8: dedup by remote_subscription_id + List-above-threshold ---

// TestSupplementRefinedConsumers_DedupsSharedRemoteSubscriptionID locks the
// dedup fix: two consumers sharing the SAME remote_subscription_id must
// trigger exactly ONE Get, with the single result fanned out to both —
// never a duplicate Get per consumer.
func TestSupplementRefinedConsumers_DedupsSharedRemoteSubscriptionID(t *testing.T) {
	c1 := refinedConsumer()
	c2 := refinedConsumer()
	c2.PID = 456 // a second, DIFFERENT consumer bound to the SAME remote_subscription_id
	consumers := []protocol.ConsumerInfo{c1, c2}
	getter := &fakeRefinedGetter{sub: subRef(remoteSub("active", 42, false))}

	supplementRefinedConsumers(context.Background(), getter, consumers)

	if getter.calls != 1 {
		t.Errorf("Get called %d times, want exactly 1 (both consumers share remote_subscription_id=%s)", getter.calls, c1.RemoteSubscriptionID)
	}
	for i, c := range consumers {
		if c.RemoteState != "active" {
			t.Errorf("consumers[%d].RemoteState = %q, want active (fanned out from the single dedup'd Get)", i, c.RemoteState)
		}
	}
}

// TestSupplementRefinedConsumers_ManyDistinctIDs_UsesScanNotManyGets locks
// the threshold fix: more than remoteSupplementListThreshold DISTINCT
// remote_subscription_ids must use ONE bounded scan (WalkSubscriptions) instead
// of one Get per id.
func TestSupplementRefinedConsumers_ManyDistinctIDs_UsesScanNotManyGets(t *testing.T) {
	n := remoteSupplementListThreshold + 1
	consumers := make([]protocol.ConsumerInfo, 0, n)
	items := make([]larkgw.RemoteSubscription, 0, n)
	for i := 0; i < n; i++ {
		c := refinedConsumer()
		c.PID = 100 + i
		c.RemoteSubscriptionID = fmt.Sprintf("sub_%d", i)
		consumers = append(consumers, c)
		items = append(items, remoteSubID(c.RemoteSubscriptionID, "active"))
	}
	getter := &fakeRefinedGetter{walkItems: items}

	supplementRefinedConsumers(context.Background(), getter, consumers)

	if getter.walkCalls != 1 {
		t.Errorf("scan called %d times, want 1", getter.walkCalls)
	}
	if getter.calls != 0 {
		t.Errorf("Get called %d times, want 0 (the scan substitutes for per-id Get above the threshold)", getter.calls)
	}
	for i, c := range consumers {
		if c.RemoteState != "active" {
			t.Errorf("consumers[%d].RemoteState = %q, want active", i, c.RemoteState)
		}
	}
}

// TestSupplementRefinedConsumers_AtThreshold_StillUsesGet locks the boundary:
// exactly remoteSupplementListThreshold distinct ids must still use Get (one
// per id), not List — only STRICTLY MORE than the threshold switches over.
func TestSupplementRefinedConsumers_AtThreshold_StillUsesGet(t *testing.T) {
	n := remoteSupplementListThreshold
	consumers := make([]protocol.ConsumerInfo, 0, n)
	for i := 0; i < n; i++ {
		c := refinedConsumer()
		c.PID = 100 + i
		c.RemoteSubscriptionID = fmt.Sprintf("sub_%d", i)
		consumers = append(consumers, c)
	}
	getter := &fakeRefinedGetter{sub: subRef(remoteSub("active", 0, false))}

	supplementRefinedConsumers(context.Background(), getter, consumers)

	if getter.calls != n {
		t.Errorf("Get called %d times, want %d (one per distinct id, at the threshold)", getter.calls, n)
	}
	if getter.walkCalls != 0 {
		t.Errorf("scan called %d times, want 0 (threshold not yet exceeded)", getter.walkCalls)
	}
}

// TestSupplementRefinedConsumers_ScanMissingID_StaysLocalOnly: an id the scan
// never yields stays local-only, same "unreachable -> local-only, no fail"
// contract as an errored/empty Get.
func TestSupplementRefinedConsumers_ScanMissingID_StaysLocalOnly(t *testing.T) {
	n := remoteSupplementListThreshold + 1
	consumers := make([]protocol.ConsumerInfo, 0, n)
	for i := 0; i < n; i++ {
		c := refinedConsumer()
		c.PID = 100 + i
		c.RemoteSubscriptionID = fmt.Sprintf("sub_missing_%d", i)
		consumers = append(consumers, c)
	}
	// The scan yields no items at all -> every id is "not found".
	getter := &fakeRefinedGetter{}

	supplementRefinedConsumers(context.Background(), getter, consumers)

	for i, c := range consumers {
		if c.RemoteSubscription != nil || c.RemoteState != "" {
			t.Errorf("consumers[%d] must stay local-only when its id isn't in the scan: %+v", i, c)
		}
	}
}

// TestSupplementRefinedConsumers_ScanYieldsAllWantedIDs_Supplemented locks that
// every wanted remote_subscription_id the bounded scan yields is supplemented
// via a single scan. The scan's own multi-page mechanics (a wanted id first
// appearing on a later page, the page cap) are the gateway's concern and are
// covered by platform/lark's TestGateway_WalkSubscriptions_*; here the seam
// hands back the found items directly.
func TestSupplementRefinedConsumers_ScanYieldsAllWantedIDs_Supplemented(t *testing.T) {
	n := remoteSupplementListThreshold + 1
	consumers := make([]protocol.ConsumerInfo, 0, n)
	items := make([]larkgw.RemoteSubscription, 0, n)
	for i := 0; i < n; i++ {
		c := refinedConsumer()
		c.PID = 100 + i
		c.RemoteSubscriptionID = fmt.Sprintf("sub_%d", i)
		consumers = append(consumers, c)
		items = append(items, remoteSubID(c.RemoteSubscriptionID, "active"))
	}
	getter := &fakeRefinedGetter{walkItems: items}

	capped := supplementRefinedConsumers(context.Background(), getter, consumers)

	if capped {
		t.Error("capped = true, want false (every wanted id was found)")
	}
	for i, c := range consumers {
		if c.RemoteState != "active" {
			t.Errorf("consumers[%d].RemoteState = %q, want active (id=%s)", i, c.RemoteState, c.RemoteSubscriptionID)
		}
	}
	if getter.walkCalls != 1 {
		t.Errorf("scan called %d times, want exactly 1", getter.walkCalls)
	}
}

// TestSupplementRefinedConsumers_CappedScan_DegradesGracefully_NotFalseNotExist
// locks the "incomplete scan must never be read as not-exist" principle: when
// the bounded scan reports capped and never yielded any of the wanted ids, every
// consumer stays local-only (the SAME degrade an unreachable Get produces) —
// capped must bubble up so the caller logs it, rather than the supplement
// silently asserting these subscriptions don't exist. The scan's page-cap
// bound itself is covered at the gateway (TestGateway_WalkSubscriptions_HitsCap).
func TestSupplementRefinedConsumers_CappedScan_DegradesGracefully_NotFalseNotExist(t *testing.T) {
	n := remoteSupplementListThreshold + 1
	consumers := make([]protocol.ConsumerInfo, 0, n)
	for i := 0; i < n; i++ {
		c := refinedConsumer()
		c.PID = 100 + i
		c.RemoteSubscriptionID = fmt.Sprintf("sub_missing_%d", i)
		consumers = append(consumers, c)
	}
	// The scan reports capped and only ever yielded unrelated subscriptions.
	getter := &fakeRefinedGetter{
		walkItems:  []larkgw.RemoteSubscription{remoteSubID("unrelated_1", "active")},
		walkCapped: true,
	}

	capped := supplementRefinedConsumers(context.Background(), getter, consumers)

	if !capped {
		t.Error("capped = false, want true (the bounded scan reported capped with none of the wanted ids found)")
	}
	for i, c := range consumers {
		if c.RemoteSubscription != nil || c.RemoteState != "" {
			t.Errorf("consumers[%d] must degrade to local-only on a capped scan, got: %+v", i, c)
		}
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
		return &fakeRefinedGetter{sub: subRef(remoteSub("enabled", 0, false))}
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
	getter := &fakeRefinedGetter{sub: subRef(remoteSub("enabled", 42, true))}
	applyRefinedSupplement(context.Background(), statuses, "cli_a", func() refinedSubscriptionGetter { return getter })

	if statuses[0].Consumers[0].RemoteSubscription != nil {
		t.Errorf("non-current app must stay local-only: %+v", statuses[0].Consumers[0])
	}
	if statuses[1].Consumers[0].RemoteSubscription == nil || statuses[1].Consumers[0].RemoteState != "enabled" {
		t.Errorf("current app consumer not filled: %+v", statuses[1].Consumers[0])
	}
}

// TestApplyRefinedSupplement_PaginationCapped_BubblesUpToCaller locks that
// applyRefinedSupplement's own return value (what runStatus logs a warning
// from) faithfully bubbles up supplementRefinedConsumers's capped result —
// the orchestration layer must not swallow it.
func TestApplyRefinedSupplement_PaginationCapped_BubblesUpToCaller(t *testing.T) {
	n := remoteSupplementListThreshold + 1
	consumers := make([]protocol.ConsumerInfo, 0, n)
	for i := 0; i < n; i++ {
		c := refinedConsumer()
		c.PID = 100 + i
		c.RemoteSubscriptionID = fmt.Sprintf("sub_missing_%d", i)
		consumers = append(consumers, c)
	}
	statuses := []appStatus{{AppID: "cli_a", State: stateRunning, Consumers: consumers}}
	getter := &fakeRefinedGetter{
		walkItems:  []larkgw.RemoteSubscription{remoteSubID("unrelated_1", "active")},
		walkCapped: true,
	}

	capped := applyRefinedSupplement(context.Background(), statuses, "cli_a", func() refinedSubscriptionGetter { return getter })

	if !capped {
		t.Error("capped = false, want true (must bubble up from supplementRefinedConsumers)")
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

// --- degraded-advisory + stale-identity phrasing ---
//
// Degraded advisory: a KNOWN degraded remote_state
// ("suspended"/"expired") gets a derived, display-only advisory appended
// alongside any pre-existing bus-side degraded_reason/stale_identity
// advisory; any OTHER (open-vocabulary / healthy, e.g. "active"/"enabled")
// remote_state gets none — no guessing. Stale-identity phrasing: the stale_identity
// advisory is phrased as an earlier-snapshot note, never a current-mismatch
// assertion, when the FRESHLY-computed current_profile_match is true.

// okGetSubscriptionRespSuspended mirrors okGetSubscriptionResp above but
// additionally sets Suspension.Code, for exercising supplementRefinedConsumers'
// verbatim SuspensionCode capture. suspensionCode=="" omits
// the Suspension object entirely (a suspended response with no suspension
// details, which must still produce a suspended advisory with no code).
func suspendedRemoteSub(suspensionCode string) larkgw.RemoteSubscription {
	d := &larkeventv1.SubscriptionDetail{State: strPtr("suspended")}
	if suspensionCode != "" {
		d.Suspension = &larkeventv1.Suspension{Code: &suspensionCode}
	}
	return larkgw.ProjectSubscription(d)
}

// --- remoteDegradedAdvisory: pure function, no ctx/getter/bus involved at
// all (the strongest possible proof that it introduces no remote call and
// no bus/Conn write: the function signature has nothing capable of either).

func TestRemoteDegradedAdvisory_Suspended_IncludesCodeWhenPresent(t *testing.T) {
	c := protocol.ConsumerInfo{
		RemoteState:        "suspended",
		RemoteSubscription: &protocol.RemoteSubscriptionInfo{State: "suspended", SuspensionCode: "app_ticket_expired"},
	}
	got := remoteDegradedAdvisory(c)
	if !strings.Contains(got, "suspended") || !strings.Contains(got, "app_ticket_expired") {
		t.Errorf("remoteDegradedAdvisory = %q, want it to mention suspended and the code verbatim", got)
	}
}

func TestRemoteDegradedAdvisory_Suspended_NoCodeStillAdvises(t *testing.T) {
	c := protocol.ConsumerInfo{RemoteState: "suspended"}
	got := remoteDegradedAdvisory(c)
	if got == "" || !strings.Contains(got, "suspended") {
		t.Errorf("remoteDegradedAdvisory = %q, want a non-empty suspended advisory even with no captured code", got)
	}
}

func TestRemoteDegradedAdvisory_Expired(t *testing.T) {
	c := protocol.ConsumerInfo{RemoteState: "expired"}
	got := remoteDegradedAdvisory(c)
	if got == "" || !strings.Contains(got, "expired") {
		t.Errorf("remoteDegradedAdvisory = %q, want a non-empty expired advisory", got)
	}
}

// TestRemoteDegradedAdvisory_UnknownOrHealthyState_ReturnsEmpty is the core
// "do not guess the open vocabulary" guard: any state other than the two
// grounded values — including a healthy value like "active"/"enabled" —
// must NOT synthesize a degraded advisory.
func TestRemoteDegradedAdvisory_UnknownOrHealthyState_ReturnsEmpty(t *testing.T) {
	for _, state := range []string{"active", "enabled", "some_future_state", ""} {
		c := protocol.ConsumerInfo{RemoteState: state}
		if got := remoteDegradedAdvisory(c); got != "" {
			t.Errorf("remoteDegradedAdvisory(remote_state=%q) = %q, want empty (no guessing)", state, got)
		}
	}
}

// --- writeStatusText: text-output integration ------------------------------

func TestWriteStatusText_RemoteStateSuspended_ShowsDegradedAdvisory(t *testing.T) {
	var buf bytes.Buffer
	c := refinedConsumer()
	c.RemoteState = "suspended"
	c.RemoteSubscription = &protocol.RemoteSubscriptionInfo{State: "suspended", SuspensionCode: "app_ticket_expired"}
	statuses := []appStatus{{
		AppID: "cli_a", State: stateRunning, PID: 1, Active: 1,
		Consumers: []protocol.ConsumerInfo{c},
	}}
	writeStatusText(&buf, statuses)
	out := buf.String()
	for _, want := range []string{"remote_state=suspended", "suspended", "app_ticket_expired", "advisory"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q; full output:\n%s", want, out)
		}
	}
}

func TestWriteStatusText_RemoteStateExpired_ShowsDegradedAdvisory(t *testing.T) {
	var buf bytes.Buffer
	c := refinedConsumer()
	c.RemoteState = "expired"
	statuses := []appStatus{{
		AppID: "cli_a", State: stateRunning, PID: 1, Active: 1,
		Consumers: []protocol.ConsumerInfo{c},
	}}
	writeStatusText(&buf, statuses)
	out := buf.String()
	for _, want := range []string{"remote_state=expired", "expired", "advisory"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q; full output:\n%s", want, out)
		}
	}
}

// TestWriteStatusText_RemoteStateUnknownHealthy_NoDegradedAdvisory locks the
// "do not guess" contract at the display-integration level: an
// unknown/healthy remote_state still shows remote_state= verbatim but adds
// no synthesized "remote subscription ..." advisory line.
func TestWriteStatusText_RemoteStateUnknownHealthy_NoDegradedAdvisory(t *testing.T) {
	var buf bytes.Buffer
	c := refinedConsumer()
	c.RemoteState = "active"
	statuses := []appStatus{{
		AppID: "cli_a", State: stateRunning, PID: 1, Active: 1,
		Consumers: []protocol.ConsumerInfo{c},
	}}
	writeStatusText(&buf, statuses)
	out := buf.String()
	if !strings.Contains(out, "remote_state=active") {
		t.Errorf("output missing remote_state=active; full output:\n%s", out)
	}
	if strings.Contains(out, "remote subscription") {
		t.Errorf("must not synthesize a degraded advisory for an unknown/healthy remote_state; full output:\n%s", out)
	}
}

// TestWriteStatusText_RemoteDegradedAdvisory_AppendsAlongsideDegradedReason is
// the "APPEND, do not clobber" guard: a pre-existing bus-side degraded_reason
// advisory must still be shown in full alongside the new remote-derived one
// — neither replaces the other.
func TestWriteStatusText_RemoteDegradedAdvisory_AppendsAlongsideDegradedReason(t *testing.T) {
	var buf bytes.Buffer
	c := refinedConsumer()
	c.DegradedReason = "bind_failed: uat_unavailable"
	c.RemoteState = "suspended"
	statuses := []appStatus{{
		AppID: "cli_a", State: stateRunning, PID: 1, Active: 1,
		Consumers: []protocol.ConsumerInfo{c},
	}}
	writeStatusText(&buf, statuses)
	out := buf.String()
	for _, want := range []string{"bind_failed: uat_unavailable", "suspended"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q (both advisories must coexist); full output:\n%s", want, out)
		}
	}
	if strings.Count(out, "advisory") < 2 {
		t.Errorf("expected at least 2 advisory lines (bus-side degraded_reason + remote-derived), got output:\n%s", out)
	}
}

// --- writeStatusJSON: --json equivalents -----------------------------------

func consumerJSONFromStatuses(t *testing.T, statuses []appStatus) map[string]interface{} {
	t.Helper()
	var buf bytes.Buffer
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
	return payload.Apps[0].Consumers[0]
}

func TestWriteStatusJSON_RemoteStateSuspended_IncludesDegradedAdvisory(t *testing.T) {
	c := refinedConsumer()
	c.RemoteState = "suspended"
	c.RemoteSubscription = &protocol.RemoteSubscriptionInfo{State: "suspended", SuspensionCode: "app_ticket_expired"}
	statuses := []appStatus{{AppID: "cli_a", State: stateRunning, Consumers: []protocol.ConsumerInfo{c}}}

	cv := consumerJSONFromStatuses(t, statuses)
	advisory, _ := cv["remote_degraded_advisory"].(string)
	if advisory == "" || !strings.Contains(advisory, "suspended") || !strings.Contains(advisory, "app_ticket_expired") {
		t.Errorf("remote_degraded_advisory = %v, want a suspended advisory including the code", cv["remote_degraded_advisory"])
	}
}

func TestWriteStatusJSON_RemoteStateUnknownHealthy_OmitsDegradedAdvisoryKey(t *testing.T) {
	c := refinedConsumer()
	c.RemoteState = "enabled"
	statuses := []appStatus{{AppID: "cli_a", State: stateRunning, Consumers: []protocol.ConsumerInfo{c}}}

	cv := consumerJSONFromStatuses(t, statuses)
	if _, present := cv["remote_degraded_advisory"]; present {
		t.Errorf("remote_degraded_advisory must be omitted for an unknown/healthy remote_state, got %v", cv["remote_degraded_advisory"])
	}
	if cv["remote_state"] != "enabled" {
		t.Errorf("remote_state = %v, want enabled (still shown verbatim)", cv["remote_state"])
	}
}

func TestWriteStatusJSON_RemoteDegradedAdvisory_AppendsAlongsideDegradedReason(t *testing.T) {
	c := refinedConsumer()
	c.DegradedReason = "bind_failed: uat_unavailable"
	c.RemoteState = "expired"
	statuses := []appStatus{{AppID: "cli_a", State: stateRunning, Consumers: []protocol.ConsumerInfo{c}}}

	cv := consumerJSONFromStatuses(t, statuses)
	if cv["degraded_reason"] != "bind_failed: uat_unavailable" {
		t.Errorf("degraded_reason = %v, want it preserved untouched", cv["degraded_reason"])
	}
	advisory, _ := cv["remote_degraded_advisory"].(string)
	if advisory == "" || !strings.Contains(advisory, "expired") {
		t.Errorf("remote_degraded_advisory = %v, want a non-empty expired advisory", cv["remote_degraded_advisory"])
	}
}

func TestWriteStatusJSON_LegacyConsumer_OmitsRemoteDegradedAdvisoryKey(t *testing.T) {
	statuses := []appStatus{{
		AppID: "cli_a", State: stateRunning,
		Consumers: []protocol.ConsumerInfo{{PID: 1, EventKey: "mail.x", Received: 1}},
	}}
	cv := consumerJSONFromStatuses(t, statuses)
	if _, present := cv["remote_degraded_advisory"]; present {
		t.Errorf("remote_degraded_advisory must be omitted for a legacy consumer, got %v", cv["remote_degraded_advisory"])
	}
}

// --- supplementRefinedConsumers: SuspensionCode capture, still exactly ONE
// Get call (no extra remote call introduced) ---

func TestSupplementRefinedConsumers_CapturesSuspensionCodeVerbatim(t *testing.T) {
	consumers := []protocol.ConsumerInfo{refinedConsumer()}
	getter := &fakeRefinedGetter{sub: subRef(suspendedRemoteSub("app_ticket_expired"))}

	supplementRefinedConsumers(context.Background(), getter, consumers)

	if consumers[0].RemoteSubscription == nil || consumers[0].RemoteSubscription.SuspensionCode != "app_ticket_expired" {
		t.Errorf("SuspensionCode not captured verbatim: %+v", consumers[0].RemoteSubscription)
	}
	if getter.calls != 1 {
		t.Errorf("Get called %d times, want exactly 1 (no extra remote call introduced)", getter.calls)
	}
}

// --- staleIdentityAdvisory ---

func TestStaleIdentityAdvisory_FreshMatchTrue_EarlierSnapshotWording(t *testing.T) {
	got := staleIdentityAdvisory(true, true)
	if strings.Contains(got, "does not match the current profile") {
		t.Errorf("staleIdentityAdvisory(match=true) must not assert a current mismatch: %q", got)
	}
	if !strings.Contains(strings.ToLower(got), "earlier") {
		t.Errorf("staleIdentityAdvisory(match=true) = %q, want it framed as an earlier-snapshot advisory", got)
	}
	lower := strings.ToLower(got)
	if strings.Contains(lower, "dead") || strings.Contains(lower, "inactive") {
		t.Errorf("must never describe the consumer as dead/inactive: %q", got)
	}
}

func TestStaleIdentityAdvisory_FreshMismatch_KeepsExistingWording(t *testing.T) {
	got := staleIdentityAdvisory(false, true)
	if !strings.Contains(got, "does not match the current profile") {
		t.Errorf("staleIdentityAdvisory(match=false, applicable=true) = %q, want the existing current-mismatch wording preserved", got)
	}
}

func TestStaleIdentityAdvisory_NotApplicable_NeverAssertsCurrentMismatch(t *testing.T) {
	got := staleIdentityAdvisory(false, false)
	if strings.Contains(got, "does not match the current profile") {
		t.Errorf("staleIdentityAdvisory(applicable=false) must not assert an unverified current mismatch: %q", got)
	}
}

// TestWriteStatusText_StaleIdentityWithFreshMatch_NoContradictoryMismatchLine
// exercises the scenario where current_profile_match=true (FRESH) but
// StaleIdentity is still set (an earlier evaluation's leftover flag).
// The two lines must never contradict each other, and the
// consumer must never be described as dead/inactive.
func TestWriteStatusText_StaleIdentityWithFreshMatch_NoContradictoryMismatchLine(t *testing.T) {
	var buf bytes.Buffer
	c := refinedConsumer()
	c.StaleIdentity = true
	statuses := []appStatus{{
		AppID: "cli_a", State: stateRunning, PID: 1, Active: 1,
		CurrentIdentityKnown: true, CurrentAppID: "cli_a", CurrentUserOpenID: "ou_1", // matches refinedConsumer()'s owner
		Consumers: []protocol.ConsumerInfo{c},
	}}
	writeStatusText(&buf, statuses)
	out := buf.String()

	if !strings.Contains(out, "current_profile_match=true") {
		t.Errorf("output missing current_profile_match=true; full output:\n%s", out)
	}
	if strings.Contains(out, "does not match the current profile") {
		t.Errorf("current_profile_match=true must never be followed by a current-mismatch assertion; full output:\n%s", out)
	}
	if !strings.Contains(strings.ToLower(out), "earlier") {
		t.Errorf("expected the stale flag framed as an earlier-snapshot advisory; full output:\n%s", out)
	}
	lower := strings.ToLower(out)
	if strings.Contains(lower, "dead") || strings.Contains(lower, "inactive") {
		t.Errorf("must never describe the consumer as dead/inactive; full output:\n%s", out)
	}
	// next_action must NOT be suggested when the fresh check says it matches.
	if strings.Contains(out, "next_action") {
		t.Errorf("no next_action expected when current_profile_match=true; full output:\n%s", out)
	}
}
