// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/credential"
	"github.com/larksuite/cli/internal/event/buslocal"
	larkgw "github.com/larksuite/cli/internal/event/platform/lark"
)

// fakeListAPI is a network-free stand-in for the platform/lark gateway's List —
// the listSubscriptionsAPI test seam — so listSubscriptions' param-building and
// page-mapping logic is exercised without a real *lark.Client or network call.
// The gateway already pages/projects, so the fake hands back a domain
// SubscriptionPage.
type fakeListAPI struct {
	page *larkgw.SubscriptionPage
	err  error
}

func (f *fakeListAPI) List(_ context.Context, _ larkgw.ListParams) (*larkgw.SubscriptionPage, error) {
	return f.page, f.err
}

// okListResp builds an SDK List response — kept for create_test.go's fakes,
// which still speak the SDK (create stays on the legacy subscription_client
// until PR2b). The migrated list command's own fake hands back a domain
// SubscriptionPage instead.
func okListResp(items []*larkeventv1.SubscriptionDetail, hasMore bool, pageToken string) *larkeventv1.ListSubscriptionResp {
	return &larkeventv1.ListSubscriptionResp{
		ApiResp: &larkcore.ApiResp{RawBody: []byte(`{"code":0}`)},
		Data: &larkeventv1.ListSubscriptionRespData{
			Items:     items,
			HasMore:   boolPtr(hasMore),
			PageToken: strPtr(pageToken),
		},
	}
}

// listPage builds a domain SubscriptionPage from SDK details (projected exactly
// as the gateway would), for the migrated list command's fake.
func listPage(items []*larkeventv1.SubscriptionDetail, hasMore bool, pageToken string) *larkgw.SubscriptionPage {
	page := &larkgw.SubscriptionPage{HasMore: hasMore, NextPageToken: pageToken}
	for _, d := range items {
		page.Items = append(page.Items, larkgw.ProjectSubscription(d))
	}
	return page
}

// TestListSubscriptions_TwoItems_JSONShape is the primary TDD case from the
// task brief: a fake List returning 2 subscriptions must map into
// subscriptions[] (remote_subscription_id/event_key/event_type/...) plus
// has_more/next_page_token.
func TestListSubscriptions_TwoItems_JSONShape(t *testing.T) {
	registerCreateFixtures(t) // so ReverseResolve reconstructs each row's executable event_key
	fake := &fakeListAPI{page: listPage([]*larkeventv1.SubscriptionDetail{
		{
			SubscriptionId: strPtr("sub_1"),
			EventType:      strPtr("im.message.created_v1"),
			TargetResource: strPtr("im.message?chat_id=oc_aaa"),
			Authority:      &larkeventv1.Authority{Type: strPtr("user"), OpenId: strPtr("ou_aaa")},
			State:          strPtr("active"),
			ExpireTime:     intPtr(1732000000),
		},
		{
			SubscriptionId: strPtr("sub_2"),
			EventType:      strPtr("im.message.created_v1"),
			TargetResource: strPtr("im.message?chat_id=oc_bbb"),
			Authority:      &larkeventv1.Authority{Type: strPtr("app")},
			State:          strPtr("suspended"),
			Suspension:     &larkeventv1.Suspension{Code: strPtr("authority_revoked")},
		},
	}, true, "tok_next")}

	result, err := listSubscriptions(context.Background(), fake, listOpts{}, nil)
	if err != nil {
		t.Fatalf("listSubscriptions: unexpected error: %v", err)
	}
	if len(result.Subscriptions) != 2 {
		t.Fatalf("len(Subscriptions) = %d, want 2", len(result.Subscriptions))
	}

	first := result.Subscriptions[0]
	if first.RemoteSubscriptionID != "sub_1" {
		t.Errorf("Subscriptions[0].RemoteSubscriptionID = %q, want sub_1", first.RemoteSubscriptionID)
	}
	// event_key is the reversed, executable materialized key (base + chat-id +
	// value), not the raw event_type.
	if first.EventKey != "im.message.created_v1/chat-id/oc_aaa" {
		t.Errorf("Subscriptions[0].EventKey = %q, want im.message.created_v1/chat-id/oc_aaa", first.EventKey)
	}
	if first.EventType != "im.message.created_v1" {
		t.Errorf("Subscriptions[0].EventType = %q, want im.message.created_v1", first.EventType)
	}
	if first.Identity != "user:ou_aaa" {
		t.Errorf("Subscriptions[0].Identity = %q, want user:ou_aaa", first.Identity)
	}
	if first.Remote.State != "active" {
		t.Errorf("Subscriptions[0].Remote.State = %q, want active", first.Remote.State)
	}

	second := result.Subscriptions[1]
	if second.RemoteSubscriptionID != "sub_2" {
		t.Errorf("Subscriptions[1].RemoteSubscriptionID = %q, want sub_2", second.RemoteSubscriptionID)
	}
	if second.Remote.SuspensionReason != "authority_revoked" {
		t.Errorf("Subscriptions[1].Remote.SuspensionReason = %q, want authority_revoked", second.Remote.SuspensionReason)
	}

	if !result.HasMore {
		t.Error("HasMore = false, want true")
	}
	if result.NextPageToken != "tok_next" {
		t.Errorf("NextPageToken = %q, want tok_next", result.NextPageToken)
	}
	if result.NextAction == "" {
		t.Error("expected a non-empty NextAction when HasMore is true")
	}

	// Round-trip through JSON to pin the wire field names, not just the Go struct.
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var generic map[string]interface{}
	if err := json.Unmarshal(raw, &generic); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	for _, field := range []string{"subscriptions", "has_more", "next_page_token", "next_action"} {
		if _, ok := generic[field]; !ok {
			t.Errorf("JSON output missing top-level field %q; got: %s", field, raw)
		}
	}
	rows, ok := generic["subscriptions"].([]interface{})
	if !ok || len(rows) != 2 {
		t.Fatalf("subscriptions field = %v, want an array of 2", generic["subscriptions"])
	}
	row0, ok := rows[0].(map[string]interface{})
	if !ok {
		t.Fatalf("subscriptions[0] wrong type: %T", rows[0])
	}
	for _, field := range []string{"remote_subscription_id", "event_key", "event_type", "identity", "remote"} {
		if _, ok := row0[field]; !ok {
			t.Errorf("subscriptions[0] missing field %q; got: %v", field, row0)
		}
	}
	if _, present := row0["local"]; present {
		t.Errorf(`subscriptions[0]["local"] present (%v), want omitted`, row0["local"])
	}
}

func TestListSubscriptions_EmptyResult_NoNextAction(t *testing.T) {
	fake := &fakeListAPI{page: listPage(nil, false, "")}

	result, err := listSubscriptions(context.Background(), fake, listOpts{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Subscriptions) != 0 {
		t.Errorf("len(Subscriptions) = %d, want 0", len(result.Subscriptions))
	}
	if result.HasMore {
		t.Error("HasMore = true, want false")
	}
	if result.NextAction != "" {
		t.Errorf("NextAction = %q, want empty when there is no next page", result.NextAction)
	}

	// Must marshal as [] not null, so `jq '.subscriptions[]'` never errors on a fresh account.
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var generic map[string]interface{}
	if err := json.Unmarshal(raw, &generic); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if _, isArray := generic["subscriptions"].([]interface{}); !isArray {
		t.Errorf(`"subscriptions" = %v (%T), want a JSON array`, generic["subscriptions"], generic["subscriptions"])
	}
}

// TestListSubscriptions_SurfacesRunningLocalConsumer locks the list-side
// local-consumer surfacing: a row whose remote_subscription_id matches a queried
// running consumer gets an additive `local` object (running + the consumer);
// a row with no matching consumer leaves `local` omitted — additive, best-effort.
func TestListSubscriptions_SurfacesRunningLocalConsumer(t *testing.T) {
	registerCreateFixtures(t)
	fake := &fakeListAPI{page: listPage([]*larkeventv1.SubscriptionDetail{
		activeDetail("sub_1", false, "user"),
		activeDetail("sub_2", false, "user"),
	}, false, "")}
	// One running consumer, bound to sub_1 only.
	consumers := []buslocal.Consumer{{AppID: "cli_x", PID: 4242, EventKey: "im.message.created_v1/chat-id/oc_aaa", RemoteSubscriptionID: "sub_1"}}

	result, err := listSubscriptions(context.Background(), fake, listOpts{}, consumers)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Subscriptions) != 2 {
		t.Fatalf("len(Subscriptions) = %d, want 2", len(result.Subscriptions))
	}

	sub1 := result.Subscriptions[0]
	if sub1.Local == nil || !sub1.Local.Running {
		t.Fatalf("sub_1 Local = %+v, want a running local consumer", sub1.Local)
	}
	if len(sub1.Local.Consumers) != 1 || sub1.Local.Consumers[0].PID != 4242 {
		t.Errorf("sub_1 Local.Consumers = %+v, want the pid=4242 consumer", sub1.Local.Consumers)
	}
	if result.Subscriptions[1].Local != nil {
		t.Errorf("sub_2 Local = %+v, want nil/omitted (no consumer bound)", result.Subscriptions[1].Local)
	}

	// Pin the wire shape: subscriptions[0].local present, subscriptions[1].local omitted.
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var generic map[string]interface{}
	if err := json.Unmarshal(raw, &generic); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	rows, _ := generic["subscriptions"].([]interface{})
	row0, _ := rows[0].(map[string]interface{})
	if _, present := row0["local"]; !present {
		t.Errorf("subscriptions[0].local absent, want present when a consumer is bound; got %v", row0)
	}
	row1, _ := rows[1].(map[string]interface{})
	if _, present := row1["local"]; present {
		t.Errorf("subscriptions[1].local present (%v), want omitted", row1["local"])
	}
}

func TestListSubscriptions_TransportError_PropagatesTyped(t *testing.T) {
	fake := &fakeListAPI{err: errors.New("boom: connection reset")}

	_, err := listSubscriptions(context.Background(), fake, listOpts{}, nil)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if err.Error() != "boom: connection reset" {
		// listSubscriptions must pass the error through unchanged — it is
		// *eventlib.SubscriptionClient's job (already tested) to
		// wrap it into a typed error; this command layer must not swallow
		// or double-wrap it.
		t.Errorf("err = %v, want it passed through unchanged", err)
	}
}

// TestRunList_MissingReadScope_ReturnsPermissionError exercises the full
// cobra wiring (NewCmdList -> Execute) with a Factory whose credential
// provider reports a token missing event:subscription:read. The scope
// pre-check must fail closed before any client/network call is attempted.
func TestRunList_MissingReadScope_ReturnsPermissionError(t *testing.T) {
	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "cli_x"})
	f.Credential = credential.NewCredentialProvider(nil, nil, &fakeTokenResolver{
		result: &credential.TokenResult{Token: "u-tok", Scopes: "im:message:send"},
	}, nil)

	cmd := NewCmdList(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--as", "user", "--json"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected a permission error, got nil")
	}
	var permErr *errs.PermissionError
	if !errors.As(err, &permErr) {
		t.Fatalf("expected *errs.PermissionError, got %T: %v", err, err)
	}
	if len(permErr.MissingScopes) != 1 || permErr.MissingScopes[0] != "event:subscription:read" {
		t.Errorf("MissingScopes = %v, want [event:subscription:read]", permErr.MissingScopes)
	}
}

func TestNewCmdList_HasExpectedFlags(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdList(f)
	for _, name := range []string{"state", "event-key", "page-size", "page-token", "json", "as"} {
		if cmd.Flags().Lookup(name) == nil {
			t.Errorf("NewCmdList missing --%s flag", name)
		}
	}
	if level, ok := cmdutil.GetRisk(cmd); !ok || level != cmdutil.RiskRead {
		t.Errorf("risk = (%q, %v), want (%q, true)", level, ok, cmdutil.RiskRead)
	}
}
