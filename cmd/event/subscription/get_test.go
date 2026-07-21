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
)

// fakeGetAPI is a network-free stand-in for *eventlib.SubscriptionClient's
// Get method — the getSubscriptionAPI test seam. Note it stands in for the
// already-classifying client (Task 7's SubscriptionClient.Get), not the raw
// SDK service: a business failure there is already surfaced as a non-nil
// typed `err` (see internal/event/subscription_client.go's classifyFailure),
// so fixtures simulating a failure set `err`, never a non-zero-code `resp`
// with a nil `err`.
type fakeGetAPI struct {
	resp *larkeventv1.GetSubscriptionResp
	err  error
}

func (f *fakeGetAPI) Get(_ context.Context, _ *larkeventv1.GetSubscriptionReq) (*larkeventv1.GetSubscriptionResp, error) {
	return f.resp, f.err
}

func okGetResp(d *larkeventv1.SubscriptionDetail) *larkeventv1.GetSubscriptionResp {
	return &larkeventv1.GetSubscriptionResp{
		ApiResp: &larkcore.ApiResp{RawBody: []byte(`{"code":0}`)},
		Data:    &larkeventv1.GetSubscriptionRespData{Subscription: d},
	}
}

// TestGetSubscription_SingleDetail_JSONShape is the primary TDD case from
// the task brief: `get sub_xxx --json` maps one SubscriptionDetail into the
// shared subscriptionRow shape.
func TestGetSubscription_SingleDetail_JSONShape(t *testing.T) {
	fake := &fakeGetAPI{resp: okGetResp(&larkeventv1.SubscriptionDetail{
		SubscriptionId: strPtr("sub_xxx"),
		EventType:      strPtr("im.message.created_v1"),
		TargetResource: strPtr("im.message?chat_id=oc_xxx"),
		Authority:      &larkeventv1.Authority{Type: strPtr("user"), OpenId: strPtr("ou_xxx")},
		State:          strPtr("active"),
		ExpireTime:     intPtr(1732000000),
		PayloadOptions: &larkeventv1.PayloadOptions{IncludeResourceData: boolPtr(false)},
	})}

	row, err := getSubscription(context.Background(), fake, "sub_xxx")
	if err != nil {
		t.Fatalf("getSubscription: unexpected error: %v", err)
	}
	if row.RemoteSubscriptionID != "sub_xxx" {
		t.Errorf("RemoteSubscriptionID = %q, want sub_xxx", row.RemoteSubscriptionID)
	}
	if row.EventKey != "im.message.created_v1" {
		t.Errorf("EventKey = %q, want im.message.created_v1", row.EventKey)
	}
	if row.Identity != "user:ou_xxx" {
		t.Errorf("Identity = %q, want user:ou_xxx", row.Identity)
	}
	if row.Remote.State != "active" {
		t.Errorf("Remote.State = %q, want active", row.Remote.State)
	}

	raw, err := json.Marshal(row)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var generic map[string]interface{}
	if err := json.Unmarshal(raw, &generic); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	for _, field := range []string{"remote_subscription_id", "event_key", "event_type", "target_resource", "identity", "payload_options", "remote"} {
		if _, ok := generic[field]; !ok {
			t.Errorf("get --json missing field %q; got: %s", field, raw)
		}
	}
	if _, present := generic["local"]; present {
		t.Errorf(`get --json "local" present (%v), want omitted in Phase B`, generic["local"])
	}
}

// TestGetSubscription_ClientError_PropagatesUnchanged locks §3.6: whatever
// typed error *eventlib.SubscriptionClient.Get already produced — for a
// transport failure or for an OAPI business failure such as an unknown/
// nonexistent remote_subscription_id (both already classified and tested in
// Task 7, see subscription_client_test.go) — must reach the caller
// unchanged. This command layer must not swallow, downgrade, or re-wrap it.
func TestGetSubscription_ClientError_PropagatesUnchanged(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"raw transport error", errors.New("boom: connection reset")},
		{"typed API error (e.g. unknown remote_subscription_id)", errs.NewAPIError(errs.SubtypeUnknown, "subscription not found").WithCode(600901)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeGetAPI{err: tc.err}
			_, err := getSubscription(context.Background(), fake, "sub_does_not_exist")
			if !errors.Is(err, tc.err) && err != tc.err {
				t.Errorf("err = %v, want the client's error passed through unchanged (%v)", err, tc.err)
			}
		})
	}
}

// TestGetSubscription_SuccessWithNoData_ReturnsTypedInternalError is a
// defensive edge case: a syntactically successful response (code==0) that
// nonetheless carries no Subscription payload must not silently render as
// an all-empty row — it is a wire anomaly, not "found an empty
// subscription".
func TestGetSubscription_SuccessWithNoData_ReturnsTypedInternalError(t *testing.T) {
	fake := &fakeGetAPI{resp: okGetResp(nil)}

	_, err := getSubscription(context.Background(), fake, "sub_xxx")
	if err == nil {
		t.Fatal("expected an error when the response carries no subscription data")
	}
	if _, ok := errs.ProblemOf(err); !ok {
		t.Fatalf("expected a typed errs.* error, got %T: %v", err, err)
	}
}

// TestRunGet_EmptyID_RejectedBeforeNetwork locks that an empty
// remote_subscription_id (cobra.ExactArgs(1) still allows "") is rejected
// client-side with a typed invalid_argument before any identity/scope/
// network work.
func TestRunGet_EmptyID_RejectedBeforeNetwork(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdGet(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{""})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected an error for an empty remote_subscription_id, got nil")
	}
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeInvalidArgument {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeInvalidArgument)
	}
	if ve.Param != "remote_subscription_id" {
		t.Errorf("Param = %q, want %q", ve.Param, "remote_subscription_id")
	}
}

// TestRunGet_MissingReadScope_ReturnsPermissionError mirrors
// TestRunList_MissingReadScope_ReturnsPermissionError for `get`.
func TestRunGet_MissingReadScope_ReturnsPermissionError(t *testing.T) {
	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "cli_x"})
	f.Credential = credential.NewCredentialProvider(nil, nil, &fakeTokenResolver{
		result: &credential.TokenResult{Token: "u-tok", Scopes: "im:message:send"},
	}, nil)

	cmd := NewCmdGet(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"sub_xxx", "--as", "user"})

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

func TestNewCmdGet_RequiresExactlyOneArg(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdGet(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{})

	if err := cmd.Execute(); err == nil {
		t.Fatal("expected an error with zero args, got nil")
	}
}
