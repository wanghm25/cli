// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/credential"
	"github.com/larksuite/cli/internal/event/buslocal"
	"github.com/larksuite/cli/internal/event/model"
	larkgw "github.com/larksuite/cli/internal/event/platform/lark"
)

// fakeGetAPI is a network-free stand-in for the platform/lark gateway's Get —
// the getSubscriptionAPI test seam. The gateway already unwraps + validates the
// response and classifies failures, so it hands back a domain RemoteSubscription
// or a typed error; a fixture simulating a failure sets `err`, and success sets
// `sub`.
type fakeGetAPI struct {
	sub *model.RemoteSubscription
	err error
}

func (f *fakeGetAPI) Get(_ context.Context, _ string) (*model.RemoteSubscription, error) {
	return f.sub, f.err
}

// TestGetSubscription_SingleDetail_JSONShape is the primary TDD case from
// the task brief: `get sub_xxx --json` maps one RemoteSubscription into the
// shared subscriptionRow shape.
func TestGetSubscription_SingleDetail_JSONShape(t *testing.T) {
	registerCreateFixtures(t) // so ReverseResolve reconstructs the executable event_key
	fake := &fakeGetAPI{sub: subPtr(larkgw.ProjectSubscription(&larkeventv1.SubscriptionDetail{
		SubscriptionId: strPtr("sub_xxx"),
		EventType:      strPtr("im.message.created_v1"),
		TargetResource: strPtr("im.message?chat_id=oc_xxx"),
		Authority:      &larkeventv1.Authority{Type: strPtr("user"), OpenId: strPtr("ou_xxx")},
		State:          strPtr("active"),
		ExpireTime:     intPtr(1732000000),
		PayloadOptions: &larkeventv1.PayloadOptions{IncludeResourceData: boolPtr(false)},
	}))}

	row, err := getSubscription(context.Background(), fake, "sub_xxx")
	if err != nil {
		t.Fatalf("getSubscription: unexpected error: %v", err)
	}
	if row.RemoteSubscriptionID != "sub_xxx" {
		t.Errorf("RemoteSubscriptionID = %q, want sub_xxx", row.RemoteSubscriptionID)
	}
	// event_key is the reversed, executable materialized key, not the raw event_type.
	if row.EventKey != "im.message.created_v1/chat-id/oc_xxx" {
		t.Errorf("EventKey = %q, want im.message.created_v1/chat-id/oc_xxx", row.EventKey)
	}
	// identity is the `--as`-composable token; the open_id is preserved separately.
	if row.Identity != "user" {
		t.Errorf("Identity = %q, want user", row.Identity)
	}
	if row.UserOpenID != "ou_xxx" {
		t.Errorf("UserOpenID = %q, want ou_xxx", row.UserOpenID)
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
		t.Errorf(`get --json "local" present (%v), want omitted`, generic["local"])
	}
}

// TestGetSubscriptionRow_SurfacesRunningLocalConsumer locks the get-side
// surfacing: a running consumer bound to this remote_subscription_id is added
// as the row's additive `local` object (running + the consumer).
func TestGetSubscriptionRow_SurfacesRunningLocalConsumer(t *testing.T) {
	registerCreateFixtures(t)
	fake := &fakeGetAPI{sub: subPtr(activeSub("sub_xxx", false, "user"))}
	consumers := []buslocal.Consumer{{AppID: "cli_x", PID: 99, EventKey: "im.message.created_v1/chat-id/oc_aaa", RemoteSubscriptionID: "sub_xxx"}}

	row, err := getSubscriptionRow(context.Background(), fake, "sub_xxx", consumers)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if row.Local == nil || !row.Local.Running {
		t.Fatalf("Local = %+v, want a running local consumer", row.Local)
	}
	if len(row.Local.Consumers) != 1 || row.Local.Consumers[0].PID != 99 {
		t.Errorf("Local.Consumers = %+v, want the pid=99 consumer", row.Local.Consumers)
	}

	raw, err := json.Marshal(row)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var generic map[string]interface{}
	if err := json.Unmarshal(raw, &generic); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if _, present := generic["local"]; !present {
		t.Errorf(`get --json "local" absent, want present when a consumer is bound; got %s`, raw)
	}
}

// TestGetSubscriptionRow_NoBus_OmitsLocal locks best-effort: with no local
// consumer known (bus down / none bound), get still succeeds and `local` is
// omitted — exactly the pre-change shape.
func TestGetSubscriptionRow_NoBus_OmitsLocal(t *testing.T) {
	registerCreateFixtures(t)
	fake := &fakeGetAPI{sub: subPtr(activeSub("sub_xxx", false, "user"))}

	row, err := getSubscriptionRow(context.Background(), fake, "sub_xxx", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if row.Local != nil {
		t.Errorf("Local = %+v, want nil/omitted when no consumer is known", row.Local)
	}
}

// TestGetSubscription_ClientError_PropagatesUnchanged locks that whatever
// typed error *eventlib.SubscriptionClient.Get already produced — for a
// transport failure or for an OAPI business failure such as an unknown/
// nonexistent remote_subscription_id (both already classified and tested in
// subscription_client_test.go) — must reach the caller
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

// The "success response carrying no subscription payload / an empty
// remote_subscription_id -> typed InvalidResponse" edge case now lives at the
// gateway (platform/lark's TestGateway_Get_NilData/EmptyID_ReturnsInvalidResponse),
// since that validation moved there; getSubscription only maps the already-validated
// RemoteSubscription the gateway guarantees.

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
