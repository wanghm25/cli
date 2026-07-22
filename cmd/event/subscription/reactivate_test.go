// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/spf13/cobra"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/credential"
)

// fakeReactivateAPI is a network-free stand-in for
// *eventlib.SubscriptionClient's Get+Reactivate — the
// reactivateSubscriptionAPI test seam.
type fakeReactivateAPI struct {
	getResp *larkeventv1.GetSubscriptionResp
	getErr  error

	reactivateFunc  func() (*larkeventv1.ReactivateSubscriptionResp, error)
	reactivateCalls int
}

func (f *fakeReactivateAPI) Get(_ context.Context, _ *larkeventv1.GetSubscriptionReq) (*larkeventv1.GetSubscriptionResp, error) {
	return f.getResp, f.getErr
}

func (f *fakeReactivateAPI) Reactivate(_ context.Context, _ *larkeventv1.ReactivateSubscriptionReq) (*larkeventv1.ReactivateSubscriptionResp, error) {
	f.reactivateCalls++
	if f.reactivateFunc == nil {
		return okReactivateResp(activeDetail("sub_1", false, "user")), nil
	}
	return f.reactivateFunc()
}

func okReactivateResp(d *larkeventv1.SubscriptionDetail) *larkeventv1.ReactivateSubscriptionResp {
	return &larkeventv1.ReactivateSubscriptionResp{
		ApiResp: &larkcore.ApiResp{RawBody: []byte(`{"code":0}`)},
		Data:    &larkeventv1.ReactivateSubscriptionRespData{Subscription: d},
	}
}

// ---- doReactivateSubscription ----

func TestDoReactivateSubscription_CallsReactivateAndReturnsDetail(t *testing.T) {
	fake := &fakeReactivateAPI{reactivateFunc: func() (*larkeventv1.ReactivateSubscriptionResp, error) {
		return okReactivateResp(activeDetail("sub_1", false, "user")), nil
	}}

	detail, err := doReactivateSubscription(context.Background(), fake, "sub_1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.reactivateCalls != 1 {
		t.Errorf("reactivateCalls = %d, want 1", fake.reactivateCalls)
	}
	if strVal(detail.State) != "active" {
		t.Errorf("detail.State = %q, want active", strVal(detail.State))
	}
}

func TestDoReactivateSubscription_TransportError_PropagatesUnchanged(t *testing.T) {
	sentinel := errors.New("boom: connection reset")
	fake := &fakeReactivateAPI{reactivateFunc: func() (*larkeventv1.ReactivateSubscriptionResp, error) { return nil, sentinel }}

	_, err := doReactivateSubscription(context.Background(), fake, "sub_1")
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want it passed through unchanged (%v)", err, sentinel)
	}
}

func TestDoReactivateSubscription_SuccessWithNoData_ReturnsTypedInternalError(t *testing.T) {
	fake := &fakeReactivateAPI{reactivateFunc: func() (*larkeventv1.ReactivateSubscriptionResp, error) { return okReactivateResp(nil), nil }}

	_, err := doReactivateSubscription(context.Background(), fake, "sub_1")
	if _, ok := errs.ProblemOf(err); !ok {
		t.Fatalf("expected a typed errs.* error, got %T: %v", err, err)
	}
}

// ---- --dry-run output shape (spec §3.4), direct-call end-to-end via the
// fake service — mirrors update_test.go's own dry-run test. Uses a
// suspended fixture (the realistic target for reactivate) to prove
// dry-run reports it informationally and never calls Reactivate.

func TestReactivateDryRun_EndToEndViaFakeService_JSONShapeAndNoReactivateCall(t *testing.T) {
	fake := &fakeReactivateAPI{getResp: okGetResp(suspendedDetail("sub_1", "authority_revoked"))}

	before, err := getSubscription(context.Background(), fake, "sub_1")
	if err != nil {
		t.Fatalf("getSubscription: unexpected error: %v", err)
	}
	result := buildMutationDryRunResult("reactivate", "sub_1", core.AsUser, before, "reactivate", reactivateLocalImpactNote,
		"run without --dry-run to reactivate remote_subscription_id=sub_1")

	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var generic map[string]interface{}
	if err := json.Unmarshal(raw, &generic); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	for _, field := range []string{"operation", "dry_run", "remote_subscription_id", "required_scopes", "preflight", "remote_before", "planned_change", "local_impact", "next_action"} {
		if _, ok := generic[field]; !ok {
			t.Errorf("dry-run JSON missing field %q; got: %s", field, raw)
		}
	}
	if generic["operation"] != "reactivate" {
		t.Errorf(`"operation" = %v, want "reactivate"`, generic["operation"])
	}
	if result.RemoteBefore == nil || result.RemoteBefore.Remote.SuspensionReason != "authority_revoked" {
		t.Errorf("RemoteBefore = %+v, want the suspended subscription row", result.RemoteBefore)
	}
	if fake.reactivateCalls != 0 {
		t.Errorf("reactivateCalls = %d, want 0: --dry-run must never issue a write", fake.reactivateCalls)
	}
}

// ---- runReactivate wiring (cobra-level; every case below must
// short-circuit before any network-capable client is built) ----

func TestRunReactivate_EmptyID_RejectedBeforeNetwork(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdReactivate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{""})

	err := cmd.Execute()
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeInvalidArgument {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeInvalidArgument)
	}
	if ve.Param != "remote_subscription_id" {
		t.Errorf("Param = %q, want remote_subscription_id", ve.Param)
	}
}

func TestRunReactivate_MissingReadScope_ReturnsPermissionError(t *testing.T) {
	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "cli_x"})
	f.Credential = credential.NewCredentialProvider(nil, nil, &fakeTokenResolver{
		result: &credential.TokenResult{Token: "u-tok", Scopes: "event:subscription:write"},
	}, nil)

	cmd := NewCmdReactivate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"sub_1", "--as", "user"})

	err := cmd.Execute()
	var permErr *errs.PermissionError
	if !errors.As(err, &permErr) {
		t.Fatalf("expected *errs.PermissionError, got %T: %v", err, err)
	}
	if len(permErr.MissingScopes) != 1 || permErr.MissingScopes[0] != "event:subscription:read" {
		t.Errorf("MissingScopes = %v, want [event:subscription:read]", permErr.MissingScopes)
	}
}

func TestRunReactivate_MissingWriteScope_ReturnsPermissionError(t *testing.T) {
	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "cli_x"})
	f.Credential = credential.NewCredentialProvider(nil, nil, &fakeTokenResolver{
		result: &credential.TokenResult{Token: "u-tok", Scopes: "event:subscription:read"},
	}, nil)

	cmd := NewCmdReactivate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"sub_1", "--as", "user"})

	err := cmd.Execute()
	var permErr *errs.PermissionError
	if !errors.As(err, &permErr) {
		t.Fatalf("expected *errs.PermissionError, got %T: %v", err, err)
	}
	if len(permErr.MissingScopes) != 1 || permErr.MissingScopes[0] != "event:subscription:write" {
		t.Errorf("MissingScopes = %v, want [event:subscription:write]", permErr.MissingScopes)
	}
}

// TestNewCmdReactivate_HasExpectedFlagsAndNoYes locks spec §3.7: reactivate
// must NOT expose --yes.
func TestNewCmdReactivate_HasExpectedFlagsAndNoYes(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdReactivate(f)
	for _, name := range []string{"dry-run", "json", "as"} {
		if cmd.Flags().Lookup(name) == nil {
			t.Errorf("NewCmdReactivate missing --%s flag", name)
		}
	}
	if cmd.Flags().Lookup("yes") != nil {
		t.Error("NewCmdReactivate must not expose --yes (spec §3.7: reactivate is a recovery action, not a high-risk confirmation-gated write)")
	}
	if level, ok := cmdutil.GetRisk(cmd); !ok || level != cmdutil.RiskWrite {
		t.Errorf("risk = (%q, %v), want (%q, true)", level, ok, cmdutil.RiskWrite)
	}
}

func TestNewCmdSubscription_RegistersReactivateAsWrite(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdSubscription(f)

	var reactivate *cobra.Command
	for _, c := range cmd.Commands() {
		if c.Name() == "reactivate" {
			reactivate = c
		}
	}
	if reactivate == nil {
		t.Fatal(`subscription command group missing "reactivate" subcommand`)
	}
	level, ok := cmdutil.GetRisk(reactivate)
	if !ok || level != cmdutil.RiskWrite {
		t.Errorf(`"reactivate" risk = (%q, %v), want (%q, true)`, level, ok, cmdutil.RiskWrite)
	}
}

// TestNewCmdSubscription_NoSuspendCommand locks spec §3.7's last line: there
// is no `suspend` command — the server exposes no such operation, so the
// only path back from suspended is `reactivate`.
func TestNewCmdSubscription_NoSuspendCommand(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdSubscription(f)
	for _, c := range cmd.Commands() {
		if c.Name() == "suspend" {
			t.Fatal(`subscription command group must not have a "suspend" subcommand (spec §3.7: no such server operation)`)
		}
	}
}
