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

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/credential"
	"github.com/larksuite/cli/internal/event/model"
)

// fakeRenewAPI is a network-free stand-in for the platform/lark gateway's
// Get+Renew — the renewSubscriptionAPI test seam. The gateway hands back a
// domain RemoteSubscription (already unwrapped, validated, classified).
type fakeRenewAPI struct {
	getSub *model.RemoteSubscription
	getErr error

	renewFunc  func() (*model.RemoteSubscription, error)
	renewCalls int
}

func (f *fakeRenewAPI) Get(_ context.Context, _ string) (*model.RemoteSubscription, error) {
	return f.getSub, f.getErr
}

func (f *fakeRenewAPI) Renew(_ context.Context, _ string) (*model.RemoteSubscription, error) {
	f.renewCalls++
	if f.renewFunc == nil {
		return subPtr(activeSub("sub_1", false, "user")), nil
	}
	return f.renewFunc()
}

// ---- doRenewSubscription ----

func TestDoRenewSubscription_CallsRenewAndReturnsDetail(t *testing.T) {
	fake := &fakeRenewAPI{renewFunc: func() (*model.RemoteSubscription, error) {
		return subPtr(activeSub("sub_1", false, "user")), nil
	}}

	sub, err := doRenewSubscription(context.Background(), fake, "sub_1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.renewCalls != 1 {
		t.Errorf("renewCalls = %d, want 1", fake.renewCalls)
	}
	if sub.ID.String() != "sub_1" {
		t.Errorf("sub.ID = %q, want sub_1", sub.ID)
	}
}

func TestDoRenewSubscription_TransportError_PropagatesUnchanged(t *testing.T) {
	sentinel := errors.New("boom: connection reset")
	fake := &fakeRenewAPI{renewFunc: func() (*model.RemoteSubscription, error) { return nil, sentinel }}

	_, err := doRenewSubscription(context.Background(), fake, "sub_1")
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want it passed through unchanged (%v)", err, sentinel)
	}
}

// The "success response with no subscription data -> typed InvalidResponse" edge
// case now lives at the gateway (platform/lark's
// TestGateway_Renew_NilData_ReturnsInvalidResponse); doRenewSubscription only
// forwards the RemoteSubscription the gateway already validated.

// ---- --dry-run output shape, direct-call end-to-end via the
// fake service — mirrors update_test.go's own dry-run test.

func TestRenewDryRun_EndToEndViaFakeService_JSONShapeAndNoRenewCall(t *testing.T) {
	fake := &fakeRenewAPI{getSub: subPtr(activeSub("sub_1", false, "user"))}

	before, err := getSubscription(context.Background(), fake, "sub_1")
	if err != nil {
		t.Fatalf("getSubscription: unexpected error: %v", err)
	}
	result := buildMutationDryRunResult("renew", "sub_1", core.AsUser, true, before, "renew", false, renewLocalImpactNote,
		"run without --dry-run to renew remote_subscription_id=sub_1")

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
	if generic["operation"] != "renew" {
		t.Errorf(`"operation" = %v, want "renew"`, generic["operation"])
	}
	if fake.renewCalls != 0 {
		t.Errorf("renewCalls = %d, want 0: --dry-run must never issue a write", fake.renewCalls)
	}
}

// ---- runRenew wiring (cobra-level; every case below must short-circuit
// before any network-capable client is built) ----

func TestRunRenew_EmptyID_RejectedBeforeNetwork(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdRenew(f)
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

func TestRunRenew_MissingReadScope_ReturnsPermissionError(t *testing.T) {
	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "cli_x"})
	f.Credential = credential.NewCredentialProvider(nil, nil, &fakeTokenResolver{
		result: &credential.TokenResult{Token: "u-tok", Scopes: "event:subscription:write"},
	}, nil)

	cmd := NewCmdRenew(f)
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

func TestRunRenew_MissingWriteScope_ReturnsPermissionError(t *testing.T) {
	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "cli_x"})
	f.Credential = credential.NewCredentialProvider(nil, nil, &fakeTokenResolver{
		result: &credential.TokenResult{Token: "u-tok", Scopes: "event:subscription:read"},
	}, nil)

	cmd := NewCmdRenew(f)
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

// TestNewCmdRenew_HasExpectedFlagsAndNoYes locks that renew must NOT
// expose --yes and must not carry an --include-resource-data flag (that is
// update-only).
func TestNewCmdRenew_HasExpectedFlagsAndNoYes(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdRenew(f)
	for _, name := range []string{"dry-run", "json", "as"} {
		if cmd.Flags().Lookup(name) == nil {
			t.Errorf("NewCmdRenew missing --%s flag", name)
		}
	}
	if cmd.Flags().Lookup("yes") != nil {
		t.Error("NewCmdRenew must not expose --yes (renew only extends TTL, not a high-risk confirmation-gated action)")
	}
	if cmd.Flags().Lookup("include-resource-data") != nil {
		t.Error("NewCmdRenew must not expose --include-resource-data (update-only)")
	}
	if level, ok := cmdutil.GetRisk(cmd); !ok || level != cmdutil.RiskWrite {
		t.Errorf("risk = (%q, %v), want (%q, true)", level, ok, cmdutil.RiskWrite)
	}
}

func TestNewCmdSubscription_RegistersRenewAsWrite(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdSubscription(f)

	var renew *cobra.Command
	for _, c := range cmd.Commands() {
		if c.Name() == "renew" {
			renew = c
		}
	}
	if renew == nil {
		t.Fatal(`subscription command group missing "renew" subcommand`)
	}
	level, ok := cmdutil.GetRisk(renew)
	if !ok || level != cmdutil.RiskWrite {
		t.Errorf(`"renew" risk = (%q, %v), want (%q, true)`, level, ok, cmdutil.RiskWrite)
	}
}
