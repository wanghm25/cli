// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/credential"
	"github.com/larksuite/cli/internal/event/model"
	"github.com/larksuite/cli/internal/output"
)

// fakeDeleteAPI is a network-free stand-in for the platform/lark gateway's
// Get+Delete — the deleteSubscriptionAPI test seam. Delete carries no payload,
// so the gateway (and this fake) returns only an error.
type fakeDeleteAPI struct {
	getSub *model.RemoteSubscription
	getErr error

	deleteFunc  func() error
	deleteCalls int
}

func (f *fakeDeleteAPI) Get(_ context.Context, _ string) (*model.RemoteSubscription, error) {
	return f.getSub, f.getErr
}

func (f *fakeDeleteAPI) Delete(_ context.Context, _ string) error {
	f.deleteCalls++
	if f.deleteFunc == nil {
		return nil
	}
	return f.deleteFunc()
}

// ---- applyDelete (confirmation gate) ----

func TestApplyDelete_NotYes_ReturnsConfirmationRequired_NoDeleteCall(t *testing.T) {
	fake := &fakeDeleteAPI{}
	before := rowFromDetail(t, activeDetail("sub_1", false, "user"))

	err := applyDelete(context.Background(), fake, "sub_1", core.AsUser, before, false)

	var ce *errs.ConfirmationRequiredError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *errs.ConfirmationRequiredError, got %T: %v", err, err)
	}
	if ce.Category != errs.CategoryConfirmation {
		t.Errorf("Category = %q, want %q", ce.Category, errs.CategoryConfirmation)
	}
	if ce.Risk != errs.RiskHighRiskWrite {
		t.Errorf("Risk = %q, want %q", ce.Risk, errs.RiskHighRiskWrite)
	}
	if ce.Action != "event subscription delete" {
		t.Errorf("Action = %q, want %q", ce.Action, "event subscription delete")
	}
	if got := output.ExitCodeOf(err); got != output.ExitConfirmationRequired {
		t.Errorf("ExitCodeOf = %d, want %d", got, output.ExitConfirmationRequired)
	}
	if !strings.Contains(ce.Hint, "sub_1") {
		t.Errorf("Hint = %q, want it to mention remote_subscription_id sub_1", ce.Hint)
	}
	if !strings.Contains(ce.Hint, "active") {
		t.Errorf("Hint = %q, want it to mention current state active", ce.Hint)
	}
	// Task brief's required phrasing: delete's confirmation must say it is
	// not a substitute for stopping local consumers.
	if !strings.Contains(ce.Hint, "not a substitute for stopping local consumers") {
		t.Errorf("Hint = %q, want it to say delete is not a substitute for stopping local consumers", ce.Hint)
	}
	if fake.deleteCalls != 0 {
		t.Errorf("deleteCalls = %d, want 0: confirmation-required must never call Delete", fake.deleteCalls)
	}
}

func TestApplyDelete_Yes_CallsDelete(t *testing.T) {
	fake := &fakeDeleteAPI{}
	before := rowFromDetail(t, activeDetail("sub_1", false, "user"))

	err := applyDelete(context.Background(), fake, "sub_1", core.AsUser, before, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.deleteCalls != 1 {
		t.Errorf("deleteCalls = %d, want 1", fake.deleteCalls)
	}
}

// ---- doDeleteSubscription ----

func TestDoDeleteSubscription_TransportError_PropagatesUnchanged(t *testing.T) {
	sentinel := errors.New("boom: connection reset")
	fake := &fakeDeleteAPI{deleteFunc: func() error { return sentinel }}

	err := doDeleteSubscription(context.Background(), fake, "sub_1")
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want it passed through unchanged (%v)", err, sentinel)
	}
}

func TestDoDeleteSubscription_Success_NoError(t *testing.T) {
	fake := &fakeDeleteAPI{}
	if err := doDeleteSubscription(context.Background(), fake, "sub_1"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if fake.deleteCalls != 1 {
		t.Errorf("deleteCalls = %d, want 1", fake.deleteCalls)
	}
}

// ---- buildDeleteResult ----

func TestBuildDeleteResult_Shape(t *testing.T) {
	before := rowFromDetail(t, activeDetail("sub_1", false, "user"))
	result := buildDeleteResult("sub_1", before)

	if result.Operation != "delete" {
		t.Errorf("Operation = %q, want delete", result.Operation)
	}
	if !result.Deleted {
		t.Error("Deleted = false, want true")
	}
	if result.RemoteSubscriptionID != "sub_1" {
		t.Errorf("RemoteSubscriptionID = %q, want sub_1", result.RemoteSubscriptionID)
	}
	if result.Subscription.RemoteSubscriptionID != "sub_1" {
		t.Errorf("Subscription snapshot RemoteSubscriptionID = %q, want sub_1", result.Subscription.RemoteSubscriptionID)
	}
	if result.NextAction == "" {
		t.Error("NextAction must not be empty")
	}
}

// ---- --dry-run output shape, direct-call end-to-end via the
// fake service — mirrors update_test.go's own dry-run test.

func TestDeleteDryRun_EndToEndViaFakeService_JSONShapeAndNoDeleteCall(t *testing.T) {
	fake := &fakeDeleteAPI{getSub: subPtr(activeSub("sub_1", false, "user"))}

	before, err := getSubscription(context.Background(), fake, "sub_1")
	if err != nil {
		t.Fatalf("getSubscription: unexpected error: %v", err)
	}
	result := buildMutationDryRunResult("delete", "sub_1", core.AsUser, before, "delete", false, deleteLocalImpactNote,
		"run with --yes (after a human confirms) to permanently delete remote_subscription_id=sub_1")

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
	if generic["operation"] != "delete" {
		t.Errorf(`"operation" = %v, want "delete"`, generic["operation"])
	}
	localImpact, ok := generic["local_impact"].(map[string]interface{})
	if !ok || !strings.Contains(localImpact["note"].(string), "not a substitute for stopping local consumers") {
		t.Errorf(`"local_impact.note" = %v, want it to mention "not a substitute for stopping local consumers"`, generic["local_impact"])
	}
	if fake.deleteCalls != 0 {
		t.Errorf("deleteCalls = %d, want 0: --dry-run must never issue a write", fake.deleteCalls)
	}
}

// ---- runDelete wiring (cobra-level; every case below must short-circuit
// before any network-capable client is built) ----

func TestRunDelete_EmptyID_RejectedBeforeNetwork(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdDelete(f)
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

func TestRunDelete_MissingReadScope_ReturnsPermissionError(t *testing.T) {
	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "cli_x"})
	f.Credential = credential.NewCredentialProvider(nil, nil, &fakeTokenResolver{
		result: &credential.TokenResult{Token: "u-tok", Scopes: "event:subscription:write"},
	}, nil)

	cmd := NewCmdDelete(f)
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

func TestRunDelete_MissingWriteScope_ReturnsPermissionError(t *testing.T) {
	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "cli_x"})
	f.Credential = credential.NewCredentialProvider(nil, nil, &fakeTokenResolver{
		result: &credential.TokenResult{Token: "u-tok", Scopes: "event:subscription:read"},
	}, nil)

	cmd := NewCmdDelete(f)
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

func TestNewCmdDelete_HasExpectedFlags(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdDelete(f)
	for _, name := range []string{"dry-run", "yes", "json", "as"} {
		if cmd.Flags().Lookup(name) == nil {
			t.Errorf("NewCmdDelete missing --%s flag", name)
		}
	}
	if cmd.Flags().Lookup("include-resource-data") != nil {
		t.Error("NewCmdDelete must not expose --include-resource-data (create-only)")
	}
	// delete is confirmation-gated, so its static risk must be high-risk-write
	// (matching the ConfirmationRequiredError it returns without --yes).
	if level, ok := cmdutil.GetRisk(cmd); !ok || level != cmdutil.RiskHighRiskWrite {
		t.Errorf("risk = (%q, %v), want (%q, true)", level, ok, cmdutil.RiskHighRiskWrite)
	}
}

func TestNewCmdSubscription_RegistersDeleteAsHighRiskWrite(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdSubscription(f)

	var del *cobra.Command
	for _, c := range cmd.Commands() {
		if c.Name() == "delete" {
			del = c
		}
	}
	if del == nil {
		t.Fatal(`subscription command group missing "delete" subcommand`)
	}
	level, ok := cmdutil.GetRisk(del)
	if !ok || level != cmdutil.RiskHighRiskWrite {
		t.Errorf(`"delete" risk = (%q, %v), want (%q, true)`, level, ok, cmdutil.RiskHighRiskWrite)
	}
}
