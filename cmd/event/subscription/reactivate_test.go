// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"bytes"
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
)

// fakeReactivateAPI is a network-free stand-in for the platform/lark gateway's
// Get+Reactivate — the reactivateSubscriptionAPI test seam.
type fakeReactivateAPI struct {
	getSub *model.RemoteSubscription
	getErr error

	reactivateFunc  func() (*model.RemoteSubscription, error)
	reactivateCalls int
}

func (f *fakeReactivateAPI) Get(_ context.Context, _ string) (*model.RemoteSubscription, error) {
	return f.getSub, f.getErr
}

func (f *fakeReactivateAPI) Reactivate(_ context.Context, _ string) (*model.RemoteSubscription, error) {
	f.reactivateCalls++
	if f.reactivateFunc == nil {
		return subPtr(activeSub("sub_1", false, "user")), nil
	}
	return f.reactivateFunc()
}

// ---- doReactivateSubscription ----

func TestDoReactivateSubscription_CallsReactivateAndReturnsDetail(t *testing.T) {
	fake := &fakeReactivateAPI{reactivateFunc: func() (*model.RemoteSubscription, error) {
		return subPtr(activeSub("sub_1", false, "user")), nil
	}}

	sub, err := doReactivateSubscription(context.Background(), fake, "sub_1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.reactivateCalls != 1 {
		t.Errorf("reactivateCalls = %d, want 1", fake.reactivateCalls)
	}
	if sub.State != "active" {
		t.Errorf("sub.State = %q, want active", sub.State)
	}
}

func TestDoReactivateSubscription_TransportError_PropagatesUnchanged(t *testing.T) {
	sentinel := errors.New("boom: connection reset")
	fake := &fakeReactivateAPI{reactivateFunc: func() (*model.RemoteSubscription, error) { return nil, sentinel }}

	_, err := doReactivateSubscription(context.Background(), fake, "sub_1")
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want it passed through unchanged (%v)", err, sentinel)
	}
}

// The "success response with no subscription data -> typed InvalidResponse" edge
// case now lives at the gateway (platform/lark's
// TestGateway_Reactivate_NilData_ReturnsInvalidResponse).

// ---- --dry-run output shape, direct-call end-to-end via the
// fake service — mirrors update_test.go's own dry-run test. Uses a
// suspended fixture (the realistic target for reactivate) to prove
// dry-run reports it informationally and never calls Reactivate.

func TestReactivateDryRun_EndToEndViaFakeService_JSONShapeAndNoReactivateCall(t *testing.T) {
	fake := &fakeReactivateAPI{getSub: subPtr(suspendedSub("sub_1", "authority_revoked"))}

	before, err := getSubscription(context.Background(), fake, "sub_1")
	if err != nil {
		t.Fatalf("getSubscription: unexpected error: %v", err)
	}
	result := buildMutationDryRunResult("reactivate", "sub_1", core.AsUser, true, before, "reactivate", false, reactivateLocalImpactNote,
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

// ---- applyReactivate: real-run no-op parity with the dry-run ----

// TestApplyReactivate_AlreadyActive_RealRun_NoReactivateCall locks the fix: a
// REAL reactivate (not --dry-run) of an already-active subscription is a no-op —
// it must NOT call Reactivate, mirroring the --dry-run's planned_action=noop via
// the shared reactivateIsNoop predicate. Before the fix the real path called
// svc.Reactivate unconditionally, so the preview and the execution disagreed.
func TestApplyReactivate_AlreadyActive_RealRun_NoReactivateCall(t *testing.T) {
	fake := &fakeReactivateAPI{getSub: subPtr(activeSub("sub_1", false, "user"))}
	before, err := getSubscription(context.Background(), fake, "sub_1")
	if err != nil {
		t.Fatalf("getSubscription: %v", err)
	}

	var buf bytes.Buffer
	if err := applyReactivate(context.Background(), fake, &buf, "sub_1", core.AsUser, reactivateOpts{}, before, true); err != nil {
		t.Fatalf("applyReactivate: unexpected error: %v", err)
	}
	if fake.reactivateCalls != 0 {
		t.Errorf("reactivateCalls = %d, want 0: an already-active real reactivate must be a no-op (matching the dry-run)", fake.reactivateCalls)
	}
	if !strings.Contains(buf.String(), "already active") {
		t.Errorf("output = %q, want it to state the subscription is already active", buf.String())
	}
}

// TestApplyReactivate_Suspended_RealRun_CallsReactivate locks the other side: a
// suspended subscription (the real reactivate target) IS reactivated exactly once.
func TestApplyReactivate_Suspended_RealRun_CallsReactivate(t *testing.T) {
	fake := &fakeReactivateAPI{getSub: subPtr(suspendedSub("sub_1", "authority_revoked"))}
	before, err := getSubscription(context.Background(), fake, "sub_1")
	if err != nil {
		t.Fatalf("getSubscription: %v", err)
	}

	if err := applyReactivate(context.Background(), fake, io.Discard, "sub_1", core.AsUser, reactivateOpts{}, before, true); err != nil {
		t.Fatalf("applyReactivate: unexpected error: %v", err)
	}
	if fake.reactivateCalls != 1 {
		t.Errorf("reactivateCalls = %d, want 1: a suspended subscription must be reactivated", fake.reactivateCalls)
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

// TestNewCmdReactivate_HasExpectedFlagsAndNoYes locks that reactivate
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
		t.Error("NewCmdReactivate must not expose --yes (reactivate is a recovery action, not a high-risk confirmation-gated write)")
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

// TestNewCmdSubscription_NoSuspendCommand locks that there
// is no `suspend` command — the server exposes no such operation, so the
// only path back from suspended is `reactivate`.
func TestNewCmdSubscription_NoSuspendCommand(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdSubscription(f)
	for _, c := range cmd.Commands() {
		if c.Name() == "suspend" {
			t.Fatal(`subscription command group must not have a "suspend" subcommand (no such server operation)`)
		}
	}
}
