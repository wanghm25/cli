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

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/credential"
	"github.com/larksuite/cli/internal/output"
)

// fakeUpdateAPI is a network-free stand-in for *eventlib.SubscriptionClient's
// Get+Patch — the updateSubscriptionAPI test seam. See listSubscriptionsAPI
// (list.go) for the rationale.
type fakeUpdateAPI struct {
	getResp *larkeventv1.GetSubscriptionResp
	getErr  error

	patchFunc  func() (*larkeventv1.PatchSubscriptionResp, error)
	patchCalls int
}

func (f *fakeUpdateAPI) Get(_ context.Context, _ *larkeventv1.GetSubscriptionReq) (*larkeventv1.GetSubscriptionResp, error) {
	return f.getResp, f.getErr
}

func (f *fakeUpdateAPI) Patch(_ context.Context, _ *larkeventv1.PatchSubscriptionReq) (*larkeventv1.PatchSubscriptionResp, error) {
	f.patchCalls++
	if f.patchFunc == nil {
		return okPatchResp(activeDetail("sub_1", false, "user")), nil
	}
	return f.patchFunc()
}

func okPatchResp(d *larkeventv1.SubscriptionDetail) *larkeventv1.PatchSubscriptionResp {
	return &larkeventv1.PatchSubscriptionResp{
		ApiResp: &larkcore.ApiResp{RawBody: []byte(`{"code":0}`)},
		Data:    &larkeventv1.PatchSubscriptionRespData{Subscription: d},
	}
}

// rowFromDetail maps d through the exact same getSubscription seam
// runUpdate/runRenew/runReactivate/runDelete use, so tests build a
// *subscriptionRow fixture identically to how production code obtains
// "before" rather than hand-constructing one that could drift from the
// real mapping.
func rowFromDetail(t *testing.T, d *larkeventv1.SubscriptionDetail) *subscriptionRow {
	t.Helper()
	fake := &fakeGetAPI{resp: okGetResp(d)}
	row, err := getSubscription(context.Background(), fake, strVal(d.SubscriptionId))
	if err != nil {
		t.Fatalf("rowFromDetail: unexpected error: %v", err)
	}
	return row
}

// ---- applyUpdate (decision sequence) ----

func TestApplyUpdate_ActiveNotYes_ReturnsConfirmationRequired_NoPatchCall(t *testing.T) {
	fake := &fakeUpdateAPI{}
	before := rowFromDetail(t, activeDetail("sub_1", false, "user"))

	_, err := applyUpdate(context.Background(), fake, "sub_1", core.AsUser, before, false, false)

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
	if ce.Action != "event subscription update" {
		t.Errorf("Action = %q, want %q", ce.Action, "event subscription update")
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
	if fake.patchCalls != 0 {
		t.Errorf("patchCalls = %d, want 0: confirmation-required must never call Patch", fake.patchCalls)
	}
}

func TestApplyUpdate_ActiveYes_CallsPatchAndReturnsDetail(t *testing.T) {
	fake := &fakeUpdateAPI{patchFunc: func() (*larkeventv1.PatchSubscriptionResp, error) {
		return okPatchResp(activeDetail("sub_1", true, "user")), nil
	}}
	before := rowFromDetail(t, activeDetail("sub_1", false, "user"))

	detail, err := applyUpdate(context.Background(), fake, "sub_1", core.AsUser, before, true, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.patchCalls != 1 {
		t.Errorf("patchCalls = %d, want 1", fake.patchCalls)
	}
	if !boolVal(detail.PayloadOptions.IncludeResourceData) {
		t.Errorf("returned detail IncludeResourceData = %v, want true (the fake's own response)", detail.PayloadOptions.IncludeResourceData)
	}
}

func TestApplyUpdate_Suspended_ReturnsFailedPrecondition_EvenWithYes_NoPatchCall(t *testing.T) {
	fake := &fakeUpdateAPI{}
	before := rowFromDetail(t, suspendedDetail("sub_1", "authority_revoked"))

	// yes=true: the suspended guard must block regardless of confirmation —
	// there is no point confirming a write that cannot proceed.
	_, err := applyUpdate(context.Background(), fake, "sub_1", core.AsUser, before, false, true)

	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	if ve.Param != "remote_subscription_id" {
		t.Errorf("Param = %q, want remote_subscription_id", ve.Param)
	}
	if !strings.Contains(ve.Hint, "reactivate") {
		t.Errorf("Hint = %q, want it to guide the caller to `reactivate`", ve.Hint)
	}
	if !strings.Contains(ve.Hint, "sub_1") {
		t.Errorf("Hint = %q, want it to mention remote_subscription_id sub_1", ve.Hint)
	}
	if !strings.Contains(ve.Error(), "authority_revoked") {
		t.Errorf("Error() = %q, want it to surface suspension_reason=authority_revoked", ve.Error())
	}
	if fake.patchCalls != 0 {
		t.Errorf("patchCalls = %d, want 0: a suspended target must never be Patched", fake.patchCalls)
	}
}

func TestApplyUpdate_SuspendedNotYes_ReturnsFailedPrecondition_NotConfirmationRequired(t *testing.T) {
	// The suspended guard must win over the confirmation gate even when
	// --yes is ALSO absent — proving the ordering is suspended-check first,
	// not "whichever fires first wins by coincidence".
	fake := &fakeUpdateAPI{}
	before := rowFromDetail(t, suspendedDetail("sub_1", "authority_revoked"))

	_, err := applyUpdate(context.Background(), fake, "sub_1", core.AsUser, before, false, false)

	var ce *errs.ConfirmationRequiredError
	if errors.As(err, &ce) {
		t.Fatalf("got *errs.ConfirmationRequiredError, want the suspended failed_precondition to win: %v", err)
	}
	var ve *errs.ValidationError
	if !errors.As(err, &ve) || ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Fatalf("expected failed_precondition, got %T: %v", err, err)
	}
}

// ---- doPatchSubscription ----

func TestDoPatchSubscription_TransportError_PropagatesUnchanged(t *testing.T) {
	sentinel := errors.New("boom: connection reset")
	fake := &fakeUpdateAPI{patchFunc: func() (*larkeventv1.PatchSubscriptionResp, error) { return nil, sentinel }}

	_, err := doPatchSubscription(context.Background(), fake, "sub_1", false)
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want it passed through unchanged (%v)", err, sentinel)
	}
}

func TestDoPatchSubscription_SuccessWithNoData_ReturnsTypedInternalError(t *testing.T) {
	fake := &fakeUpdateAPI{patchFunc: func() (*larkeventv1.PatchSubscriptionResp, error) { return okPatchResp(nil), nil }}

	_, err := doPatchSubscription(context.Background(), fake, "sub_1", false)
	if _, ok := errs.ProblemOf(err); !ok {
		t.Fatalf("expected a typed errs.* error, got %T: %v", err, err)
	}
}

// ---- updatePlannedAction ----

func TestUpdatePlannedAction(t *testing.T) {
	if got := updatePlannedAction(rowFromDetail(t, activeDetail("sub_1", false, "user"))); got != "update" {
		t.Errorf("active: updatePlannedAction = %q, want update", got)
	}
	if got := updatePlannedAction(rowFromDetail(t, suspendedDetail("sub_1", "authority_revoked"))); got != "blocked_suspended" {
		t.Errorf("suspended: updatePlannedAction = %q, want blocked_suspended", got)
	}
}

// ---- --dry-run output shape, direct-call end-to-end via the
// fake service — mirrors create_test.go's
// TestDryRun_NotFound_EndToEndViaFakeService_JSONShapeAndNoCreateCall: this
// drives the exact same two calls runUpdate's --dry-run branch makes
// (getSubscription then buildMutationDryRunResult) against the fakeUpdateAPI
// seam, never through cobra Execute + a real network-capable client.

func TestUpdateDryRun_Active_EndToEndViaFakeService_JSONShapeAndNoPatchCall(t *testing.T) {
	fake := &fakeUpdateAPI{getResp: okGetResp(activeDetail("sub_1", false, "user"))}

	before, err := getSubscription(context.Background(), fake, "sub_1")
	if err != nil {
		t.Fatalf("getSubscription: unexpected error: %v", err)
	}
	result := buildMutationDryRunResult("update", "sub_1", core.AsUser, before,
		updatePlannedAction(before), updateLocalImpactNote, updateDryRunNextAction("sub_1", core.AsUser, before))

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
	if dryRun, _ := generic["dry_run"].(bool); !dryRun {
		t.Errorf(`"dry_run" = %v, want true`, generic["dry_run"])
	}
	if generic["remote_before"] == nil {
		t.Error(`"remote_before" = nil, want the fetched subscription row (update always targets an existing id)`)
	}
	plannedChange, ok := generic["planned_change"].(map[string]interface{})
	if !ok || plannedChange["action"] != "update" {
		t.Errorf(`"planned_change.action" = %v, want "update"`, generic["planned_change"])
	}
	if fake.patchCalls != 0 {
		t.Errorf("patchCalls = %d, want 0: --dry-run must never issue a write", fake.patchCalls)
	}
}

func TestUpdateDryRun_Suspended_ReportsBlockedInformationally_NoPatchCall(t *testing.T) {
	// Mirroring create.go's own conflict/suspended dry-run
	// handling, dry-run always reports the plan informationally rather
	// than erroring on remote business state. The suspended guard's typed
	// failed_precondition (TestApplyUpdate_Suspended_...) only fires on a
	// REAL run, never under --dry-run.
	fake := &fakeUpdateAPI{getResp: okGetResp(suspendedDetail("sub_1", "authority_revoked"))}

	before, err := getSubscription(context.Background(), fake, "sub_1")
	if err != nil {
		t.Fatalf("getSubscription: unexpected error: %v", err)
	}
	result := buildMutationDryRunResult("update", "sub_1", core.AsUser, before,
		updatePlannedAction(before), updateLocalImpactNote, updateDryRunNextAction("sub_1", core.AsUser, before))

	if result.PlannedChange.Action != "blocked_suspended" {
		t.Errorf("PlannedChange.Action = %q, want blocked_suspended", result.PlannedChange.Action)
	}
	if result.RemoteBefore == nil || result.RemoteBefore.Remote.SuspensionReason != "authority_revoked" {
		t.Errorf("RemoteBefore = %+v, want the suspended subscription row", result.RemoteBefore)
	}
	if !strings.Contains(result.NextAction, "reactivate") {
		t.Errorf("NextAction = %q, want it to guide the caller to `reactivate`", result.NextAction)
	}
	if fake.patchCalls != 0 {
		t.Errorf("patchCalls = %d, want 0: --dry-run must never issue a write, even for a plan a real run would reject", fake.patchCalls)
	}
}

// ---- runUpdate wiring (cobra-level; every case below must short-circuit
// before any network-capable client is built) ----

func TestRunUpdate_EmptyID_RejectedBeforeNetwork(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdUpdate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"", "--include-resource-data=false"})

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

func TestRunUpdate_MissingIncludeResourceDataFlag_ReturnsInvalidArgument(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdUpdate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"sub_1"}) // --include-resource-data never passed

	err := cmd.Execute()
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeInvalidArgument {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeInvalidArgument)
	}
	if ve.Param != "--include-resource-data" {
		t.Errorf("Param = %q, want --include-resource-data", ve.Param)
	}
}

// TestRunUpdate_IncludeResourceDataTrue_RejectsSwitchWithDeleteRecreateGuidance
// locks that switching include_resource_data / encryption ON via update is
// refused (encryption is Create-only). This replaces the old
// deferral gate (reason resource_data_encryption_deferred) — now that the
// encryption module ships, the rejection is permanent-by-design, not a
// temporary defer, and guides the caller to delete + recreate (human confirm)
// rather than to "retry later". Like the gate it replaced, it is a pure local
// check that fires before any identity/scope/remote work, so a rejected
// request never touches the network (f carries no config here).
func TestRunUpdate_IncludeResourceDataTrue_RejectsSwitchWithDeleteRecreateGuidance(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdUpdate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"sub_1", "--include-resource-data=true"})

	err := cmd.Execute()
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	if ve.Param != "--include-resource-data" {
		t.Errorf("Param = %q, want --include-resource-data", ve.Param)
	}
	// New guidance: cannot change include_resource_data / encryption in place.
	msg := ve.Error()
	for _, want := range []string{"include_resource_data", "encryption"} {
		if !strings.Contains(msg, want) {
			t.Errorf("Error() = %q, want it to mention %q", msg, want)
		}
	}
	// delete + recreate, after a human confirms.
	for _, want := range []string{"delete", "create", "confirm"} {
		if !strings.Contains(ve.Hint, want) {
			t.Errorf("Hint = %q, want it to guide the caller to %q", ve.Hint, want)
		}
	}
	if !strings.Contains(ve.Hint, "sub_1") {
		t.Errorf("Hint = %q, want it to name remote_subscription_id sub_1", ve.Hint)
	}
	// The old deferral reason must be gone: this is no longer a "not yet
	// supported / retry later" defer.
	if strings.Contains(msg, "resource_data_encryption_deferred") || strings.Contains(ve.Hint, "resource_data_encryption_deferred") {
		t.Errorf("error must no longer mention resource_data_encryption_deferred (deferral gate retired); got msg=%q hint=%q", msg, ve.Hint)
	}
}

func TestRunUpdate_MissingReadScope_ReturnsPermissionError(t *testing.T) {
	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "cli_x"})
	f.Credential = credential.NewCredentialProvider(nil, nil, &fakeTokenResolver{
		result: &credential.TokenResult{Token: "u-tok", Scopes: "event:subscription:write"},
	}, nil)

	cmd := NewCmdUpdate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"sub_1", "--as", "user", "--include-resource-data=false"})

	err := cmd.Execute()
	var permErr *errs.PermissionError
	if !errors.As(err, &permErr) {
		t.Fatalf("expected *errs.PermissionError, got %T: %v", err, err)
	}
	if len(permErr.MissingScopes) != 1 || permErr.MissingScopes[0] != "event:subscription:read" {
		t.Errorf("MissingScopes = %v, want [event:subscription:read]", permErr.MissingScopes)
	}
}

func TestRunUpdate_MissingWriteScope_ReturnsPermissionError(t *testing.T) {
	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "cli_x"})
	f.Credential = credential.NewCredentialProvider(nil, nil, &fakeTokenResolver{
		result: &credential.TokenResult{Token: "u-tok", Scopes: "event:subscription:read"},
	}, nil)

	cmd := NewCmdUpdate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"sub_1", "--as", "user", "--include-resource-data=false"})

	err := cmd.Execute()
	var permErr *errs.PermissionError
	if !errors.As(err, &permErr) {
		t.Fatalf("expected *errs.PermissionError, got %T: %v", err, err)
	}
	if len(permErr.MissingScopes) != 1 || permErr.MissingScopes[0] != "event:subscription:write" {
		t.Errorf("MissingScopes = %v, want [event:subscription:write]", permErr.MissingScopes)
	}
}

func TestNewCmdUpdate_HasExpectedFlags(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdUpdate(f)
	for _, name := range []string{"include-resource-data", "dry-run", "yes", "json", "as"} {
		if cmd.Flags().Lookup(name) == nil {
			t.Errorf("NewCmdUpdate missing --%s flag", name)
		}
	}
	if level, ok := cmdutil.GetRisk(cmd); !ok || level != cmdutil.RiskWrite {
		t.Errorf("risk = (%q, %v), want (%q, true)", level, ok, cmdutil.RiskWrite)
	}
}

// TestNewCmdSubscription_RegistersUpdateAsWrite mirrors create_test.go's
// TestNewCmdSubscription_RegistersCreateAsWrite: it locks that update is
// registered in the subscription group (without disturbing list/get/create)
// and carries risk=write.
func TestNewCmdSubscription_RegistersUpdateAsWrite(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdSubscription(f)

	var update *cobra.Command
	for _, c := range cmd.Commands() {
		if c.Name() == "update" {
			update = c
		}
	}
	if update == nil {
		t.Fatal(`subscription command group missing "update" subcommand`)
	}
	level, ok := cmdutil.GetRisk(update)
	if !ok || level != cmdutil.RiskWrite {
		t.Errorf(`"update" risk = (%q, %v), want (%q, true)`, level, ok, cmdutil.RiskWrite)
	}
}
