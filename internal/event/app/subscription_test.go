// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"errors"
	"testing"

	"github.com/larksuite/cli/errs"
	event "github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/model"
	lark "github.com/larksuite/cli/internal/event/platform/lark"
	subscription "github.com/larksuite/cli/internal/event/subscription"
)

// fakeController is a network-free SubscriptionController that returns canned
// Plan/Apply results and records how many times each was called, so a test
// asserts both the outcome classification AND that Apply is (or is not) reached.
type fakeController struct {
	plan      subscription.SubscriptionPlan
	planErr   error
	planCalls int

	receipt     subscription.ApplyReceipt
	applyErr    error
	applyCalls  int
	appliedPlan subscription.SubscriptionPlan
}

func (c *fakeController) Plan(_ context.Context, _ subscription.Policy, _ subscription.Request) (subscription.SubscriptionPlan, error) {
	c.planCalls++
	return c.plan, c.planErr
}

func (c *fakeController) Apply(_ context.Context, plan subscription.SubscriptionPlan, _ subscription.Policy, _ subscription.Request) (subscription.ApplyReceipt, error) {
	c.applyCalls++
	c.appliedPlan = plan
	return c.receipt, c.applyErr
}

func provision(t *testing.T, c *fakeController, dryRun bool) ProvisionOutcome {
	t.Helper()
	out, err := NewSubscriptionUseCase().Provision(context.Background(), c, subscription.ManagementCreate, subscription.Request{}, dryRun)
	if err != nil {
		t.Fatalf("Provision: unexpected error: %v", err)
	}
	return out
}

// TestProvision_DryRun_StopsAfterPlan_NoApply locks the security-relevant
// invariant: --dry-run computes the plan and returns it as a Preview WITHOUT
// ever calling Apply (no write), even for a writable Create plan.
func TestProvision_DryRun_StopsAfterPlan_NoApply(t *testing.T) {
	c := &fakeController{plan: subscription.SubscriptionPlan{Action: subscription.ActionCreate, Reason: "fresh"}}
	out := provision(t, c, true)

	if out.Kind != ProvisionPreview {
		t.Errorf("Kind = %v, want ProvisionPreview", out.Kind)
	}
	if c.applyCalls != 0 {
		t.Errorf("Apply calls = %d on a dry-run, want 0", c.applyCalls)
	}
	if out.Plan.Action != subscription.ActionCreate {
		t.Errorf("preview carried plan action %q, want the planned %q", out.Plan.Action, subscription.ActionCreate)
	}
}

// TestProvision_Block_ClassifiesBlocked_NoApply locks that a Block plan is
// returned as ProvisionBlocked carrying the plan, never applied.
func TestProvision_Block_ClassifiesBlocked_NoApply(t *testing.T) {
	c := &fakeController{plan: subscription.SubscriptionPlan{
		Action:         subscription.ActionBlock,
		Reason:         "include_resource_data_mismatch",
		ConflictFields: []errs.InvalidParam{{Name: "include_resource_data", Reason: "differs"}},
	}}
	out := provision(t, c, false)

	if out.Kind != ProvisionBlocked {
		t.Fatalf("Kind = %v, want ProvisionBlocked", out.Kind)
	}
	if c.applyCalls != 0 {
		t.Errorf("Apply calls = %d on a Block, want 0", c.applyCalls)
	}
	if len(out.Plan.ConflictFields) != 1 {
		t.Errorf("blocked outcome must carry the plan's conflict fields for the caller to render, got %d", len(out.Plan.ConflictFields))
	}
}

// TestProvision_Indeterminate_ClassifiesIndeterminate_NoApply locks the
// fail-closed inconclusive-scan outcome.
func TestProvision_Indeterminate_ClassifiesIndeterminate_NoApply(t *testing.T) {
	c := &fakeController{plan: subscription.SubscriptionPlan{Action: subscription.ActionIndeterminate, Reason: "list_incomplete"}}
	out := provision(t, c, false)

	if out.Kind != ProvisionIndeterminate {
		t.Errorf("Kind = %v, want ProvisionIndeterminate", out.Kind)
	}
	if c.applyCalls != 0 {
		t.Errorf("Apply calls = %d on Indeterminate, want 0", c.applyCalls)
	}
}

// TestProvision_WritableActions_Apply covers each writable action reaching
// Apply and returning an Applied outcome carrying the receipt + the plan it
// acted on.
func TestProvision_WritableActions_Apply(t *testing.T) {
	for _, action := range []subscription.Action{subscription.ActionCreate, subscription.ActionReuse, subscription.ActionReactivate} {
		t.Run(string(action), func(t *testing.T) {
			c := &fakeController{
				plan:    subscription.SubscriptionPlan{Action: action},
				receipt: subscription.ApplyReceipt{Action: action, CreatedByAttempt: action == subscription.ActionCreate},
			}
			out := provision(t, c, false)

			if out.Kind != ProvisionApplied {
				t.Fatalf("Kind = %v, want ProvisionApplied", out.Kind)
			}
			if c.applyCalls != 1 {
				t.Errorf("Apply calls = %d, want 1", c.applyCalls)
			}
			if out.Receipt.Action != action {
				t.Errorf("receipt action = %q, want %q", out.Receipt.Action, action)
			}
			if c.appliedPlan.Action != action {
				t.Errorf("Apply received plan action %q, want %q", c.appliedPlan.Action, action)
			}
		})
	}
}

// TestProvision_PlanError_Propagates_NoApply locks that a Plan (Observe) error
// short-circuits before Apply.
func TestProvision_PlanError_Propagates_NoApply(t *testing.T) {
	sentinel := errors.New("list failed")
	c := &fakeController{planErr: sentinel}
	_, err := NewSubscriptionUseCase().Provision(context.Background(), c, subscription.ManagementCreate, subscription.Request{}, false)
	if !errors.Is(err, sentinel) {
		t.Fatalf("Provision err = %v, want the Plan error", err)
	}
	if c.applyCalls != 0 {
		t.Errorf("Apply calls = %d after a Plan error, want 0", c.applyCalls)
	}
}

// TestProvision_ApplyError_Propagates locks that a non-PlanBlocked Apply error
// surfaces unchanged (the original create failure).
func TestProvision_ApplyError_Propagates(t *testing.T) {
	sentinel := errors.New("create failed")
	c := &fakeController{plan: subscription.SubscriptionPlan{Action: subscription.ActionCreate}, applyErr: sentinel}
	_, err := NewSubscriptionUseCase().Provision(context.Background(), c, subscription.ManagementCreate, subscription.Request{}, false)
	if !errors.Is(err, sentinel) {
		t.Fatalf("Provision err = %v, want the Apply error unchanged", err)
	}
}

// TestProvision_ApplyPlanBlocked_UnwrapsToBlocked locks the reconcile-after-a-
// failed-create recovery: a Controller PlanBlockedError becomes a
// ProvisionBlocked outcome carrying the RACED plan, so the caller renders the
// same typed conflict a first-pass Block would, not the raw create error.
func TestProvision_ApplyPlanBlocked_UnwrapsToBlocked(t *testing.T) {
	raced := subscription.SubscriptionPlan{Action: subscription.ActionBlock, Reason: "raced_conflict"}
	c := &fakeController{
		plan:     subscription.SubscriptionPlan{Action: subscription.ActionCreate},
		applyErr: &subscription.PlanBlockedError{Plan: raced},
	}
	out := provision(t, c, false)

	if out.Kind != ProvisionBlocked {
		t.Fatalf("Kind = %v, want ProvisionBlocked", out.Kind)
	}
	if out.Plan.Reason != "raced_conflict" {
		t.Errorf("blocked outcome carried plan reason %q, want the raced plan %q", out.Plan.Reason, "raced_conflict")
	}
}

// TestProvision_NonWritableAction_InternalError locks the fail-closed default:
// a plan action the provisioning flow never emits (e.g. Update) on a real run
// is an internal inconsistency, not a silent write.
func TestProvision_NonWritableAction_InternalError(t *testing.T) {
	c := &fakeController{plan: subscription.SubscriptionPlan{Action: subscription.ActionUpdate}}
	_, err := NewSubscriptionUseCase().Provision(context.Background(), c, subscription.ManagementCreate, subscription.Request{}, false)
	if err == nil {
		t.Fatal("Provision(Update) returned no error, want an internal error")
	}
	if c.applyCalls != 0 {
		t.Errorf("Apply calls = %d for a non-writable action, want 0", c.applyCalls)
	}
}

// ---- Update (owns the read -> validate -> no-op-guard -> patch flow) ----

// init seeds a filter capability for the app.update.test_v1 event type so the
// Update tests can build a non-empty "current" filter via ParseAndValidateFilter
// (mirroring the self-contained registration internal/event/consume's
// refined_test.go uses).
func init() {
	event.RegisterFilterMeta("app.update.test_v1", event.FilterMeta{
		Supported:     true,
		LogicOps:      []string{"and", "or"},
		Operators:     []string{"eq"},
		MaxDepth:      2,
		MaxConditions: 10,
		MaxBytes:      1024,
		Operands: []event.FilterOperandMeta{
			{Key: "message_type", Operators: []string{"eq"}},
		},
	})
}

const updateTestEventType = "app.update.test_v1"

// nonEmptyFilter builds a valid, non-empty filter for updateTestEventType.
func nonEmptyFilter(t *testing.T) *event.Filter {
	t.Helper()
	f, err := event.ParseAndValidateFilter(
		`{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"message_type","op":"eq","value":"text"}}]}}`,
		event.FilterMetaFor(updateTestEventType))
	if err != nil {
		t.Fatalf("ParseAndValidateFilter: %v", err)
	}
	if f.IsEmpty() {
		t.Fatal("nonEmptyFilter built an empty filter")
	}
	return f
}

// fakeUpdatePort is a network-free UpdatePort: Get hands back a canned
// subscription; Patch captures its spec and returns a canned result.
type fakeUpdatePort struct {
	getSub    *model.RemoteSubscription
	getErr    error
	patchResp *model.RemoteSubscription
	patchErr  error

	patchCalls int
	patchSpec  lark.PatchSpec
}

func (f *fakeUpdatePort) Get(context.Context, string) (*model.RemoteSubscription, error) {
	return f.getSub, f.getErr
}

func (f *fakeUpdatePort) Patch(_ context.Context, _ string, spec lark.PatchSpec) (*model.RemoteSubscription, error) {
	f.patchCalls++
	f.patchSpec = spec
	return f.patchResp, f.patchErr
}

func emptyFilterSub() *model.RemoteSubscription {
	return &model.RemoteSubscription{EventType: updateTestEventType, Filter: &event.Filter{}}
}

// TestUpdate_GetError_Propagates locks that the mandatory read's error
// short-circuits before any Patch.
func TestUpdate_GetError_Propagates(t *testing.T) {
	sentinel := errors.New("get failed")
	svc := &fakeUpdatePort{getErr: sentinel}
	_, err := NewSubscriptionUseCase().Update(context.Background(), svc, "sub_1", "", true, false, nil)
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want the Get error", err)
	}
	if svc.patchCalls != 0 {
		t.Errorf("patchCalls = %d after a Get error, want 0", svc.patchCalls)
	}
}

// TestUpdate_MissingEventType_TypedError locks the wire-anomaly guard: a Get
// that omits event_type is an InvalidResponse, never a guess, and never patches.
func TestUpdate_MissingEventType_TypedError(t *testing.T) {
	svc := &fakeUpdatePort{getSub: &model.RemoteSubscription{EventType: "", Filter: &event.Filter{}}}
	_, err := NewSubscriptionUseCase().Update(context.Background(), svc, "sub_1", "", true, false, nil)
	var ie *errs.InternalError
	if !errors.As(err, &ie) || ie.Subtype != errs.SubtypeInvalidResponse {
		t.Fatalf("err = %v, want InternalError/invalid_response", err)
	}
	if svc.patchCalls != 0 {
		t.Errorf("patchCalls = %d, want 0", svc.patchCalls)
	}
}

// TestUpdate_DryRun_Preview_NoPatch locks that --dry-run computes the outcome
// (including NoChange) without ever patching.
func TestUpdate_DryRun_Preview_NoPatch(t *testing.T) {
	svc := &fakeUpdatePort{getSub: emptyFilterSub()}
	out, err := NewSubscriptionUseCase().Update(context.Background(), svc, "sub_1", "", true, true, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Kind != UpdatePreview {
		t.Errorf("Kind = %v, want UpdatePreview", out.Kind)
	}
	if !out.NoChange {
		t.Error("clearing an already-empty filter should preview NoChange=true")
	}
	if svc.patchCalls != 0 {
		t.Errorf("patchCalls = %d on a dry-run, want 0", svc.patchCalls)
	}
}

// TestUpdate_ClearFilter_Noop_WhenAlreadyEmpty locks the no-op guard: clearing
// an already-empty filter issues no Patch.
func TestUpdate_ClearFilter_Noop_WhenAlreadyEmpty(t *testing.T) {
	svc := &fakeUpdatePort{getSub: emptyFilterSub()}
	out, err := NewSubscriptionUseCase().Update(context.Background(), svc, "sub_1", "", true, false, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Kind != UpdateNoop {
		t.Errorf("Kind = %v, want UpdateNoop", out.Kind)
	}
	if svc.patchCalls != 0 {
		t.Errorf("patchCalls = %d for a no-op, want 0", svc.patchCalls)
	}
}

// TestUpdate_ClearFilter_Patches locks the write path: clearing a present filter
// issues exactly one Patch with the empty/clear form and returns the fresh
// subscription as After.
func TestUpdate_ClearFilter_Patches(t *testing.T) {
	after := &model.RemoteSubscription{EventType: updateTestEventType, Filter: &event.Filter{}}
	svc := &fakeUpdatePort{
		getSub:    &model.RemoteSubscription{EventType: updateTestEventType, Filter: nonEmptyFilter(t)},
		patchResp: after,
	}
	out, err := NewSubscriptionUseCase().Update(context.Background(), svc, "sub_1", "", true, false, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Kind != UpdateApplied {
		t.Fatalf("Kind = %v, want UpdateApplied", out.Kind)
	}
	if svc.patchCalls != 1 {
		t.Errorf("patchCalls = %d, want 1", svc.patchCalls)
	}
	if svc.patchSpec.Filter == nil || !svc.patchSpec.Filter.IsEmpty() {
		t.Errorf("Patch spec filter = %v, want the empty/clear form", svc.patchSpec.Filter)
	}
}

// TestUpdate_PatchError_Propagates locks that a Patch failure surfaces unchanged.
func TestUpdate_PatchError_Propagates(t *testing.T) {
	sentinel := errors.New("patch failed")
	svc := &fakeUpdatePort{
		getSub:   &model.RemoteSubscription{EventType: updateTestEventType, Filter: nonEmptyFilter(t)},
		patchErr: sentinel,
	}
	_, err := NewSubscriptionUseCase().Update(context.Background(), svc, "sub_1", "", true, false, nil)
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want the Patch error unchanged", err)
	}
}
