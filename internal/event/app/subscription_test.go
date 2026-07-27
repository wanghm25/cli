// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"errors"
	"testing"

	"github.com/larksuite/cli/errs"
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
