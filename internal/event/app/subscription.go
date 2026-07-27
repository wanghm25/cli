// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"errors"

	"github.com/larksuite/cli/errs"
	subscription "github.com/larksuite/cli/internal/event/subscription"
)

// SubscriptionController is the Observe→Plan→Apply surface the provisioning
// flow drives: Plan (the read-only Observe+classify a --dry-run renders and a
// real run acts on) and Apply (the single remote write — Create/Reactivate/
// reuse). *subscription.Controller satisfies it. Declared here once so the
// create command and the refined-consume startup share one seam instead of each
// re-declaring an identical Plan/Apply interface, and so tests drive the flow
// with a real Controller over a fake platform/lark gateway (or a fake
// Controller) with no network.
type SubscriptionController interface {
	Plan(ctx context.Context, policy subscription.Policy, req subscription.Request) (subscription.SubscriptionPlan, error)
	Apply(ctx context.Context, plan subscription.SubscriptionPlan, policy subscription.Policy, req subscription.Request) (subscription.ApplyReceipt, error)
}

// ProvisionKind classifies a Provision outcome so the caller renders it without
// re-deciding anything. Exactly one applies to a given result.
type ProvisionKind int

const (
	// ProvisionPreview: --dry-run stopped after Plan. Render the preview from
	// Outcome.Plan; nothing was written.
	ProvisionPreview ProvisionKind = iota
	// ProvisionApplied: a writable plan (create/reuse/reactivate) was applied.
	// Outcome.Receipt is valid; Outcome.Plan is the plan it acted on.
	ProvisionApplied
	// ProvisionBlocked: Plan.Action was Block — a configuration conflict
	// (Outcome.Plan.ConflictFields populated) or, under a policy that does not
	// auto-resume, a suspended match. Render the typed conflict/suspended error
	// from Outcome.Plan; nothing was written.
	ProvisionBlocked
	// ProvisionIndeterminate: the remote scan was inconclusive (the paginated
	// List hit its page cap without a definitive answer). Fail closed; nothing
	// was written.
	ProvisionIndeterminate
)

// ProvisionOutcome is the single result the create command (and any future
// provisioning caller) renders. Plan is ALWAYS the one Plan the flow computed —
// the same object a --dry-run previews and a real run acts on, so there is never
// a separately-built fake preview. Receipt is meaningful only for
// ProvisionApplied.
type ProvisionOutcome struct {
	Kind    ProvisionKind
	Plan    subscription.SubscriptionPlan
	Receipt subscription.ApplyReceipt
}

// SubscriptionUseCase owns the remote-Subscription management-plane
// orchestration. Provision is the Observe→Plan→Apply flow that
// `event subscription create` drives; it composes the subscription.Controller
// (PR2's Observe/Plan/Apply owner) and returns a domain ProvisionOutcome the
// command renders. It never renders output, builds a wire shape, or reads a
// Cobra flag itself.
type SubscriptionUseCase struct{}

// NewSubscriptionUseCase builds a SubscriptionUseCase. It is stateless — each
// method takes the identity-bound Controller the command already built — so one
// value serves every invocation.
func NewSubscriptionUseCase() SubscriptionUseCase { return SubscriptionUseCase{} }

// Provision runs the one Observe→Plan→(Apply) flow under policy for req and
// returns a ProvisionOutcome the caller renders:
//
//   - Plan once (a read-only Observe+classify). The resulting SubscriptionPlan
//     rides on EVERY outcome, so a --dry-run and a real run render from the SAME
//     plan.
//   - dryRun: stop after Plan (ProvisionPreview) — no Apply, no write.
//   - Block: ProvisionBlocked (a configuration conflict, or a suspended match a
//     non-resuming policy refuses to overwrite) — no write.
//   - Indeterminate: ProvisionIndeterminate (inconclusive scan) — no write.
//   - Create/Reuse/Reactivate: Apply the plan (ProvisionApplied). A Controller
//     reconcile-after-a-failed-create that lands on a now-blocking remote state
//     surfaces as a *subscription.PlanBlockedError, which is unwrapped back into
//     a ProvisionBlocked outcome carrying the raced plan — so the caller renders
//     the same typed conflict a first-pass Block would, never the raw create
//     error.
//
// This is the single place controller.Plan / controller.Apply are called for
// the provisioning flow, so the command holds none of that orchestration.
func (SubscriptionUseCase) Provision(ctx context.Context, controller SubscriptionController, policy subscription.Policy, req subscription.Request, dryRun bool) (ProvisionOutcome, error) {
	plan, err := controller.Plan(ctx, policy, req)
	if err != nil {
		return ProvisionOutcome{}, err
	}

	if dryRun {
		return ProvisionOutcome{Kind: ProvisionPreview, Plan: plan}, nil
	}

	switch plan.Action {
	case subscription.ActionBlock:
		return ProvisionOutcome{Kind: ProvisionBlocked, Plan: plan}, nil
	case subscription.ActionIndeterminate:
		return ProvisionOutcome{Kind: ProvisionIndeterminate, Plan: plan}, nil
	case subscription.ActionCreate, subscription.ActionReuse, subscription.ActionReactivate:
		receipt, err := controller.Apply(ctx, plan, policy, req)
		if err != nil {
			// The Controller's bounded reconcile-after-a-failed-create pass may
			// have found a raced remote state that now blocks — surface it as the
			// same ProvisionBlocked outcome a first-pass block produces, carrying
			// the raced plan for the caller to render.
			var blocked *subscription.PlanBlockedError
			if errors.As(err, &blocked) {
				return ProvisionOutcome{Kind: ProvisionBlocked, Plan: blocked.Plan}, nil
			}
			return ProvisionOutcome{}, err
		}
		return ProvisionOutcome{Kind: ProvisionApplied, Plan: plan, Receipt: receipt}, nil
	default:
		// Update/unknown: the provisioning flow never plans these; a Plan that
		// returns one is an internal inconsistency, not a writable action.
		return ProvisionOutcome{}, errs.NewInternalError(errs.SubtypeUnknown,
			"subscription use case: Provision reached a non-writable plan action %q", plan.Action)
	}
}
