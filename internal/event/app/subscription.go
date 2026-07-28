// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"errors"

	"github.com/larksuite/cli/errs"
	event "github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/model"
	lark "github.com/larksuite/cli/internal/event/platform/lark"
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

// UpdatePort is the subset of the platform/lark gateway the update flow drives:
// Get (the mandatory remote read — update carries no EventKey, so the fetched
// subscription is the only source of the event type --filter is validated
// against, and of the current filter the no-op guard compares against) and
// Patch (the write, which changes only the server-side filter). The command's
// updateSubscriptionAPI seam satisfies it.
type UpdatePort interface {
	Get(ctx context.Context, remoteSubscriptionID string) (*model.RemoteSubscription, error)
	Patch(ctx context.Context, remoteSubscriptionID string, spec lark.PatchSpec) (*model.RemoteSubscription, error)
}

// UpdateKind classifies an Update outcome for the command to render.
type UpdateKind int

const (
	// UpdatePreview: --dry-run stopped after computing the desired filter and
	// the no-op comparison. Render from Before + NoChange; nothing was written.
	UpdatePreview UpdateKind = iota
	// UpdateNoop: the requested filter already matches the current one, so no
	// Patch was issued. Render the unchanged Before.
	UpdateNoop
	// UpdateApplied: the filter changed and was patched. After is the fresh
	// subscription the Patch returned.
	UpdateApplied
)

// UpdateOutcome is the domain result the update command renders. Before is the
// subscription as read (always present). After is meaningful only for
// UpdateApplied. NoChange is meaningful only for UpdatePreview — it reports
// whether a real run would no-op (already matches) or patch.
type UpdateOutcome struct {
	Kind     UpdateKind
	Before   model.RemoteSubscription
	After    model.RemoteSubscription
	NoChange bool
}

// Update runs the filter-update flow and returns an UpdateOutcome the command
// renders:
//
//   - Read the subscription (svc.Get). Its event_type selects the filter
//     capability the desired filter is validated against; its current filter is
//     the no-op comparison base. A success response with no event_type is a wire
//     anomaly (typed InvalidResponse).
//   - Build the desired filter: --clear-filter is the empty/clear form;
//     otherwise parse+validate filterInput against the event type (a parse/rule
//     violation is the gateway/eventlib typed invalid_argument on --filter,
//     returned unchanged, and never echoes the filter contents).
//   - dryRun: stop (UpdatePreview) — no Patch.
//   - already matches: stop (UpdateNoop) — no Patch.
//   - otherwise: run confirm (if any), then Patch (svc.Patch) and return
//     UpdateApplied.
//
// confirm is an OPTIONAL caller-supplied gate invoked exactly once, at the
// write boundary: after the dry-run and no-op branches, so it fires ONLY when a
// real Patch is imminent. The command layer uses it to require --yes when a
// running local consumer is affected (returning a ConfirmationRequiredError);
// any error it returns aborts before the Patch, so nothing is written. nil
// means "no gate" — the previous unconditional-write behavior. Keeping the
// policy in the caller (which alone knows --yes and the local bus) while the
// use case owns WHEN it fires keeps this layer free of both concerns.
//
// include_resource_data is never touched here (Patch is filter-only). This is
// the one place the update decision lives, so the command only renders.
func (SubscriptionUseCase) Update(ctx context.Context, svc UpdatePort, remoteSubscriptionID, filterInput string, clearFilter, dryRun bool, confirm func() error) (UpdateOutcome, error) {
	before, err := svc.Get(ctx, remoteSubscriptionID)
	if err != nil {
		return UpdateOutcome{}, err
	}

	eventType := before.EventType
	if eventType == "" {
		// A subscription with no event_type can't be mapped to a filter
		// capability, so --filter cannot be validated against it. Treat a
		// successful Get that omits it as a wire anomaly rather than guessing.
		return UpdateOutcome{}, errs.NewInternalError(errs.SubtypeInvalidResponse,
			"subscription %s did not report an event_type, so its filter capability cannot be determined", remoteSubscriptionID)
	}

	var desired *event.Filter
	if clearFilter {
		desired = &event.Filter{}
	} else {
		desired, err = event.ParseAndValidateFilter(filterInput, event.FilterMetaFor(eventType))
		if err != nil {
			return UpdateOutcome{}, err
		}
	}

	noChange := event.Equal(desired, before.Filter)

	if dryRun {
		return UpdateOutcome{Kind: UpdatePreview, Before: *before, NoChange: noChange}, nil
	}
	if noChange {
		return UpdateOutcome{Kind: UpdateNoop, Before: *before}, nil
	}

	// A real filter change is imminent — give the caller its one chance to gate
	// the write (e.g. require --yes when a running local consumer is affected).
	if confirm != nil {
		if err := confirm(); err != nil {
			return UpdateOutcome{}, err
		}
	}

	after, err := svc.Patch(ctx, remoteSubscriptionID, lark.PatchSpec{Filter: desired})
	if err != nil {
		return UpdateOutcome{}, err
	}
	return UpdateOutcome{Kind: UpdateApplied, Before: *before, After: *after}, nil
}
