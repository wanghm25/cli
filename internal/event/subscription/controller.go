// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"context"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/model"
	lark "github.com/larksuite/cli/internal/event/platform/lark"
)

// Gateway is the remote-Subscription surface the Controller (and its embedded
// Observer/Planner) drives: the bounded List scan, the two writes it performs
// (Create/Reactivate), and the encrypt-key probe. *lark.Gateway satisfies it.
type Gateway interface {
	WalkSubscriptions(ctx context.Context, params lark.ListParams, visit func(lark.RemoteSubscription) bool) (bool, error)
	Create(ctx context.Context, spec lark.CreateSpec) (*lark.RemoteSubscription, error)
	Reactivate(ctx context.Context, remoteSubscriptionID string) (*lark.RemoteSubscription, error)
	GetEncryptKey(ctx context.Context, remoteSubscriptionID string) (string, error)
}

// ApplyReceipt records the outcome of a completed writable plan. RemoteID is the
// non-empty remote subscription id the caller threads onward (into the create
// result, or the consume Hello); an empty id from the wire is refused as an
// InvalidResponse rather than surfaced as an empty receipt. CreatedByAttempt is
// true only when this Apply issued the Create that produced the subscription.
// Before is the pre-existing match (nil for a fresh create); After is the
// resulting subscription (the created/reactivated one, or the reused match).
type ApplyReceipt struct {
	RemoteID         model.RemoteSubscriptionID
	Action           Action
	CreatedByAttempt bool
	Before           *lark.RemoteSubscription
	After            *lark.RemoteSubscription
}

// Controller is the single remote-write path for the Observe -> Plan -> Apply
// flow. It composes an Observer and Planner over the same gateway, so Plan is a
// read-only classification a --dry-run can render, and Apply is the only place
// gateway.Create / gateway.Reactivate are ever called.
type Controller struct {
	gateway       Gateway
	observer      Observer
	planner       Planner
	newEncryptKey func() (string, error)
}

// Option customizes a Controller at construction.
type Option func(*Controller)

// WithEncryptKeyGenerator overrides the per-subscription encrypt_key generator
// used for an encrypted create. Production uses event.NewEncryptKey; tests inject
// a spy to assert it is called exactly once for an encrypted create and never for
// a plaintext create / reuse / reactivate, and to capture the generated key for
// redaction checks.
func WithEncryptKeyGenerator(fn func() (string, error)) Option {
	return func(c *Controller) { c.newEncryptKey = fn }
}

// NewController builds a Controller over gw.
func NewController(gw Gateway, opts ...Option) *Controller {
	c := &Controller{
		gateway:       gw,
		observer:      NewObserver(gw),
		planner:       NewPlanner(gw),
		newEncryptKey: event.NewEncryptKey,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Plan observes remote state and classifies it under policy for req. It never
// writes, so it is what a --dry-run / plan-only preflight calls.
func (c *Controller) Plan(ctx context.Context, policy Policy, req Request) (SubscriptionPlan, error) {
	obs, err := c.observer.Observe(ctx, req)
	if err != nil {
		return SubscriptionPlan{}, err
	}
	return c.planner.Plan(ctx, obs, policy, req)
}

// PlanBlockedError wraps a non-writable (Block) plan the Controller reached
// during its bounded reconcile-after-a-failed-Create pass, so the caller renders
// its own typed conflict/suspended error for the raced remote state instead of
// the raw create failure. Only ManagementCreate's Apply can produce it.
type PlanBlockedError struct {
	Plan SubscriptionPlan
}

func (e *PlanBlockedError) Error() string {
	return "subscription apply: reconcile after a failed create found a blocking remote state"
}

// Apply executes a writable plan (Create/Reuse/Reactivate) and returns its
// receipt. Block and Indeterminate are NOT writable — the caller must handle
// those before calling Apply (rendering its own typed error); calling Apply with
// them is an internal error.
//
// For a Create whose gateway.Create fails, a Policy with
// reconcileAfterCreateFailure re-plans once (a bounded pass, never a retry loop
// and never a second Create): if that finds a now-reusable/reactivatable match
// it applies it; if it finds a Block it returns a PlanBlockedError for the caller
// to render; otherwise the ORIGINAL create error is returned unchanged. This
// preserves create's "someone raced us to a compatible/conflicting subscription"
// recovery without ever silently downgrading an encrypted create to plaintext
// (req is unchanged between passes) and without a consumer bootstrap re-listing.
func (c *Controller) Apply(ctx context.Context, plan SubscriptionPlan, policy Policy, req Request) (ApplyReceipt, error) {
	switch plan.Action {
	case ActionReuse:
		return c.reuseReceipt(plan)
	case ActionReactivate:
		return c.reactivate(ctx, plan)
	case ActionCreate:
		receipt, err := c.create(ctx, req)
		if err == nil {
			return receipt, nil
		}
		if !policy.reconcileAfterCreateFailure {
			return ApplyReceipt{}, err
		}
		return c.reconcileAfterCreateFailure(ctx, policy, req, err)
	default:
		return ApplyReceipt{}, errs.NewInternalError(errs.SubtypeUnknown,
			"subscription controller: Apply called with non-writable action %q", plan.Action)
	}
}

// reconcileAfterCreateFailure is the one bounded post-Create-failure pass. It
// never re-issues Create itself; origErr is the Create error it falls back to
// whenever the re-plan is inconclusive.
func (c *Controller) reconcileAfterCreateFailure(ctx context.Context, policy Policy, req Request, origErr error) (ApplyReceipt, error) {
	plan2, planErr := c.Plan(ctx, policy, req)
	if planErr != nil {
		return ApplyReceipt{}, origErr
	}
	switch plan2.Action {
	case ActionReuse:
		return c.reuseReceipt(plan2)
	case ActionReactivate:
		return c.reactivate(ctx, plan2)
	case ActionBlock:
		return ApplyReceipt{}, &PlanBlockedError{Plan: plan2}
	default:
		// Still Create, or Indeterminate: nothing new blocks or reuses -- the
		// original Create error is the most useful thing to surface.
		return ApplyReceipt{}, origErr
	}
}

// reuseReceipt turns a Reuse plan into a receipt, validating the reused match's
// id is non-empty (a List item projects an empty id straight through; reusing
// one is an InvalidResponse rather than a silently empty receipt).
func (c *Controller) reuseReceipt(plan SubscriptionPlan) (ApplyReceipt, error) {
	if plan.Before == nil {
		return ApplyReceipt{}, errs.NewInternalError(errs.SubtypeUnknown,
			"subscription controller: reuse plan carries no existing subscription")
	}
	id, err := requireID(plan.Before.ID)
	if err != nil {
		return ApplyReceipt{}, err
	}
	return ApplyReceipt{RemoteID: id, Action: ActionReuse, CreatedByAttempt: false, Before: plan.Before, After: plan.Before}, nil
}

// reactivate resumes a suspended match via the gateway and returns its receipt.
func (c *Controller) reactivate(ctx context.Context, plan SubscriptionPlan) (ApplyReceipt, error) {
	if plan.Before == nil {
		return ApplyReceipt{}, errs.NewInternalError(errs.SubtypeUnknown,
			"subscription controller: reactivate plan carries no existing subscription")
	}
	before := plan.Before
	after, err := c.gateway.Reactivate(ctx, before.ID.String())
	if err != nil {
		return ApplyReceipt{}, err
	}
	id, err := requireID(subscriptionID(after))
	if err != nil {
		return ApplyReceipt{}, err
	}
	return ApplyReceipt{RemoteID: id, Action: ActionReactivate, CreatedByAttempt: false, Before: before, After: after}, nil
}

// create issues the single Create write. For an encrypted request it generates a
// fresh per-subscription encrypt_key and hands it to the gateway to inject
// atomically with include_resource_data=true; a key-gen failure aborts the
// create (fail-closed, never a plaintext fallback). The key is never logged,
// persisted, or returned.
func (c *Controller) create(ctx context.Context, req Request) (ApplyReceipt, error) {
	spec := lark.CreateSpec{
		EventType:           req.EventType,
		TargetResource:      req.TargetResource,
		IncludeResourceData: req.IncludeResourceData,
		Filter:              req.Filter,
	}
	if req.IncludeResourceData {
		key, err := c.newEncryptKey()
		if err != nil {
			return ApplyReceipt{}, err
		}
		spec.EncryptKey = key
	}
	after, err := c.gateway.Create(ctx, spec)
	if err != nil {
		return ApplyReceipt{}, err
	}
	id, err := requireID(subscriptionID(after))
	if err != nil {
		return ApplyReceipt{}, err
	}
	return ApplyReceipt{RemoteID: id, Action: ActionCreate, CreatedByAttempt: true, After: after}, nil
}

// subscriptionID reads a (possibly nil) gateway result's id. The gateway already
// validates non-empty on success, but this stays nil-safe so requireID produces
// one uniform InvalidResponse.
func subscriptionID(sub *lark.RemoteSubscription) model.RemoteSubscriptionID {
	if sub == nil {
		return ""
	}
	return sub.ID
}

// requireID enforces the non-empty remote_subscription_id invariant, mapping an
// empty id to a typed InvalidResponse.
func requireID(id model.RemoteSubscriptionID) (model.RemoteSubscriptionID, error) {
	validated, err := model.NewRemoteSubscriptionID(id.String())
	if err != nil {
		return "", errs.NewInternalError(errs.SubtypeInvalidResponse,
			"subscription apply reported success but produced an empty remote_subscription_id")
	}
	return validated, nil
}
