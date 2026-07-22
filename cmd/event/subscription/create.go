// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	eventlib "github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/output"
)

// subscriptionMutationScopes are the scopes hard-required by every mutating
// subscription subcommand (create/update/renew/reactivate/delete — spec
// §3.5/§7): unlike list/get, which only ever read, every mutating command
// always reads remote state first (List/Get, for idempotency/conflict/impact
// analysis) before it may write, so it needs read AND write simultaneously —
// neither implies the other. Declared once here so later sibling commands
// (update/renew/reactivate/delete) can reuse it without duplicating the list.
var subscriptionMutationScopes = []string{"event:subscription:read", "event:subscription:write"}

// createOpts holds `event subscription create`'s flag values (spec §3.1).
type createOpts struct {
	includeResourceData bool
	dryRun              bool
	asJSON              bool
}

// NewCmdCreate builds `event subscription create <refined EventKey>` (spec
// §3.3). Unlike list/get, create additionally: resolves the EventKey via
// eventlib.ResolveEventKey (target_resource/event_type), enforces the
// matched KeyTemplate's own AuthTypes on top of the usual identity checks
// (spec §2.8), requires BOTH event:subscription:read and
// event:subscription:write (spec §3.5), gates --include-resource-data=true
// (spec §9), and reconciles against remote state before ever writing (spec
// §3.3/§4.2: not-exist -> create, active+compatible -> idempotent reuse,
// active+conflicting -> typed failed_precondition, suspended -> guide
// reactivate). It never exposes --yes: create is additive and pre-checked
// for conflicts, so it is not a high-risk confirmation-gated action (spec
// §3.7) — that is reserved for update/delete.
func NewCmdCreate(f *cmdutil.Factory) *cobra.Command {
	var o createOpts
	cmd := &cobra.Command{
		Use:   "create <refined EventKey>",
		Short: "Create (or idempotently reuse) a remote event Subscription",
		Long: `Create a remote Subscription for a materialized refined EventKey (e.g.
'im.message.created_v1/chat-id/oc_xxx'), or idempotently reuse an existing
compatible one. Requires BOTH the event:subscription:read and
event:subscription:write scopes: this command always reads remote state
first (to detect an existing subscription and analyze impact) before it may
create anything.

A bare refined base key (no template segment) is rejected — pass a
materialized key such as one of the key_templates[].example values from
'event schema <base> --json'.

The resolved --as identity must be supported by the EventKey's matched
KeyTemplate specifically (not just the EventKey in general) — e.g. the
'owner/me' template only supports --as user.

Use --dry-run to preview the plan (parse/identity/scope preflight + a
remote read + impact analysis) without creating, reusing, or changing
anything. Creating a subscription does not start listening — run 'event
consume <refined EventKey>' afterwards.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCreate(cmd, f, args[0], o)
		},
	}

	cmd.Flags().BoolVar(&o.includeResourceData, "include-resource-data", false,
		"Include resource data in delivered events (gated: not yet supported pending the encryption module, see design spec §9)")
	cmd.Flags().BoolVar(&o.dryRun, "dry-run", false,
		"Preview the plan (identity/scope preflight + remote read + impact analysis) without creating, reusing, or changing anything")
	cmd.Flags().BoolVar(&o.asJSON, "json", false, "Emit the result as JSON (for AI / scripts)")
	addAsFlag(cmd)
	cmdutil.SetRisk(cmd, "write")

	return cmd
}

func runCreate(cmd *cobra.Command, f *cmdutil.Factory, eventKeyArg string, o createOpts) error {
	ctx := cmd.Context()

	// f.Config() is deliberately deferred until it is actually needed (just
	// before the scope preflight below, which is the first thing that reads
	// cfg.AppID) rather than called unconditionally up front the way
	// list.go/get.go do: create has several cheap, purely local checks
	// (EventKey resolution, template identity, the §9 E-gate) that must be
	// able to reject a request before doing ANY other work, including a
	// config lookup.
	resolved, err := eventlib.ResolveEventKey(eventKeyArg)
	if err != nil {
		// Passed through unchanged: R1 (bare refined base key), an unknown
		// base, a legacy key with a suffix, a bad template segment/value,
		// etc. are all already typed errors (Task 3) — this command must not
		// re-wrap or reword them.
		return err
	}
	if !resolved.IsRefined {
		// A legacy (non-refined) key has no Template/TargetResource — there
		// is nothing for this command's reconciliation to key off, and no
		// remote Subscription resource concept applies to it at all (spec
		// §3.3 frames `create` exclusively around "refined EventKey").
		return errCreateRequiresRefinedKey(eventKeyArg)
	}

	identity, err := resolveEffectiveIdentity(cmd, f)
	if err != nil {
		return err
	}
	// Tier 1 (spec §2.8): the identity must be one the whole refined base
	// key accepts at all — mirrors cmd/event/consume.go's resolveIdentity.
	// Empty AuthTypes means "no restriction" (KeyDefinition.AuthTypes' own
	// doc comment), so guard the call the same way consume.go does: calling
	// f.CheckIdentity with an empty supported list would reject everything.
	if len(resolved.Definition.AuthTypes) > 0 {
		if err := f.CheckIdentity(identity, resolved.Definition.AuthTypes); err != nil {
			return err
		}
	}
	// Tier 2 (spec §2.8, stricter — this is what list/get do not need,
	// since they carry no EventKey/KeyTemplate context at all): the specific
	// matched KeyTemplate can be narrower than the key itself (e.g.
	// "owner/me" is user-only even though the key allows user+bot). A
	// key-level pass does not imply a template-level pass.
	if err := checkTemplateAuthTypes(identity, resolved); err != nil {
		return err
	}

	// §9 E-gate: independent of identity/scope/remote state. Checked before
	// the scope preflight/remote read below so a gated request never causes
	// any network activity at all.
	if o.includeResourceData {
		return errIncludeResourceDataGated()
	}

	cfg, err := f.Config()
	if err != nil {
		return err
	}
	uat, err := resolveUATAndCheckScopes(ctx, f, cfg.AppID, identity, subscriptionMutationScopes)
	if err != nil {
		return err
	}

	sdk, err := f.LarkClient()
	if err != nil {
		return err
	}
	client, err := eventlib.NewSubscriptionClient(sdk, identity, uat)
	if err != nil {
		return err
	}

	if o.dryRun {
		plan, err := reconcileExisting(ctx, client, resolved.Definition.EventType, resolved.TargetResource, identity, o.includeResourceData)
		if err != nil {
			return err
		}
		result := buildDryRunResult(resolved, identity, plan)
		if o.asJSON {
			output.PrintJson(f.IOStreams.Out, result)
			return nil
		}
		writeCreateDryRunText(f.IOStreams.Out, result)
		return nil
	}

	outcome, err := createOrReuseSubscription(ctx, client, resolved, identity, o.includeResourceData)
	if err != nil {
		return err
	}
	result := buildCreateResult(resolved, identity, outcome)
	if o.asJSON {
		output.PrintJson(f.IOStreams.Out, result)
		return nil
	}
	writeCreateText(f.IOStreams.Out, result)
	return nil
}

// checkTemplateAuthTypes enforces design spec §2.8's second, stricter
// identity tier for a refined EventKey with a matched KeyTemplate: even when
// the resolved identity is one the whole base key accepts (checked by the
// caller against resolved.Definition.AuthTypes), the SPECIFIC matched
// template may accept a narrower set (e.g. "owner/me" is user-only even
// though its base key im.message.created_v1 allows user+bot). Empty
// Template.AuthTypes means "no additional restriction" (mirrors
// KeyDefinition.AuthTypes' own "empty = no identity required" convention).
// Violated -> typed failed_precondition (never a silent identity switch —
// AGENTS.md "no silent downgrade"), naming the allowed identities so the
// caller can retry explicitly with --as.
func checkTemplateAuthTypes(identity core.Identity, resolved eventlib.ResolvedEventKey) error {
	allowed := resolved.Template.AuthTypes
	if len(allowed) == 0 {
		return nil
	}
	if slices.Contains(allowed, string(identity)) {
		return nil
	}
	return errs.NewValidationError(errs.SubtypeFailedPrecondition,
		"EventKey template %s only supports identity: %s; resolved identity is %q",
		resolved.Template.Template, strings.Join(allowed, ", "), identity).
		WithParam("--as").
		WithHint("retry with --as %s", strings.Join(allowed, " or "))
}

// errCreateRequiresRefinedKey rejects a syntactically valid, registered
// EventKey that resolved as legacy (non-refined): `subscription create` only
// operates on the remote Subscription resource, which only a materialized
// refined key maps onto (spec §3.3).
func errCreateRequiresRefinedKey(eventKeyArg string) error {
	return errs.NewValidationError(errs.SubtypeInvalidArgument,
		"EventKey %q is not a refined-subscription key; `event subscription create` only accepts a materialized refined EventKey", eventKeyArg).
		WithParam("event_key").
		WithHint("run `lark-cli event schema %s --json` to check whether this key supports refined subscriptions and, if so, see key_templates[].example for a key to pass here", eventKeyArg)
}

// errIncludeResourceDataGated implements the §9 E-gate: this iteration
// gates --include-resource-data=true for both user and bot identities (even
// though bot delivery does not itself need an encrypt_key at the OAPI
// level) so the consume side never has to handle resource_data it cannot yet
// decrypt.
func errIncludeResourceDataGated() error {
	return errs.NewValidationError(errs.SubtypeFailedPrecondition,
		"--include-resource-data=true is not yet supported").
		WithParam("--include-resource-data").
		WithHint("resource-data delivery and decryption (including user-subscription encrypt_key generation) is not yet supported (reason: resource_data_encryption_deferred); it will ship with the encryption module. Retry with --include-resource-data=false (the default).")
}

// ---- remote reconciliation (spec §3.3/§4.2) ----
//
// The reconcile classification itself — the state table, authority
// matching, and the ReconcilePlan shape — moved to
// internal/event/reconcile.go (Task 15a), EXPORTED, so it can be shared
// with the refined `event consume` startup chain's PlanRemoteSubscription
// stage (Task 15b) instead of staying package-private here. The
// aliases/thin wrapper below keep this package's own names
// (reconcilePlan/planAction*/reconcileExisting) so create_test.go and this
// file's own createOrReuseSubscription/outcomeFromPlan/buildDryRunResult
// below are unchanged — only the underlying implementation moved.

// createSubscriptionAPI is the subset of *eventlib.SubscriptionClient this
// command calls: List (to reconcile against the unique key event_type +
// target_resource + authority before ever writing) and Create. It is the
// test seam — see listSubscriptionsAPI (list.go) for the rationale; tests
// substitute a fake implementing just these two methods, so every branch
// below is exercised without a real *lark.Client or network call. It is
// structurally identical to eventlib.SubscriptionCreateAPI (defined
// alongside the reconcile logic this now calls into) — kept as its own
// declaration here, rather than an alias, so this file's exported-to-tests
// shape doesn't change.
type createSubscriptionAPI interface {
	List(ctx context.Context, req *larkeventv1.ListSubscriptionReq) (*larkeventv1.ListSubscriptionResp, error)
	Create(ctx context.Context, req *larkeventv1.CreateSubscriptionReq) (*larkeventv1.CreateSubscriptionResp, error)
}

// reconcilePlan is a local alias for eventlib.ReconcilePlan — see the block
// doc comment above.
type reconcilePlan = eventlib.ReconcilePlan

const (
	planActionCreate    = eventlib.PlanActionCreate
	planActionReuse     = eventlib.PlanActionReuse
	planActionConflict  = eventlib.PlanActionConflict
	planActionSuspended = eventlib.PlanActionSuspended
)

// reconcileExisting forwards to eventlib.ReconcileExisting — see the block
// doc comment above for why this thin wrapper exists instead of every call
// site here naming eventlib.ReconcileExisting directly. See
// eventlib.ReconcileExisting's own doc comment for the full state-table and
// authority-matching rationale (unchanged by the move).
func reconcileExisting(ctx context.Context, svc createSubscriptionAPI, eventType, targetResource string, identity core.Identity, requestedIncludeResourceData bool) (*reconcilePlan, error) {
	return eventlib.ReconcileExisting(ctx, svc, eventType, targetResource, identity, requestedIncludeResourceData)
}

// createOutcome is the result of a completed (non-dry-run) create request:
// either a freshly created Subscription, or an idempotently reused existing
// one — the two cases createOrReuseSubscription can return without error.
// Every other reconcilePlan.Action (conflict/suspended) becomes a typed
// error instead (spec §3.3), never a createOutcome.
type createOutcome struct {
	Action string // "created" | "reused"
	Detail *larkeventv1.SubscriptionDetail
}

// createOrReuseSubscription is the write-capable half of §3.3/§4.2: it
// reconciles against remote state, then acts on the plan — Create when
// nothing blocks it, idempotent reuse when a compatible active match
// exists, or a typed failure (never a write) for conflict/suspended.
//
// If Create itself fails, this reconciles once more via a fresh List (spec
// §3.3: "Create 遇 duplicate 或 transport timeout：重新 List 对账") before
// giving up — a single bounded pass, not a retry loop: List-before-Create is
// an optimization, not a substitute for the server's own unique-key
// enforcement. If the second reconcile still finds nothing blocking (or
// itself fails), the ORIGINAL Create error is what is returned — it is never
// swallowed in favor of a less informative one.
func createOrReuseSubscription(ctx context.Context, svc createSubscriptionAPI, resolved eventlib.ResolvedEventKey, identity core.Identity, includeResourceData bool) (*createOutcome, error) {
	eventType := resolved.Definition.EventType
	targetResource := resolved.TargetResource

	plan, err := reconcileExisting(ctx, svc, eventType, targetResource, identity, includeResourceData)
	if err != nil {
		return nil, err
	}
	if plan.Action != planActionCreate {
		return outcomeFromPlan(plan, resolved, identity)
	}

	detail, createErr := doCreateSubscription(ctx, svc, eventType, targetResource, includeResourceData)
	if createErr == nil {
		return &createOutcome{Action: "created", Detail: detail}, nil
	}

	plan2, listErr := reconcileExisting(ctx, svc, eventType, targetResource, identity, includeResourceData)
	if listErr != nil || plan2.Action == planActionCreate {
		// The reconcile-after-failure found nothing new (or itself failed):
		// the original Create error is the most useful thing to surface.
		return nil, createErr
	}
	return outcomeFromPlan(plan2, resolved, identity)
}

// outcomeFromPlan converts a non-"create" plan into either a success
// (idempotent reuse) or a typed error (conflict/suspended) — shared by both
// the first reconcile and the post-Create-failure reconcile in
// createOrReuseSubscription.
func outcomeFromPlan(plan *reconcilePlan, resolved eventlib.ResolvedEventKey, identity core.Identity) (*createOutcome, error) {
	switch plan.Action {
	case planActionReuse:
		return &createOutcome{Action: "reused", Detail: plan.Existing}, nil
	case planActionConflict:
		return nil, conflictError(resolved, identity, plan)
	case planActionSuspended:
		return nil, suspendedError(resolved, identity, plan)
	default:
		return nil, errs.NewInternalError(errs.SubtypeUnknown, "unexpected reconcile plan action %q", plan.Action)
	}
}

// doCreateSubscription issues the actual Create call and unwraps its
// response. Any error svc.Create returns (transport or already-classified
// typed business failure — Task 7's SubscriptionClient.Create) is passed
// through unchanged; the caller (createOrReuseSubscription) decides whether
// to reconcile-and-retry.
func doCreateSubscription(ctx context.Context, svc createSubscriptionAPI, eventType, targetResource string, includeResourceData bool) (*larkeventv1.SubscriptionDetail, error) {
	body := larkeventv1.NewCreateSubscriptionReqBodyBuilder().
		EventType(eventType).
		TargetResource(targetResource).
		PayloadOptions(larkeventv1.NewCreatePayloadOptionsBuilder().IncludeResourceData(includeResourceData).Build()).
		Build()
	req := larkeventv1.NewCreateSubscriptionReqBuilder().Body(body).Build()

	resp, err := svc.Create(ctx, req)
	if err != nil {
		return nil, err
	}
	var detail *larkeventv1.SubscriptionDetail
	if resp != nil && resp.Data != nil {
		detail = resp.Data.Subscription
	}
	if detail == nil {
		return nil, errs.NewInternalError(errs.SubtypeInvalidResponse,
			"subscription create reported success but returned no subscription data")
	}
	return detail, nil
}

// conflictError implements spec §3.3's "active but conflicting" row and
// §3.6's error-contract mapping: typed failed_precondition, Param the
// EventKey positional argument, conflicting fields in Params[].Reason, and
// the existing remote_subscription_id plus a guide-to-`get` in Hint.
func conflictError(resolved eventlib.ResolvedEventKey, identity core.Identity, plan *reconcilePlan) error {
	id := strVal(plan.Existing.SubscriptionId)
	return errs.NewValidationError(errs.SubtypeFailedPrecondition,
		"an active subscription already exists for %s with a conflicting configuration (remote_subscription_id=%s)",
		resolved.MaterializedKey, id).
		WithParam("event_key").
		WithParams(plan.ConflictFields...).
		WithHint("run `lark-cli event subscription get %s --as %s --json` to inspect remote_subscription_id=%s, then either accept its existing configuration or delete it before creating a differently-configured one", id, identity, id)
}

// suspendedError implements spec §3.3's "suspended" row: do not overwrite;
// guide the caller to `reactivate` instead of creating a duplicate.
func suspendedError(resolved eventlib.ResolvedEventKey, identity core.Identity, plan *reconcilePlan) error {
	id := strVal(plan.Existing.SubscriptionId)
	reason := ""
	if plan.Existing.Suspension != nil {
		reason = strVal(plan.Existing.Suspension.Code)
	}
	return errs.NewValidationError(errs.SubtypeFailedPrecondition,
		"a suspended subscription already exists for %s (remote_subscription_id=%s, suspension_reason=%s); it is not automatically overwritten",
		resolved.MaterializedKey, id, reason).
		WithParam("event_key").
		WithHint("run `lark-cli event subscription reactivate %s --as %s` to resume delivery instead of creating a duplicate", id, identity)
}

// ---- JSON output shapes ----

// createDryRunResult is `event subscription create --dry-run`'s JSON shape
// (design spec §3.4). Populated purely from parse/identity/scope preflight
// (already done by the time runCreate reaches this) plus one remote List —
// never a Create.
type createDryRunResult struct {
	Operation      string           `json:"operation"`
	DryRun         bool             `json:"dry_run"`
	EventKey       string           `json:"event_key"`
	TargetResource string           `json:"target_resource"`
	RequiredScopes []string         `json:"required_scopes"`
	Preflight      createPreflight  `json:"preflight"`
	RemoteBefore   *subscriptionRow `json:"remote_before"`
	PlannedChange  plannedChange    `json:"planned_change"`
	LocalImpact    localImpact      `json:"local_impact"`
	NextAction     string           `json:"next_action"`
}

// createPreflight summarizes the (already-passed, by the time this is
// built) identity/template/scope checks §3.4 requires dry-run to perform.
type createPreflight struct {
	Identity        string `json:"identity"`
	MatchedTemplate string `json:"matched_template"`
	ScopesOK        bool   `json:"scopes_ok"`
}

// plannedChange describes what a real (non-dry-run) run would do, per
// reconcilePlan — including outcomes that would themselves be a typed
// failure (conflict/suspended): dry-run always reports the plan
// informationally rather than failing on it (only the preflight steps
// themselves — identity/template/scope — are real dry-run failures).
type plannedChange struct {
	Action               string              `json:"action"`
	RemoteSubscriptionID string              `json:"remote_subscription_id,omitempty"`
	ConflictFields       []errs.InvalidParam `json:"conflict_fields,omitempty"`
}

// localImpact documents that `subscription create` never touches a local
// `event consume` process — only the remote management plane.
type localImpact struct {
	LocalConsumerAffected bool   `json:"local_consumer_affected"`
	Note                  string `json:"note"`
}

func buildDryRunResult(resolved eventlib.ResolvedEventKey, identity core.Identity, plan *reconcilePlan) *createDryRunResult {
	var remoteBefore *subscriptionRow
	if plan.Existing != nil {
		row := mapSubscriptionDetail(plan.Existing)
		remoteBefore = &row
	}
	pc := plannedChange{Action: plan.Action, ConflictFields: plan.ConflictFields}
	if plan.Existing != nil {
		pc.RemoteSubscriptionID = strVal(plan.Existing.SubscriptionId)
	}

	return &createDryRunResult{
		Operation:      "create",
		DryRun:         true,
		EventKey:       resolved.MaterializedKey,
		TargetResource: resolved.TargetResource,
		RequiredScopes: subscriptionMutationScopes,
		Preflight: createPreflight{
			Identity:        string(identity),
			MatchedTemplate: resolved.Template.Template,
			ScopesOK:        true, // reaching this point already passed the scope preflight
		},
		RemoteBefore:  remoteBefore,
		PlannedChange: pc,
		LocalImpact: localImpact{
			LocalConsumerAffected: false,
			Note:                  "`event subscription create` never starts or changes a local `event consume` process; run `event consume` separately after create succeeds.",
		},
		NextAction: dryRunNextAction(plan, resolved, identity),
	}
}

func dryRunNextAction(plan *reconcilePlan, resolved eventlib.ResolvedEventKey, identity core.Identity) string {
	switch plan.Action {
	case planActionReuse:
		return fmt.Sprintf("run without --dry-run to idempotently reuse remote_subscription_id=%s, then `lark-cli event consume %s --as %s`", strVal(plan.Existing.SubscriptionId), resolved.MaterializedKey, identity)
	case planActionConflict:
		id := strVal(plan.Existing.SubscriptionId)
		return fmt.Sprintf("run `lark-cli event subscription get %s --as %s --json` to inspect the conflicting remote_subscription_id=%s before deciding how to proceed", id, identity, id)
	case planActionSuspended:
		id := strVal(plan.Existing.SubscriptionId)
		return fmt.Sprintf("run `lark-cli event subscription reactivate %s --as %s` instead of creating a duplicate", id, identity)
	default: // planActionCreate
		return fmt.Sprintf("run without --dry-run to create the subscription, then `lark-cli event consume %s --as %s`", resolved.MaterializedKey, identity)
	}
}

// createResult is `event subscription create`'s (non-dry-run) JSON shape on
// success — either a freshly created or an idempotently reused Subscription
// (spec §3.3; conflict/suspended never reach here, they return a typed error
// instead).
type createResult struct {
	Operation            string          `json:"operation"`
	Action               string          `json:"action"` // "created" | "reused"
	RemoteSubscriptionID string          `json:"remote_subscription_id"`
	Subscription         subscriptionRow `json:"subscription"`
	NextAction           string          `json:"next_action"`
}

func buildCreateResult(resolved eventlib.ResolvedEventKey, identity core.Identity, outcome *createOutcome) *createResult {
	row := mapSubscriptionDetail(outcome.Detail)
	return &createResult{
		Operation:            "create",
		Action:               outcome.Action,
		RemoteSubscriptionID: row.RemoteSubscriptionID,
		Subscription:         row,
		NextAction:           createNextAction(resolved, identity),
	}
}

// createNextAction implements spec §3.3's "creation succeeding does not
// mean listening started" guidance.
func createNextAction(resolved eventlib.ResolvedEventKey, identity core.Identity) string {
	return fmt.Sprintf("run `lark-cli event consume %s --as %s` to start receiving these events", resolved.MaterializedKey, identity)
}

func writeCreateDryRunText(out io.Writer, result *createDryRunResult) {
	fmt.Fprintf(out, "[dry-run] planned change: %s\n", result.PlannedChange.Action)
	fmt.Fprintf(out, "Event Key:       %s\n", result.EventKey)
	fmt.Fprintf(out, "Target Resource: %s\n", result.TargetResource)
	if result.RemoteBefore != nil {
		fmt.Fprintf(out, "Existing remote state: %s (remote_subscription_id=%s)\n", result.RemoteBefore.Remote.State, result.RemoteBefore.RemoteSubscriptionID)
	} else {
		fmt.Fprintln(out, "Existing remote state: none found")
	}
	fmt.Fprintf(out, "Next: %s\n", result.NextAction)
}

func writeCreateText(out io.Writer, result *createResult) {
	verb := "Created"
	if result.Action == "reused" {
		verb = "Reused existing"
	}
	fmt.Fprintf(out, "%s remote Subscription %s\n", verb, result.RemoteSubscriptionID)
	fmt.Fprintf(out, "Event Key:       %s\n", result.Subscription.EventKey)
	if result.Subscription.TargetResource != "" {
		fmt.Fprintf(out, "Target Resource: %s\n", result.Subscription.TargetResource)
	}
	fmt.Fprintf(out, "Next: %s\n", result.NextAction)
}
