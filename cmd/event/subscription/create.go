// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	eventlib "github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/app"
	larkgw "github.com/larksuite/cli/internal/event/platform/lark"
	subown "github.com/larksuite/cli/internal/event/subscription"
	"github.com/larksuite/cli/internal/output"
)

// subscriptionMutationScopes are the scopes hard-required by every mutating
// subscription subcommand (create/update/renew/reactivate/delete):
// unlike list/get, which only ever read, every mutating command
// always reads remote state first (List/Get, for idempotency/conflict/impact
// analysis) before it may write, so it needs read AND write simultaneously —
// neither implies the other. Declared once here so later sibling commands
// (update/renew/reactivate/delete) can reuse it without duplicating the list.
var subscriptionMutationScopes = []string{"event:subscription:read", "event:subscription:write"}

// createOpts holds `event subscription create`'s flag values.
type createOpts struct {
	includeResourceData bool
	dryRun              bool
	asJSON              bool
	filter              string
	// scopesVerified is NOT a flag: it is the resolved scope-preflight state
	// (resolveUATAndCheckScopes's second return) that runCreate stamps on before
	// calling applyCreate, so the --dry-run preview reports scopes_ok honestly
	// ("verified" vs "unknown") instead of a hardcoded green light. Defaults to
	// false (unknown) — the correct value in tests that never ran a real check.
	scopesVerified bool
}

// NewCmdCreate builds `event subscription create <refined EventKey>`.
// Unlike list/get, create additionally: resolves the EventKey via
// eventlib.ResolveEventKey (target_resource/event_type), enforces the
// matched KeyTemplate's own AuthTypes on top of the usual identity checks,
// requires BOTH event:subscription:read and
// event:subscription:write — plus event:encrypt_key:read when
// --include-resource-data=true — creates an ENCRYPTED subscription for
// --include-resource-data=true
// (a fresh CSPRNG-generated per-subscription encrypt_key, submitted
// atomically with the Create request by the subscription Controller), and
// reconciles against remote state before ever writing:
// not-exist -> create, active+compatible -> idempotent reuse,
// active+conflicting (including the encryption dimension) -> typed
// failed_precondition, suspended -> guide reactivate. It never exposes
// --yes: create is additive and pre-checked for conflicts, so it is not a
// high-risk confirmation-gated action — that is reserved for delete (update
// changes only a reversible filter and is not confirmation-gated either).
func NewCmdCreate(f *cmdutil.Factory) *cobra.Command {
	var o createOpts
	cmd := &cobra.Command{
		Use:   "create <refined EventKey>",
		Short: "Create (or idempotently reuse) a remote event Subscription",
		Long: `Create a remote Subscription for a materialized refined EventKey.

A bare refined base key (no template segment) is rejected — pass a
materialized key such as one of the key_templates[].example values from
'event schema <base> --json'.

IDENTITY: --as user|bot|auto. For identity requirements, strictly adhere to 
the KeyTemplate. If the KeyTemplate does not have identity requirements, 
then adhere to the BaseKey requirements. A mismatch is a typed error naming 
the allowed identities; it never silently falls back to another identity.

SCOPE: requires BOTH event:subscription:read and event:subscription:write;
--include-resource-data=true ALSO requires --as user and event:encrypt_key:read
(resource data is user-only). Every mutation always reads remote state first
(to detect an existing subscription and analyze impact) before it may create
anything.

OUTPUT: {operation, action: "created"|"reused", remote_subscription_id,
subscription{...}, next_action}. A conflicting active subscription
(different payload_options) or a suspended one is never silently overwritten
— both return a typed
failed_precondition guiding you to 'get' or 'reactivate' instead.

NEXT STEP: creating a subscription does not start listening — run 'event
consume <refined EventKey>' afterwards.

SAFETY: --include-resource-data=true includes resource data in delivered events. 
Resource data is delivered encrypted by the platform and decrypted by the CLI 
before output; users and agents do not manage keys or decryption. 
Use --dry-run to preview the plan (parse/identity/scope preflight + a remote 
read + impact analysis) without creating, reusing, or changing anything; 
create never requires --yes (additive and pre-checked for conflicts).`,

		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCreate(cmd, f, args[0], o)
		},
	}

	cmd.Flags().BoolVar(&o.includeResourceData, "include-resource-data", false,
		"This flag requires a refined key and must be user-only; legacy key or --as bot/auto→bot will be rejected. Required scope: event:encrypt_key:read")
	cmd.Flags().BoolVar(&o.dryRun, "dry-run", false,
		"Preview the plan (identity/scope preflight + remote read + impact analysis) without creating, reusing, or changing anything")
	cmd.Flags().BoolVar(&o.asJSON, "json", false, "Emit the result as JSON (for AI / scripts)")
	cmd.Flags().StringVar(&o.filter, "filter", "",
		"Inline `json` event filter to apply server-side; validated against this event type's filter schema (see 'event schema <key> --json'). Omit for no filter.")
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
	// (EventKey resolution, template identity) that must be able to reject a
	// request before doing ANY other work, including a config lookup.
	resolved, err := eventlib.ResolveEventKey(eventKeyArg)
	if err != nil {
		// Passed through unchanged: a bare refined base key, an unknown
		// base, a legacy key with a suffix, a bad template segment/value,
		// etc. are all already typed errors — this command must not
		// re-wrap or reword them.
		return err
	}
	if !resolved.IsRefined {
		// A legacy (non-refined) key has no Template/TargetResource — there
		// is nothing for this command's reconciliation to key off, and no
		// remote Subscription resource concept applies to it at all — `create`
		// is defined exclusively for a refined EventKey.
		return errCreateRequiresRefinedKey(eventKeyArg)
	}

	// Validate --filter against this event type's filter capability. Empty
	// input is "no filter". A parse/validation failure is already a typed
	// invalid_argument on --filter — returned unchanged, before identity/scope
	// or any remote call, alongside the other cheap local rejections above.
	reqFilter, err := eventlib.ParseAndValidateFilter(o.filter, eventlib.FilterMetaFor(resolved.Definition.EventType))
	if err != nil {
		return err
	}

	identity, err := resolveEffectiveIdentity(cmd, f)
	if err != nil {
		return err
	}
	// Tier 1: the identity must be one the whole refined base
	// key accepts at all — mirrors cmd/event/consume.go's resolveIdentity.
	// Empty AuthTypes means "no restriction" (KeyDefinition.AuthTypes' own
	// doc comment), so guard the call the same way consume.go does: calling
	// f.CheckIdentity with an empty supported list would reject everything.
	if len(resolved.Definition.AuthTypes) > 0 {
		if err := f.CheckIdentity(identity, resolved.Definition.AuthTypes); err != nil {
			return err
		}
	}
	// Tier 2 (stricter — this is what list/get do not need,
	// since they carry no EventKey/KeyTemplate context at all): the specific
	// matched KeyTemplate can be narrower than the key itself (e.g.
	// "owner/me" is user-only even though the key allows user+bot). A
	// key-level pass does not imply a template-level pass.
	if err := checkTemplateAuthTypes(identity, resolved); err != nil {
		return err
	}

	// include_resource_data is a user-only platform capability: a bot/app
	// subscription cannot carry it. Reject before any remote call (and before
	// generating a key) so an encrypted bot subscription is never attempted.
	if o.includeResourceData && identity != core.AsUser {
		return errIncludeResourceDataRequiresUser(identity)
	}

	cfg, err := f.Config()
	if err != nil {
		return err
	}
	uat, scopesVerified, err := resolveUATAndCheckScopes(ctx, f, cfg.AppID, identity, createRequiredScopes(o.includeResourceData))
	if err != nil {
		return err
	}
	// Carry the honest scope-preflight state into the dry-run preview.
	o.scopesVerified = scopesVerified

	sdk, err := f.LarkClient()
	if err != nil {
		return err
	}
	gateway, err := larkgw.NewSubscriptionGateway(sdk, identity, uat)
	if err != nil {
		return err
	}

	cmdCtx := eventlib.CommandContext{Profile: cfg.ProfileName, Identity: identity}
	return applyCreate(ctx, subown.NewController(gateway), f.IOStreams.Out, resolved, cmdCtx, o, reqFilter)
}

// createController is the subset of the subscription Controller create's flow
// drives: Plan (the read-only Observe+classify a --dry-run renders and a real
// run acts on) and Apply (the single remote write). It is
// app.SubscriptionController — the shared provisioning seam the use case owns —
// aliased here so create_test.go keeps substituting a fake Controller (or a real
// one over a fake gateway) with no *lark.Client or network call.
type createController = app.SubscriptionController

// applyCreate is create's testable core: it builds the subscription Request and
// hands it to the SubscriptionUseCase's Observe→Plan→Apply provisioning flow,
// then renders the domain outcome — the --dry-run preview, the created/reused
// result, or the typed conflict/suspended/indeterminate error a non-writable
// plan maps to. It never itself calls controller.Plan/Apply, handles the
// reconcile-after-failure matrix, builds a request body, or calls the SDK — the
// use case, the Controller, and the platform/lark gateway own all of that; this
// function only builds the request and renders.
func applyCreate(ctx context.Context, controller createController, out io.Writer, resolved eventlib.ResolvedEventKey, cmdCtx eventlib.CommandContext, o createOpts, reqFilter *eventlib.Filter) error {
	req := subown.Request{
		EventType:           resolved.Definition.EventType,
		TargetResource:      resolved.TargetResource,
		Identity:            cmdCtx.Identity,
		IncludeResourceData: o.includeResourceData,
		Filter:              reqFilter,
	}

	outcome, err := app.NewSubscriptionUseCase().Provision(ctx, controller, subown.ManagementCreate, req, o.dryRun)
	if err != nil {
		return err
	}

	switch outcome.Kind {
	case app.ProvisionPreview:
		result := buildDryRunResult(resolved, cmdCtx, outcome.Plan, o.includeResourceData, o.scopesVerified)
		if o.asJSON {
			output.PrintJson(out, result)
			return nil
		}
		writeCreateDryRunText(out, result)
		return nil
	case app.ProvisionBlocked:
		// A block carrying conflict fields is a configuration conflict; a block
		// with none is a suspended match create refuses to overwrite. blockError
		// renders the right typed failed_precondition from the plan.
		return blockError(resolved, cmdCtx, outcome.Plan, o.includeResourceData)
	case app.ProvisionIndeterminate:
		return indeterminateError(resolved, cmdCtx)
	case app.ProvisionApplied:
		result := buildCreateResult(resolved, cmdCtx, outcome.Receipt)
		if o.asJSON {
			output.PrintJson(out, result)
			return nil
		}
		writeCreateText(out, result)
		return nil
	default:
		return errs.NewInternalError(errs.SubtypeUnknown, "unexpected subscription provision outcome %d", outcome.Kind)
	}
}

// checkTemplateAuthTypes enforces the second, stricter
// identity tier for a refined EventKey with a matched KeyTemplate — see
// eventlib.CheckTemplateAuthTypes's own doc comment for the full
// rationale/state (empty = no restriction, violated = typed
// failed_precondition naming the allowed identities). The check itself
// moved to internal/event so it is shared,
// byte-identical, with the refined `event consume` startup chain
// (cmd/event/consume.go's runRefinedConsume), which needs the exact same
// tier-2 gate before its own remote write. This thin wrapper keeps this
// package's own name (create_test.go calls it directly) — behavior is
// unchanged, only the implementation moved.
func checkTemplateAuthTypes(identity core.Identity, resolved eventlib.ResolvedEventKey) error {
	return eventlib.CheckTemplateAuthTypes(identity, resolved)
}

// errIncludeResourceDataRequiresUser rejects --include-resource-data=true on a
// non-user identity: resource data is a user-only platform capability, so a
// bot/app subscription cannot carry it. Typed invalid_argument (a caller
// mistake, not a transient state) fired before any remote call.
func errIncludeResourceDataRequiresUser(identity core.Identity) error {
	return errs.NewValidationError(errs.SubtypeInvalidArgument,
		"include_resource_data requires --as user: resource data is only supported for a user subscription, not %s", identity).
		WithParam("--include-resource-data").
		WithHint("re-run with --as user, or drop --include-resource-data to create a subscription without resource data")
}

// errCreateRequiresRefinedKey rejects a syntactically valid, registered
// EventKey that resolved as legacy (non-refined): `subscription create` only
// operates on the remote Subscription resource, which only a materialized
// refined key maps onto.
func errCreateRequiresRefinedKey(eventKeyArg string) error {
	return errs.NewValidationError(errs.SubtypeInvalidArgument,
		"EventKey %q is not a refined-subscription key; `event subscription create` only accepts a materialized refined EventKey", eventKeyArg).
		WithParam("event_key").
		WithHint("run `lark-cli event schema %s --json` to check whether this key supports refined subscriptions and, if so, see key_templates[].example for a key to pass here", eventKeyArg)
}

// createRequiredScopes returns the scopes create's preflight
// (resolveUATAndCheckScopes) must check for this request: the usual
// subscriptionMutationScopes, plus event:encrypt_key:read when
// includeResourceData is true (create's ManagementCreate policy probes
// GetEncryptKey to classify an existing include_resource_data=true match, which
// needs that scope). Always builds a fresh slice rather than `append`-ing onto
// subscriptionMutationScopes directly — that package-level var is shared by
// dry-run's default scope reporting and any future sibling command, so mutating
// (or risking an aliasing reallocation into) its backing array here would be a
// subtle, hard-to-spot bug.
func createRequiredScopes(includeResourceData bool) []string {
	if !includeResourceData {
		return subscriptionMutationScopes
	}
	scopes := make([]string, 0, len(subscriptionMutationScopes)+len(subscriptionEncryptKeyReadScopes))
	scopes = append(scopes, subscriptionMutationScopes...)
	scopes = append(scopes, subscriptionEncryptKeyReadScopes...)
	return scopes
}

// ---- typed errors for non-writable plans ----

// blockError maps an ActionBlock plan to create's typed failed_precondition. A
// block carrying the "state" dimension is a match in an unrecognized remote
// state (fail-closed); a block carrying include_resource_data/filter fields is a
// configuration conflict (the active/suspended match disagrees); a block with no
// conflict fields is a suspended match create refuses to overwrite. Each renders
// its own guidance (inspect vs get/delete vs reactivate).
func blockError(resolved eventlib.ResolvedEventKey, cmdCtx eventlib.CommandContext, plan subown.SubscriptionPlan, includeResourceData bool) error {
	if subown.ConflictOnState(plan.ConflictFields) {
		return unknownStateError(resolved, cmdCtx, plan)
	}
	if len(plan.ConflictFields) > 0 {
		return conflictError(resolved, cmdCtx, plan, includeResourceData)
	}
	return suspendedError(resolved, cmdCtx, plan)
}

// unknownStateError implements the "unrecognized remote state" block: a match
// exists but its state is not one this CLI can classify (not active/suspended/
// expired/deleted), so create fails closed rather than risk duplicating a
// still-live subscription. It guides the human to inspect and decide.
func unknownStateError(resolved eventlib.ResolvedEventKey, cmdCtx eventlib.CommandContext, plan subown.SubscriptionPlan) error {
	id := planBeforeID(plan)
	state := ""
	if plan.Before != nil {
		state = plan.Before.State
	}
	return errs.NewValidationError(errs.SubtypeFailedPrecondition,
		"a subscription already exists for %s in an unrecognized remote state %q (remote_subscription_id=%s); create fails closed rather than risk duplicating it",
		resolved.MaterializedKey, state, id).
		WithParam("event_key").
		WithParams(plan.ConflictFields...).
		WithHint("run `%s event subscription get %s --as %s --json` to inspect remote_subscription_id=%s and its state, then reactivate, delete, or wait as appropriate before re-running create", cmdCtx.CLIHead(), id, cmdCtx.Identity, id)
}

// conflictError implements the "active but conflicting" case and
// its error-contract mapping: typed failed_precondition, Param the
// EventKey positional argument, conflicting fields in Params[].Reason, and
// the existing remote_subscription_id plus a guide-to-`get` in Hint.
//
// includeResourceData selects an encryption-aware Hint: encryption cannot be
// switched in place (Encrypt is Create-only), so
// an encrypted request's conflict is only ever resolved by a human — verify
// the existing subscription (and this identity's event:encrypt_key:read
// scope), or delete and recreate.
func conflictError(resolved eventlib.ResolvedEventKey, cmdCtx eventlib.CommandContext, plan subown.SubscriptionPlan, includeResourceData bool) error {
	id := planBeforeID(plan)
	head := cmdCtx.CLIHead()
	var hint string
	switch {
	case subown.ConflictOnFilter(plan.ConflictFields):
		// A filter difference is resolvable in place — change it with `update`
		// rather than deleting a possibly-shared subscription. Guide inspect ->
		// preview -> apply -> re-run; the filter values are never named here.
		hint = fmt.Sprintf("run `%s event subscription get %s --as %s --json` to inspect remote_subscription_id=%s, preview the change with `%s event subscription update %s --filter <json> --dry-run --as %s`, apply it with `%s event subscription update %s --filter <json> --as %s`, then re-run create to reuse the now-matching subscription — a filter change is reversible, so there is no need to delete a possibly-shared subscription", head, id, cmdCtx.Identity, id, head, id, cmdCtx.Identity, head, id, cmdCtx.Identity)
	case includeResourceData:
		hint = fmt.Sprintf("run `%s event subscription get %s --as %s --json` to inspect remote_subscription_id=%s; include_resource_data cannot be changed in place, so after human confirmation either keep the existing subscription, ensure this identity has scope `event:encrypt_key:read`, or delete it and create a new one with the desired resource-data setting", head, id, cmdCtx.Identity, id)
	default:
		hint = fmt.Sprintf("run `%s event subscription get %s --as %s --json` to inspect remote_subscription_id=%s, then either accept its existing configuration or delete it before creating a differently-configured one", head, id, cmdCtx.Identity, id)
	}
	return errs.NewValidationError(errs.SubtypeFailedPrecondition,
		"an active subscription already exists for %s with a conflicting configuration (remote_subscription_id=%s)",
		resolved.MaterializedKey, id).
		WithParam("event_key").
		WithParams(plan.ConflictFields...).
		WithHint("%s", hint)
}

// suspendedError implements the "suspended" case: do not overwrite;
// guide the caller to `reactivate` instead of creating a duplicate.
func suspendedError(resolved eventlib.ResolvedEventKey, cmdCtx eventlib.CommandContext, plan subown.SubscriptionPlan) error {
	id := planBeforeID(plan)
	reason := ""
	if plan.Before != nil {
		reason = plan.Before.SuspensionReason
	}
	return errs.NewValidationError(errs.SubtypeFailedPrecondition,
		"a suspended subscription already exists for %s (remote_subscription_id=%s, suspension_reason=%s); it is not automatically overwritten",
		resolved.MaterializedKey, id, reason).
		WithParam("event_key").
		WithHint("run `%s event subscription reactivate %s --as %s` to resume delivery instead of creating a duplicate", cmdCtx.CLIHead(), id, cmdCtx.Identity)
}

// indeterminateError implements the "the remote scan was inconclusive" case (the
// paginated List hit the page cap without a definitive answer). Rather than
// silently creating — which risks a duplicate against a match beyond the pages
// read — create fails closed with actionable guidance.
func indeterminateError(resolved eventlib.ResolvedEventKey, cmdCtx eventlib.CommandContext) error {
	return errs.NewValidationError(errs.SubtypeFailedPrecondition,
		"could not determine whether a matching subscription already exists for %s: the remote subscription list scan reached its %d-page cap before finding a match or exhausting all results, so creating now could duplicate an existing subscription",
		resolved.MaterializedKey, eventlib.MaxSubscriptionListPages).
		WithParam("event_key").
		WithHint("re-run `%s event subscription list --as %s --json` to inspect existing subscriptions (the scan was NOT confirmed absence, only \"no match within the pages read\"); if none matches, retry create", cmdCtx.CLIHead(), cmdCtx.Identity)
}

// planBeforeID renders the existing match's remote_subscription_id, or "" when a
// plan carries no Before.
func planBeforeID(plan subown.SubscriptionPlan) string {
	if plan.Before == nil {
		return ""
	}
	return plan.Before.ID.String()
}

// ---- JSON output shapes ----

// createDryRunResult is `event subscription create --dry-run`'s JSON shape.
// Populated purely from parse/identity/scope preflight
// (already done by the time applyCreate reaches this) plus one remote List —
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
// built) identity/template/scope checks dry-run performs.
type createPreflight struct {
	Identity        string `json:"identity"`
	MatchedTemplate string `json:"matched_template"`
	// ScopesOK stays a bool (backward-compatible for existing scripts / typed
	// decoders): true ONLY when the local scope pre-check actually confirmed the
	// required scopes, false when it could not be determined — never a green
	// light we did not verify. ScopeStatus is the additive nuance ("verified" vs
	// "unknown"). See scopeStateLabel.
	ScopesOK    bool   `json:"scopes_ok"`
	ScopeStatus string `json:"scope_status"`
}

// plannedChange describes what a real (non-dry-run) run would do, per
// the subscription plan — including outcomes that would themselves be a typed
// failure (conflict/suspended/indeterminate): dry-run always reports the plan
// informationally rather than failing on it (only the preflight steps
// themselves — identity/template/scope — are real dry-run failures).
type plannedChange struct {
	Action               string              `json:"action"`
	RemoteSubscriptionID string              `json:"remote_subscription_id,omitempty"`
	ConflictFields       []errs.InvalidParam `json:"conflict_fields,omitempty"`
}

// localImpact documents a mutation's effect on local `event consume`
// process(es). LocalConsumerAffected/Consumers are computed per command from a
// best-effort local-bus query (buslocal): update/delete populate Consumers with
// the REAL running consumers bound to this subscription; create/renew/
// reactivate never affect one, so they leave Consumers empty (omitted).
type localImpact struct {
	LocalConsumerAffected bool                `json:"local_consumer_affected"`
	Consumers             []localConsumerInfo `json:"consumers,omitempty"`
	Note                  string              `json:"note"`
}

// legacyPlanAction maps a subscription plan action to the stable
// planned_change.action vocabulary this command's JSON has always used
// ("create"/"reuse"/"conflict"/"suspended"), plus "indeterminate" for the
// inconclusive-scan outcome. A Block splits into "conflict" (a configuration
// conflict, carrying conflict fields) or "suspended" (a suspended match).
func legacyPlanAction(plan subown.SubscriptionPlan) string {
	switch plan.Action {
	case subown.ActionBlock:
		if len(plan.ConflictFields) > 0 {
			return "conflict"
		}
		return "suspended"
	case subown.ActionIndeterminate:
		return "indeterminate"
	default:
		return string(plan.Action)
	}
}

func buildDryRunResult(resolved eventlib.ResolvedEventKey, cmdCtx eventlib.CommandContext, plan subown.SubscriptionPlan, includeResourceData, scopesVerified bool) *createDryRunResult {
	var remoteBefore *subscriptionRow
	if plan.Before != nil {
		row := mapRemoteSubscription(*plan.Before)
		remoteBefore = &row
	}
	pc := plannedChange{Action: legacyPlanAction(plan), ConflictFields: plan.ConflictFields}
	if plan.Before != nil {
		pc.RemoteSubscriptionID = plan.Before.ID.String()
	}

	return &createDryRunResult{
		Operation:      "create",
		DryRun:         true,
		EventKey:       resolved.MaterializedKey,
		TargetResource: resolved.TargetResource,
		RequiredScopes: createRequiredScopes(includeResourceData),
		Preflight: createPreflight{
			Identity:        string(cmdCtx.Identity),
			MatchedTemplate: resolved.Template.Template,
			// scopes_ok true ONLY when the pre-check actually confirmed the scopes;
			// false when scope data was unavailable — never a false green light.
			// scope_status carries the "verified" vs "unknown" nuance additively.
			ScopesOK:    scopesVerified,
			ScopeStatus: scopeStateLabel(scopesVerified),
		},
		RemoteBefore:  remoteBefore,
		PlannedChange: pc,
		LocalImpact: localImpact{
			LocalConsumerAffected: false,
			Note:                  "`event subscription create` never starts or changes a local `event consume` process; run `event consume` separately after create succeeds.",
		},
		NextAction: dryRunNextAction(plan, resolved, cmdCtx),
	}
}

func dryRunNextAction(plan subown.SubscriptionPlan, resolved eventlib.ResolvedEventKey, cmdCtx eventlib.CommandContext) string {
	head := cmdCtx.CLIHead()
	switch plan.Action {
	case subown.ActionReuse:
		return fmt.Sprintf("run without --dry-run to idempotently reuse remote_subscription_id=%s, then `%s event consume %s --as %s`", planBeforeID(plan), head, resolved.MaterializedKey, cmdCtx.Identity)
	case subown.ActionBlock:
		id := planBeforeID(plan)
		if len(plan.ConflictFields) == 0 {
			return fmt.Sprintf("run `%s event subscription reactivate %s --as %s` instead of creating a duplicate", head, id, cmdCtx.Identity)
		}
		return fmt.Sprintf("run `%s event subscription get %s --as %s --json` to inspect the conflicting remote_subscription_id=%s before deciding how to proceed", head, id, cmdCtx.Identity, id)
	case subown.ActionIndeterminate:
		return fmt.Sprintf("run `%s event subscription list --as %s --json` to inspect existing subscriptions before running create — the scan reached its page cap without a definitive answer", head, cmdCtx.Identity)
	default: // ActionCreate
		return fmt.Sprintf("run without --dry-run to create the subscription, then `%s event consume %s --as %s`", head, resolved.MaterializedKey, cmdCtx.Identity)
	}
}

// createResult is `event subscription create`'s (non-dry-run) JSON shape on
// success — either a freshly created or an idempotently reused Subscription
// (conflict/suspended never reach here, they return a typed error
// instead).
type createResult struct {
	Operation            string          `json:"operation"`
	Action               string          `json:"action"` // "created" | "reused"
	RemoteSubscriptionID string          `json:"remote_subscription_id"`
	Subscription         subscriptionRow `json:"subscription"`
	NextAction           string          `json:"next_action"`
}

func buildCreateResult(resolved eventlib.ResolvedEventKey, cmdCtx eventlib.CommandContext, receipt subown.ApplyReceipt) *createResult {
	action := "created"
	if receipt.Action == subown.ActionReuse {
		action = "reused"
	}
	var row subscriptionRow
	if receipt.After != nil {
		row = mapRemoteSubscription(*receipt.After)
	}
	return &createResult{
		Operation:            "create",
		Action:               action,
		RemoteSubscriptionID: receipt.RemoteID.String(),
		Subscription:         row,
		NextAction:           createNextAction(resolved, cmdCtx),
	}
}

// createNextAction implements the "creation succeeding does not
// mean listening started" guidance.
func createNextAction(resolved eventlib.ResolvedEventKey, cmdCtx eventlib.CommandContext) string {
	return fmt.Sprintf("run `%s event consume %s --as %s` to start receiving these events", cmdCtx.CLIHead(), resolved.MaterializedKey, cmdCtx.Identity)
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
