// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	eventlib "github.com/larksuite/cli/internal/event"
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
// atomically with the Create request; see doCreateSubscription), and
// reconciles against remote state before ever writing:
// not-exist -> create, active+compatible -> idempotent reuse,
// active+conflicting (including the encryption dimension) -> typed
// failed_precondition, suspended -> guide reactivate. It never exposes
// --yes: create is additive and pre-checked for conflicts, so it is not a
// high-risk confirmation-gated action — that is reserved for
// update/delete.
func NewCmdCreate(f *cmdutil.Factory) *cobra.Command {
	var o createOpts
	cmd := &cobra.Command{
		Use:   "create <refined EventKey>",
		Short: "Create (or idempotently reuse) a remote event Subscription",
		Long: `Create a remote Subscription for a materialized refined EventKey (e.g.
'im.message.created_v1/chat-id/oc_xxx'), or idempotently reuse an existing
compatible one.

A bare refined base key (no template segment) is rejected — pass a
materialized key such as one of the key_templates[].example values from
'event schema <base> --json'.

IDENTITY: --as user|bot|auto. The resolved identity must be supported by the
EventKey's matched KeyTemplate SPECIFICALLY (not just the EventKey in
general) — e.g. the 'owner/me' template only supports --as user even though
its base key allows user+bot. A mismatch is a typed error naming the
allowed identities; it never silently falls back to another identity.

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

SAFETY: --include-resource-data=true includes resource data in delivered
events. Resource data is delivered encrypted by the platform and decrypted by
the CLI before output; users and agents do not manage keys or decryption. A
failed create never falls back to a subscription without resource data. An
existing remote subscription with a conflicting include_resource_data setting
is a typed failed_precondition (human decision: verify it, or delete and
recreate) — never silently resolved. Use --dry-run to preview the plan
(parse/identity/scope preflight + a remote read + impact analysis) without
creating, reusing, or changing anything; create never requires --yes
(additive and pre-checked for conflicts).`,
		Example: `  lark-cli event schema im.message.created_v1 --json                                          # find key_templates[].example first
  lark-cli event subscription create im.message.created_v1/chat-id/oc_xxx --dry-run --as bot --json
  lark-cli event subscription create im.message.created_v1/chat-id/oc_xxx --as bot --json
  lark-cli event subscription create im.message.created_v1/owner/me --as user --json               # fixed-value template, user only`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCreate(cmd, f, args[0], o)
		},
	}

	cmd.Flags().BoolVar(&o.includeResourceData, "include-resource-data", false,
		"Include resource data in delivered events. Requires --as user and scope event:encrypt_key:read; the platform delivers resource data encrypted and the CLI decrypts it before output.")
	cmd.Flags().BoolVar(&o.dryRun, "dry-run", false,
		"Preview the plan (identity/scope preflight + remote read + impact analysis) without creating, reusing, or changing anything")
	cmd.Flags().BoolVar(&o.asJSON, "json", false, "Emit the result as JSON (for AI / scripts)")
	cmd.Flags().StringVar(&o.filter, "filter", "",
		"Inline JSON event filter to apply server-side; validated against this event type's filter schema (see `event schema <key> --json`). Omit for no filter.")
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
	uat, err := resolveUATAndCheckScopes(ctx, f, cfg.AppID, identity, createRequiredScopes(o.includeResourceData))
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
		plan, err := reconcileExisting(ctx, client, resolved.Definition.EventType, resolved.TargetResource, identity, o.includeResourceData, reqFilter)
		if err != nil {
			return err
		}
		if plan.PaginationCapped {
			fmt.Fprintln(f.IOStreams.ErrOut, eventlib.PaginationCappedWarning(resolved.Definition.EventType, resolved.TargetResource))
		}
		result := buildDryRunResult(resolved, identity, plan, o.includeResourceData)
		if o.asJSON {
			output.PrintJson(f.IOStreams.Out, result)
			return nil
		}
		writeCreateDryRunText(f.IOStreams.Out, result)
		return nil
	}

	outcome, err := createOrReuseSubscription(ctx, client, resolved, identity, o.includeResourceData, reqFilter)
	if err != nil {
		return err
	}
	if outcome.PaginationCapped {
		fmt.Fprintln(f.IOStreams.ErrOut, eventlib.PaginationCappedWarning(resolved.Definition.EventType, resolved.TargetResource))
	}
	result := buildCreateResult(resolved, identity, outcome)
	if o.asJSON {
		output.PrintJson(f.IOStreams.Out, result)
		return nil
	}
	writeCreateText(f.IOStreams.Out, result)
	return nil
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
// includeResourceData is true (create's
// reconcile probes GetEncryptKey via WithEncryptKeyProber to classify an
// existing include_resource_data=true match, which needs that scope; see
// reconcileExisting below). Always builds a fresh slice rather than
// `append`-ing onto subscriptionMutationScopes directly — that package-level
// var is shared by dry-run's default scope reporting and any future sibling
// command, so mutating (or risking an aliasing reallocation into) its
// backing array here would be a subtle, hard-to-spot bug.
func createRequiredScopes(includeResourceData bool) []string {
	if !includeResourceData {
		return subscriptionMutationScopes
	}
	scopes := make([]string, 0, len(subscriptionMutationScopes)+len(subscriptionEncryptKeyReadScopes))
	scopes = append(scopes, subscriptionMutationScopes...)
	scopes = append(scopes, subscriptionEncryptKeyReadScopes...)
	return scopes
}

// ---- remote reconciliation ----
//
// The reconcile classification itself — the state table, authority
// matching, and the ReconcilePlan shape — moved to
// internal/event/reconcile.go, EXPORTED, so it can be shared
// with the refined `event consume` startup chain's PlanRemoteSubscription
// stage instead of staying package-private here. The
// aliases/thin wrapper below keep this package's own names
// (reconcilePlan/planAction*/reconcileExisting) so create_test.go and this
// file's own createOrReuseSubscription/outcomeFromPlan/buildDryRunResult
// below are unchanged — only the underlying implementation moved.

// createSubscriptionAPI is the subset of *eventlib.SubscriptionClient this
// command calls: List (to reconcile against the unique key event_type +
// target_resource + authority before ever writing), Create, and
// GetEncryptKey (the encryption conflict-matrix probe reconcileExisting
// wires in below for an encrypted request — eventlib.EncryptKeyProber's
// method, added here so the SAME svc value satisfies both
// eventlib.SubscriptionLister and eventlib.EncryptKeyProber without a
// separate adapter type). It is the test seam — see listSubscriptionsAPI
// (list.go) for the rationale; tests substitute a fake implementing these
// three methods, so every branch below is exercised without a real
// *lark.Client or network call. It is a strict superset of
// eventlib.SubscriptionCreateAPI (defined alongside the reconcile logic this
// now calls into) — kept as its own declaration here, rather than an alias,
// so this file's exported-to-tests shape doesn't change.
type createSubscriptionAPI interface {
	List(ctx context.Context, req *larkeventv1.ListSubscriptionReq) (*larkeventv1.ListSubscriptionResp, error)
	Create(ctx context.Context, req *larkeventv1.CreateSubscriptionReq) (*larkeventv1.CreateSubscriptionResp, error)
	GetEncryptKey(ctx context.Context, req *larkeventv1.GetEncryptKeySubscriptionReq) (*larkeventv1.GetEncryptKeySubscriptionResp, error)
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
//
// When requestedIncludeResourceData is true (the caller
// wants an ENCRYPTED subscription), this also supplies svc itself as the
// eventlib.WithEncryptKeyProber option — svc already satisfies
// eventlib.EncryptKeyProber structurally (createSubscriptionAPI declares the
// same GetEncryptKey method), so ReconcileExisting can resolve the
// encryption conflict matrix for an active, include_resource_data=true
// match. requestedIncludeResourceData=false takes the plaintext path with
// no encryption probe.
//
// requestedFilter is the requested server-side filter, forwarded as
// eventlib.WithRequestedFilter so a filter that differs from an existing active
// subscription's blocks reuse as a conflict; a nil/empty requestedFilter is
// compared as "no filter", so an unfiltered request against an unfiltered match
// still reuses.
func reconcileExisting(ctx context.Context, svc createSubscriptionAPI, eventType, targetResource string, identity core.Identity, requestedIncludeResourceData bool, requestedFilter *eventlib.Filter) (*reconcilePlan, error) {
	opts := []eventlib.ReconcileOption{eventlib.WithRequestedFilter(requestedFilter)}
	if requestedIncludeResourceData {
		opts = append(opts, eventlib.WithEncryptKeyProber(svc))
	}
	return eventlib.ReconcileExisting(ctx, svc, eventType, targetResource, identity, requestedIncludeResourceData, opts...)
}

// newEncryptKeyFunc generates a fresh per-subscription encrypt_key for an
// encrypted create. Indirected through a package-level var — rather than
// calling
// eventlib.NewEncryptKey directly at its one production call site
// (doCreateSubscription, via createOrReuseSubscription) — solely so tests
// can substitute a counting spy and assert this is invoked exactly zero
// times on the --dry-run path (a hard invariant: dry-run generates no
// key), a property that is otherwise awkward to prove directly. Production
// code must never reassign this outside tests.
var newEncryptKeyFunc = eventlib.NewEncryptKey

// createOutcome is the result of a completed (non-dry-run) create request:
// either a freshly created Subscription, or an idempotently reused existing
// one — the two cases createOrReuseSubscription can return without error.
// Every other reconcilePlan.Action (conflict/suspended) becomes a typed
// error instead, never a createOutcome.
type createOutcome struct {
	Action string // "created" | "reused"
	Detail *larkeventv1.SubscriptionDetail

	// PaginationCapped is true when the reconcile List scan that led to this
	// outcome hit the page cap before finding a match (see
	// eventlib.ReconcilePlan.PaginationCapped). This is only ever possible
	// alongside Action=="created" — a "reused" outcome always comes from an
	// early, uncapped match — and is not itself a failure: runCreate logs it
	// as an advisory rather than silently proceeding as if the scan had
	// confirmed no conflicting subscription exists.
	PaginationCapped bool
}

// createOrReuseSubscription is the write-capable half: it
// reconciles against remote state, then acts on the plan — Create when
// nothing blocks it, idempotent reuse when a compatible active match
// exists, or a typed failure (never a write) for conflict/suspended.
//
// If Create itself fails, this reconciles once more via a fresh List (on a
// duplicate or transport timeout, re-List to reconcile) before
// giving up — a single bounded pass, not a retry loop: List-before-Create is
// an optimization, not a substitute for the server's own unique-key
// enforcement. If the second reconcile still finds nothing blocking (or
// itself fails), the ORIGINAL Create error is what is returned — it is never
// swallowed in favor of a less informative one. This bounded retry NEVER
// changes includeResourceData between the two reconcile calls, and never
// re-attempts Create itself (fail-closed: a failed encrypted create
// must not silently fall back to a plaintext subscription, nor generate and
// submit a second key).
//
// When includeResourceData is true, a fresh encrypt_key is
// generated (newEncryptKeyFunc) ONLY once the plan has actually decided to
// create (never on a reuse/conflict/suspended outcome, and never — by
// construction, since this function is only reached from runCreate's
// non-dry-run branch — during --dry-run), and is submitted to
// doCreateSubscription in the SAME call that carries includeResourceData,
// so the two are always atomic.
func createOrReuseSubscription(ctx context.Context, svc createSubscriptionAPI, resolved eventlib.ResolvedEventKey, identity core.Identity, includeResourceData bool, requestedFilter *eventlib.Filter) (*createOutcome, error) {
	eventType := resolved.Definition.EventType
	targetResource := resolved.TargetResource

	plan, err := reconcileExisting(ctx, svc, eventType, targetResource, identity, includeResourceData, requestedFilter)
	if err != nil {
		return nil, err
	}
	if plan.Action != planActionCreate {
		return outcomeFromPlan(plan, resolved, identity, includeResourceData)
	}

	var encryptKey string
	if includeResourceData {
		encryptKey, err = newEncryptKeyFunc()
		if err != nil {
			return nil, err
		}
	}

	detail, createErr := doCreateSubscription(ctx, svc, eventType, targetResource, includeResourceData, encryptKey, requestedFilter)
	if createErr == nil {
		return &createOutcome{Action: "created", Detail: detail, PaginationCapped: plan.PaginationCapped}, nil
	}

	plan2, listErr := reconcileExisting(ctx, svc, eventType, targetResource, identity, includeResourceData, requestedFilter)
	if listErr != nil || plan2.Action == planActionCreate {
		// The reconcile-after-failure found nothing new (or itself failed):
		// the original Create error is the most useful thing to surface.
		// Deliberately never retries doCreateSubscription/newEncryptKeyFunc
		// here — see this function's own doc comment.
		return nil, createErr
	}
	return outcomeFromPlan(plan2, resolved, identity, includeResourceData)
}

// outcomeFromPlan converts a non-"create" plan into either a success
// (idempotent reuse) or a typed error (conflict/suspended) — shared by both
// the first reconcile and the post-Create-failure reconcile in
// createOrReuseSubscription. includeResourceData is threaded through purely
// so conflictError can enrich its Hint with encryption-specific guidance
// when this conflict was reached via an encrypted request.
func outcomeFromPlan(plan *reconcilePlan, resolved eventlib.ResolvedEventKey, identity core.Identity, includeResourceData bool) (*createOutcome, error) {
	switch plan.Action {
	case planActionReuse:
		return &createOutcome{Action: "reused", Detail: plan.Existing}, nil
	case planActionConflict:
		return nil, conflictError(resolved, identity, plan, includeResourceData)
	case planActionSuspended:
		return nil, suspendedError(resolved, identity, plan)
	default:
		return nil, errs.NewInternalError(errs.SubtypeUnknown, "unexpected reconcile plan action %q", plan.Action)
	}
}

// doCreateSubscription issues the actual Create call and unwraps its
// response. Any error svc.Create returns (transport or already-classified
// typed business failure — SubscriptionClient.Create) is passed
// through unchanged; the caller (createOrReuseSubscription) decides whether
// to reconcile-and-retry.
//
// encryptKey, when non-empty, is injected via
// PayloadOptionsEncryptBuilder into the SAME CreatePayloadOptions as
// includeResourceData, in the SAME request this function builds — the
// atomicity requirement (include_resource_data=true and encrypt.encrypt_key
// must be submitted together) and the fact that Encrypt is a
// Create-only field (Patch has no encrypt) both mean this is the ONLY place
// in this command that ever sets it. Callers must never log encryptKey —
// see newEncryptKeyFunc's own doc comment.
func doCreateSubscription(ctx context.Context, svc createSubscriptionAPI, eventType, targetResource string, includeResourceData bool, encryptKey string, requestedFilter *eventlib.Filter) (*larkeventv1.SubscriptionDetail, error) {
	if includeResourceData && encryptKey == "" {
		// Defensive fail-closed (atomic, no
		// plaintext-fallback path): the sole caller
		// (createOrReuseSubscription) always generates a key before reaching
		// here whenever includeResourceData is true, and returns its own
		// error early if generation itself fails — so this should be
		// unreachable in practice. Refuse rather than ever submitting an
		// include_resource_data=true Create with no encrypt_key.
		return nil, errs.NewInternalError(errs.SubtypeUnknown,
			"refusing to create an include_resource_data=true subscription without an encrypt_key")
	}
	body := buildCreateSubscriptionBody(eventType, targetResource, includeResourceData, encryptKey, requestedFilter)
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

// buildCreateSubscriptionBody constructs the Create request body, always
// setting includeResourceData and (when non-empty) encryptKey on the SAME
// CreatePayloadOptions within the SAME returned body value — the
// atomicity requirement is structural here, not merely a matter of call
// ordering: there is no code path that could build/send them separately.
//
// This is split out of doCreateSubscription (rather than inlined) so this
// package's own tests can assert the atomicity property directly against a
// plain, fully-inspectable *larkeventv1.CreateSubscriptionReqBody value.
// *larkeventv1.CreateSubscriptionReq itself (what
// NewCreateSubscriptionReqBuilder().Body(body).Build() produces) is NOT
// similarly inspectable from this package: its builder stores body into an
// internal, unexported apiReq field for the SDK's own transport to read,
// never into CreateSubscriptionReq's own same-named exported Body field. A
// test capturing the built *CreateSubscriptionReq itself therefore cannot
// read back what it carried; testing this function directly is the reliable
// way to assert the request's actual content.
func buildCreateSubscriptionBody(eventType, targetResource string, includeResourceData bool, encryptKey string, requestedFilter *eventlib.Filter) *larkeventv1.CreateSubscriptionReqBody {
	payloadOptions := larkeventv1.NewCreatePayloadOptionsBuilder().IncludeResourceData(includeResourceData)
	if encryptKey != "" {
		payloadOptions = payloadOptions.Encrypt(larkeventv1.NewPayloadOptionsEncryptBuilder().EncryptKey(encryptKey).Build())
	}
	builder := larkeventv1.NewCreateSubscriptionReqBodyBuilder().
		EventType(eventType).
		TargetResource(targetResource).
		PayloadOptions(payloadOptions.Build())
	// Only send filter when one was requested. Omitting the field entirely
	// (rather than sending an empty {"filter":{}}) is how create says "no
	// server-side filter"; the empty/clear form is an update-only concept.
	if !requestedFilter.IsEmpty() {
		builder = builder.Filter(eventlib.FilterToSDK(requestedFilter))
	}
	return builder.Build()
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
func conflictError(resolved eventlib.ResolvedEventKey, identity core.Identity, plan *reconcilePlan, includeResourceData bool) error {
	id := strVal(plan.Existing.SubscriptionId)
	hint := fmt.Sprintf("run `lark-cli event subscription get %s --as %s --json` to inspect remote_subscription_id=%s, then either accept its existing configuration or delete it before creating a differently-configured one", id, identity, id)
	if includeResourceData {
		hint = fmt.Sprintf("run `lark-cli event subscription get %s --as %s --json` to inspect remote_subscription_id=%s; include_resource_data cannot be changed in place, so after human confirmation either keep the existing subscription, ensure this identity has scope `event:encrypt_key:read`, or delete it and create a new one with the desired resource-data setting", id, identity, id)
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

// createDryRunResult is `event subscription create --dry-run`'s JSON shape.
// Populated purely from parse/identity/scope preflight
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
// built) identity/template/scope checks dry-run performs.
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

func buildDryRunResult(resolved eventlib.ResolvedEventKey, identity core.Identity, plan *reconcilePlan, includeResourceData bool) *createDryRunResult {
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
		RequiredScopes: createRequiredScopes(includeResourceData),
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
// (conflict/suspended never reach here, they return a typed error
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

// createNextAction implements the "creation succeeding does not
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
