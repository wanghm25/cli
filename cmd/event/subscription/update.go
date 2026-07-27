// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	eventlib "github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/output"
)

// updateSubscriptionAPI is the subset of *eventlib.SubscriptionClient this
// command calls: Get (the remote read this command always performs first, per
// the CLI-side read+write invariant, both to report remote_before/impact for
// --dry-run and — since update carries no EventKey — to learn the event type
// and current filter the new one is validated and compared against) and Patch
// (the actual write, which only changes the subscription's server-side
// filter). See listSubscriptionsAPI (list.go) for the test-seam rationale.
type updateSubscriptionAPI interface {
	Get(ctx context.Context, req *larkeventv1.GetSubscriptionReq) (*larkeventv1.GetSubscriptionResp, error)
	Patch(ctx context.Context, req *larkeventv1.PatchSubscriptionReq) (*larkeventv1.PatchSubscriptionResp, error)
}

// updateOpts holds `event subscription update`'s flag values.
type updateOpts struct {
	filter              string
	clearFilter         bool
	includeResourceData bool
	dryRun              bool
	asJSON              bool
}

// NewCmdUpdate builds `event subscription update <remote_subscription_id>`.
// Its one job is to change the subscription's server-side event filter, set
// with --filter <json> or removed with --clear-filter (exactly one is
// required). Like create/renew/reactivate/delete it requires BOTH
// event:subscription:read and event:subscription:write: it always reads the
// current subscription first — both to learn the event type (so --filter can
// be validated against it, since this command carries no EventKey) and to
// skip the write when the requested filter already matches.
//
// Unlike delete, update is not a high-risk confirmation-gated action — a
// filter change is reversible with another update, so it does not expose
// --yes and never returns a ConfirmationRequiredError.
//
// include_resource_data is deliberately not updatable: the platform's Patch
// API carries only a filter, and resource-data delivery (with the encryption
// it implies) is fixed when a Subscription is created. --include-resource-data
// is kept as a recognized flag purely so a caller reaching for it gets a
// specific, actionable rejection instead of an "unknown flag" error.
func NewCmdUpdate(f *cmdutil.Factory) *cobra.Command {
	var o updateOpts
	cmd := &cobra.Command{
		Use:   "update <remote_subscription_id>",
		Short: "Change a remote event Subscription's server-side filter",
		Long: `Change the server-side event filter on an existing remote Subscription,
identified by remote_subscription_id.

The filter is the only field update changes. Pass exactly one of:
  --filter <json>   set (or replace) the server-side filter, validated
                    against this subscription's event type.
  --clear-filter    remove the filter entirely, so every matching event is
                    delivered.

IDENTITY: --as user|bot|auto, resolved to one effective identity (no
per-template check — this command carries no EventKey context).

SCOPE: requires BOTH event:subscription:read and event:subscription:write:
this command always reads the current subscription first — to learn the
event type its --filter is validated against, and to skip a no-op write when
the requested filter already matches — before it may patch.

OUTPUT: {operation, remote_subscription_id, subscription{...}, next_action}.
When the requested filter already matches the current one, no write is
issued and the result reports the unchanged subscription.

NEXT STEP: run 'lark-cli event subscription get <remote_subscription_id>
--json' to confirm the new filter. A running local 'event consume' process
keeps its current filter until it re-syncs — check 'lark-cli event status'.

SAFETY: changing a filter is reversible with another update, so update is NOT
a high-risk confirmation-gated action and does not accept --yes. Use
--dry-run to preview the change without patching anything. Resource-data
delivery cannot be changed here — see --include-resource-data.`,
		Example: `  lark-cli event subscription update sub_xxx --clear-filter --dry-run --as bot --json
  lark-cli event subscription update sub_xxx --filter '{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"message_type","op":"eq","value":"text"}}]}}' --as bot --json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runUpdate(cmd, f, args[0], o)
		},
	}

	cmd.Flags().StringVar(&o.filter, "filter", "",
		"Inline JSON event filter to set server-side, replacing any current filter; validated against this subscription's event type (see `event schema <key> --json`). Mutually exclusive with --clear-filter.")
	cmd.Flags().BoolVar(&o.clearFilter, "clear-filter", false,
		"Remove the server-side event filter from this subscription so every matching event is delivered. Mutually exclusive with --filter.")
	cmd.Flags().BoolVar(&o.includeResourceData, "include-resource-data", false,
		"Not updatable: whether delivered events include resource data is fixed when a subscription is created and cannot be changed via update. Passing this flag is always rejected; delete and recreate (after human confirmation) to change it.")
	cmd.Flags().BoolVar(&o.dryRun, "dry-run", false,
		"Preview the plan (identity/scope preflight + remote read + impact analysis) without changing the filter")
	cmd.Flags().BoolVar(&o.asJSON, "json", false, "Emit the result as JSON (for AI / scripts)")
	addAsFlag(cmd)
	cmdutil.SetRisk(cmd, "write")

	return cmd
}

func runUpdate(cmd *cobra.Command, f *cmdutil.Factory, remoteSubscriptionID string, o updateOpts) error {
	if strings.TrimSpace(remoteSubscriptionID) == "" {
		return errEmptyRemoteSubscriptionID()
	}

	// include_resource_data is not updatable at all — reject before resolving
	// an identity, checking scopes, or touching the network. That the Patch
	// API is filter-only (and resource-data delivery is fixed at create time)
	// is a structural fact about the request, independent of remote state, so
	// this short-circuits here exactly as it did when update did nothing else.
	if cmd.Flags().Changed("include-resource-data") {
		return errUpdateCannotSwitchEncryption(remoteSubscriptionID, o.includeResourceData)
	}

	// Exactly one of --filter / --clear-filter selects the change to make.
	// Both, or neither, is a caller mistake rejected locally before any
	// identity/scope/network work.
	filterSet := cmd.Flags().Changed("filter")
	switch {
	case filterSet && o.clearFilter:
		return errs.NewValidationError(errs.SubtypeInvalidArgument,
			"use either --filter or --clear-filter, not both").
			WithParam("--filter").
			WithHint("pass --filter <json> to set a filter, or --clear-filter to remove it — not both")
	case !filterSet && !o.clearFilter:
		return errs.NewValidationError(errs.SubtypeInvalidArgument,
			"specify --filter <json> or --clear-filter").
			WithParam("--filter").
			WithHint("pass --filter <json> to set a server-side filter, or --clear-filter to remove the current one; see `lark-cli event schema <key> --json` for the filter shape")
	case filterSet && strings.TrimSpace(o.filter) == "":
		// A blank --filter (e.g. from an unset variable interpolated into the
		// command line, --filter "$UNSET_VAR") is a caller mistake, not a
		// request to clear the filter — ParseAndValidateFilter would otherwise
		// accept "" as the empty filter and silently widen delivery on a
		// currently-filtered subscription. Rejected here, before any
		// identity/scope/network work.
		return errs.NewValidationError(errs.SubtypeInvalidArgument,
			"--filter was given a blank value").
			WithParam("--filter").
			WithHint("pass --filter <json> with a non-empty filter, or use --clear-filter to remove the current filter instead")
	}

	ctx := cmd.Context()
	identity, err := resolveEffectiveIdentity(cmd, f)
	if err != nil {
		return err
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

	return applyUpdate(ctx, client, f.IOStreams.Out, remoteSubscriptionID, identity, o)
}

// applyUpdate is the read+decide+write core of update's flow, factored out of
// runUpdate (which only handles the purely-local flag rejections and builds
// the real network-capable client) so the whole sequence — the mandatory
// remote read, the no-op guard, --dry-run, and the actual Patch — is directly
// testable against a fake updateSubscriptionAPI, mirroring delete.go's
// applyDelete.
//
// The remote read comes first and is mandatory: update carries no EventKey,
// so the fetched subscription is the only source of the event type --filter
// must be validated against, and of the current filter the no-op guard
// compares the requested one against.
func applyUpdate(ctx context.Context, svc updateSubscriptionAPI, out io.Writer, remoteSubscriptionID string, identity core.Identity, o updateOpts) error {
	before, err := readSubscriptionForUpdate(ctx, svc, remoteSubscriptionID)
	if err != nil {
		return err
	}

	eventType := strVal(before.EventType)
	if eventType == "" {
		// A subscription with no event_type can't be mapped to a filter
		// capability, so --filter cannot be validated against it. Treat a
		// successful Get that omits it as a wire anomaly rather than guessing.
		return errs.NewInternalError(errs.SubtypeInvalidResponse,
			"subscription %s did not report an event_type, so its filter capability cannot be determined", remoteSubscriptionID)
	}

	// Build the desired filter. --clear-filter is the empty filter (wired as
	// the {"filter":{}} clear form). --filter is parsed and validated against
	// this event type's capability; a parse/validation failure is already a
	// typed invalid_argument on --filter — returned unchanged, before any
	// write, and it never echoes the filter's contents.
	var desired *eventlib.Filter
	if o.clearFilter {
		desired = &eventlib.Filter{}
	} else {
		desired, err = eventlib.ParseAndValidateFilter(o.filter, eventlib.FilterMetaFor(eventType))
		if err != nil {
			return err
		}
	}

	current := eventlib.FilterFromSDK(before.Filter)
	noChange := eventlib.Equal(desired, current)

	if o.dryRun {
		beforeRow := mapSubscriptionDetail(before)
		result := buildMutationDryRunResult("update", remoteSubscriptionID, identity, &beforeRow,
			updatePlannedAction(noChange), updateLocalImpactNote,
			updateDryRunNextAction(remoteSubscriptionID, o.clearFilter, noChange))
		if o.asJSON {
			output.PrintJson(out, result)
			return nil
		}
		writeMutationDryRunText(out, result)
		return nil
	}

	if noChange {
		// The requested filter already matches the current one — report the
		// unchanged subscription without issuing a Patch. Mirrors
		// updateDryRunNextAction's noop wording: a --clear-filter no-op says
		// the subscription already has no filter; a --filter no-op says it
		// already has the requested filter.
		already := "already has the requested filter"
		if o.clearFilter {
			already = "already has no filter"
		}
		result := buildMutationResult("update", before,
			fmt.Sprintf("no change: remote_subscription_id=%s %s; run `lark-cli event subscription get %s --as %s --json` to confirm", remoteSubscriptionID, already, remoteSubscriptionID, identity))
		if o.asJSON {
			output.PrintJson(out, result)
			return nil
		}
		writeMutationResultText(out, "Unchanged", result)
		return nil
	}

	detail, err := doUpdateSubscription(ctx, svc, remoteSubscriptionID, desired)
	if err != nil {
		return err
	}
	result := buildMutationResult("update", detail,
		updateSuccessNextAction(remoteSubscriptionID, identity, o.clearFilter))
	if o.asJSON {
		output.PrintJson(out, result)
		return nil
	}
	writeMutationResultText(out, "Updated", result)
	return nil
}

// readSubscriptionForUpdate fetches the current remote subscription and
// unwraps its detail. Unlike the shared, row-returning getSubscription
// (get.go), update needs the raw *larkeventv1.SubscriptionDetail: its
// event_type selects the filter capability --filter is validated against, and
// its current filter is what the no-op guard compares the requested one
// against via eventlib.Equal. Any error svc.Get returns (transport, or an
// already-classified typed business failure such as an unknown
// remote_subscription_id) is passed through unchanged.
func readSubscriptionForUpdate(ctx context.Context, svc updateSubscriptionAPI, remoteSubscriptionID string) (*larkeventv1.SubscriptionDetail, error) {
	req := larkeventv1.NewGetSubscriptionReqBuilder().SubscriptionId(remoteSubscriptionID).Build()
	resp, err := svc.Get(ctx, req)
	if err != nil {
		return nil, err
	}
	var detail *larkeventv1.SubscriptionDetail
	if resp != nil && resp.Data != nil {
		detail = resp.Data.Subscription
	}
	if detail == nil {
		// Defensive: a syntactically successful response with no Subscription
		// payload is a wire anomaly, not "found an empty subscription".
		return nil, errs.NewInternalError(errs.SubtypeInvalidResponse,
			"subscription API reported success for %s but returned no subscription data", remoteSubscriptionID)
	}
	return detail, nil
}

// doUpdateSubscription issues the actual Patch and unwraps its response. Any
// error svc.Patch returns (transport, or an already-classified typed business
// failure from SubscriptionClient.Patch) is passed through unchanged.
func doUpdateSubscription(ctx context.Context, svc updateSubscriptionAPI, remoteSubscriptionID string, desired *eventlib.Filter) (*larkeventv1.SubscriptionDetail, error) {
	req := larkeventv1.NewPatchSubscriptionReqBuilder().
		SubscriptionId(remoteSubscriptionID).
		Body(buildPatchSubscriptionBody(desired)).
		Build()
	resp, err := svc.Patch(ctx, req)
	if err != nil {
		return nil, err
	}
	var detail *larkeventv1.SubscriptionDetail
	if resp != nil && resp.Data != nil {
		detail = resp.Data.Subscription
	}
	if detail == nil {
		return nil, errs.NewInternalError(errs.SubtypeInvalidResponse,
			"subscription update reported success but returned no subscription data")
	}
	return detail, nil
}

// buildPatchSubscriptionBody constructs the Patch request body, which carries
// only the filter (the sole field update changes). eventlib.FilterToSDK
// projects the desired filter to the SDK type; an empty/cleared filter becomes
// the {"filter":{}} clear form (a non-nil empty *Filter) rather than an
// omitted field, so a clear is distinguishable on the wire from "leave the
// filter unchanged".
//
// This is split out of doUpdateSubscription (rather than inlined) so this
// package's own tests can assert the projected filter directly against a
// plain, fully-inspectable *larkeventv1.PatchSubscriptionReqBody value — the
// built *PatchSubscriptionReq itself stores the body in an internal,
// unexported field the SDK transport reads, so a test capturing the request
// cannot read back what it carried (mirrors create.go's
// buildCreateSubscriptionBody, same rationale).
func buildPatchSubscriptionBody(desired *eventlib.Filter) *larkeventv1.PatchSubscriptionReqBody {
	return larkeventv1.NewPatchSubscriptionReqBodyBuilder().
		Filter(eventlib.FilterToSDK(desired)).
		Build()
}

// updatePlannedAction is the --dry-run planned_change.action: "noop" when the
// requested filter already matches the current one (a real run would issue no
// Patch), "update" otherwise.
func updatePlannedAction(noChange bool) string {
	if noChange {
		return "noop"
	}
	return "update"
}

func updateDryRunNextAction(remoteSubscriptionID string, clearFilter, noChange bool) string {
	switch {
	case noChange && clearFilter:
		return fmt.Sprintf("remote_subscription_id=%s already has no filter; running without --dry-run makes no change", remoteSubscriptionID)
	case noChange:
		return fmt.Sprintf("remote_subscription_id=%s already has the requested filter; running without --dry-run makes no change", remoteSubscriptionID)
	case clearFilter:
		return fmt.Sprintf("run without --dry-run to remove the server-side filter from remote_subscription_id=%s", remoteSubscriptionID)
	default:
		return fmt.Sprintf("run without --dry-run to apply the new server-side filter to remote_subscription_id=%s", remoteSubscriptionID)
	}
}

func updateSuccessNextAction(remoteSubscriptionID string, identity core.Identity, clearFilter bool) string {
	what := "the new filter"
	if clearFilter {
		what = "that the filter was removed"
	}
	return fmt.Sprintf("run `lark-cli event subscription get %s --as %s --json` to confirm %s; a running local `event consume` process picks up the change only after it re-syncs — check `lark-cli event status`", remoteSubscriptionID, identity, what)
}

// errUpdateCannotSwitchEncryption reports the by-design refusal to change
// include_resource_data (and the encryption it implies) on an existing remote
// Subscription via update, in either direction: the platform's Patch API is
// filter-only and carries no field that could touch it, and `encrypt` is
// create-only. desiredIncludeResourceData is the value the caller asked for,
// echoed into the delete+recreate example so the guidance is directionally
// correct either way.
//
// Changing resource-data delivery is therefore only ever done by deleting the
// Subscription and creating a new one (or creating a separate new one), after
// a human confirms — the CLI never leaves a Subscription in an inconsistent
// state (resource data toggled while its key can be neither added nor
// removed).
func errUpdateCannotSwitchEncryption(remoteSubscriptionID string, desiredIncludeResourceData bool) error {
	return errs.NewValidationError(errs.SubtypeFailedPrecondition,
		"cannot change include_resource_data on an existing subscription via update").
		WithParam("--include-resource-data").
		WithHint("resource-data delivery (and the encryption it implies) is decided when a subscription is created and cannot be changed via update; after human confirmation, delete this subscription and create a new one (or create a separate new subscription) — e.g. `lark-cli event subscription delete %s` then `lark-cli event subscription create <refined-event-key> --include-resource-data=%t`", remoteSubscriptionID, desiredIncludeResourceData)
}

const updateLocalImpactNote = "`event subscription update` only changes the remote Subscription's server-side filter; it never starts, stops, or changes a local `event consume` process — a running local consumer keeps its current filter until it re-syncs."
