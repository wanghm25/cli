// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/event/app"
	larkgw "github.com/larksuite/cli/internal/event/platform/lark"
	"github.com/larksuite/cli/internal/output"
)

// updateSubscriptionAPI is the subset of the platform/lark SubscriptionGateway
// this command calls: Get (the remote read this command always performs first,
// per the CLI-side read+write invariant, both to report remote_before/impact
// for --dry-run and — since update carries no EventKey — to learn the event
// type and current filter the new one is validated and compared against) and
// Patch (the actual write, which only changes the subscription's server-side
// filter). See listSubscriptionsAPI (list.go) for the test-seam rationale.
type updateSubscriptionAPI interface {
	Get(ctx context.Context, remoteSubscriptionID string) (*larkgw.RemoteSubscription, error)
	Patch(ctx context.Context, remoteSubscriptionID string, spec larkgw.PatchSpec) (*larkgw.RemoteSubscription, error)
}

// updateOpts holds `event subscription update`'s flag values.
type updateOpts struct {
	filter      string
	clearFilter bool
	dryRun      bool
	asJSON      bool
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
// it implies) is fixed when a Subscription is created. Changing it is only ever
// done by deleting the Subscription and creating a new one (after human
// confirmation), so update exposes no flag for it and touches only the filter.
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
delivery (include_resource_data) cannot be changed here at all; to change it,
delete the subscription and create a new one after human confirmation.`,
		Example: `  lark-cli event subscription update sub_xxx --clear-filter --dry-run --as bot --json
  lark-cli event subscription update sub_xxx --filter '{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"message_type","op":"eq","value":"text"}}]}}' --as bot --json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runUpdate(cmd, f, args[0], o)
		},
	}

	cmd.Flags().StringVar(&o.filter, "filter", "",
		"Inline `json` event filter to set server-side, replacing any current filter; validated against this subscription's event type (see 'event schema <key> --json'). Mutually exclusive with --clear-filter.")
	cmd.Flags().BoolVar(&o.clearFilter, "clear-filter", false,
		"Remove the server-side event filter from this subscription so every matching event is delivered. Mutually exclusive with --filter.")
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
	client, err := larkgw.NewSubscriptionGateway(sdk, identity, uat)
	if err != nil {
		return err
	}

	return applyUpdate(ctx, client, f.IOStreams.Out, remoteSubscriptionID, identity, o)
}

// applyUpdate is update's testable core: it hands the request to the
// SubscriptionUseCase's Update flow (the mandatory remote read, the desired-
// filter build + validation, the no-op guard, --dry-run, and the Patch) and
// renders the domain outcome — the --dry-run preview, the unchanged no-op, or
// the patched result. It never itself reads remote state, validates the filter,
// decides the no-op, or issues the Patch; the use case owns all of that, and
// this function only renders (against a fake updateSubscriptionAPI in tests).
func applyUpdate(ctx context.Context, svc updateSubscriptionAPI, out io.Writer, remoteSubscriptionID string, identity core.Identity, o updateOpts) error {
	outcome, err := app.NewSubscriptionUseCase().Update(ctx, svc, remoteSubscriptionID, o.filter, o.clearFilter, o.dryRun)
	if err != nil {
		return err
	}

	switch outcome.Kind {
	case app.UpdatePreview:
		beforeRow := mapRemoteSubscription(outcome.Before)
		localAffected, impactNote := updateLocalImpact(outcome.NoChange)
		result := buildMutationDryRunResult("update", remoteSubscriptionID, identity, &beforeRow,
			updatePlannedAction(outcome.NoChange), localAffected, impactNote,
			updateDryRunNextAction(remoteSubscriptionID, o.clearFilter, outcome.NoChange))
		if o.asJSON {
			output.PrintJson(out, result)
			return nil
		}
		writeMutationDryRunText(out, result)
		return nil

	case app.UpdateNoop:
		// The requested filter already matches the current one — report the
		// unchanged subscription without issuing a Patch. Mirrors
		// updateDryRunNextAction's noop wording: a --clear-filter no-op says
		// the subscription already has no filter; a --filter no-op says it
		// already has the requested filter.
		already := "already has the requested filter"
		if o.clearFilter {
			already = "already has no filter"
		}
		result := buildMutationResult("update", outcome.Before,
			fmt.Sprintf("no change: remote_subscription_id=%s %s; run `lark-cli event subscription get %s --as %s --json` to confirm", remoteSubscriptionID, already, remoteSubscriptionID, identity))
		if o.asJSON {
			output.PrintJson(out, result)
			return nil
		}
		writeMutationResultText(out, "Unchanged", result)
		return nil

	default: // app.UpdateApplied
		result := buildMutationResult("update", outcome.After,
			updateSuccessNextAction(remoteSubscriptionID, identity, o.clearFilter))
		if o.asJSON {
			output.PrintJson(out, result)
			return nil
		}
		writeMutationResultText(out, "Updated", result)
		return nil
	}
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
		return fmt.Sprintf("run without --dry-run to remove the server-side filter from remote_subscription_id=%s; any running local `event consume` process receives the widened event stream only after it re-syncs — check `lark-cli event status` and restart it if needed", remoteSubscriptionID)
	default:
		return fmt.Sprintf("run without --dry-run to apply the new server-side filter to remote_subscription_id=%s; any running local `event consume` process receives the new event stream only after it re-syncs — check `lark-cli event status` and restart it if needed", remoteSubscriptionID)
	}
}

func updateSuccessNextAction(remoteSubscriptionID string, identity core.Identity, clearFilter bool) string {
	what := "the new filter"
	if clearFilter {
		what = "that the filter was removed"
	}
	return fmt.Sprintf("run `lark-cli event subscription get %s --as %s --json` to confirm %s; a running local `event consume` process picks up the change only after it re-syncs — check `lark-cli event status`", remoteSubscriptionID, identity, what)
}

// updateLocalImpact returns the honest local-consumer impact for update's
// --dry-run. A real filter change alters the delivered event stream, so any
// local consumer of this subscription is affected once it re-syncs; update
// cannot cheaply tell whether one is actually running, so it discloses the
// possible impact via the note rather than asserting none. A no-op (the
// requested filter already matches the current one) changes nothing and so
// reports no impact — this command never claims impact it does not cause.
func updateLocalImpact(noChange bool) (bool, string) {
	if noChange {
		return false, updateNoopLocalImpactNote
	}
	return true, updateLocalImpactNote
}

const updateLocalImpactNote = "a filter change affects any local consumer of this subscription — it will receive the new event stream and should be re-synced; this command cannot tell whether a consumer is running, so run `lark-cli event status` and restart the consumer if needed"

const updateNoopLocalImpactNote = "the requested filter already matches the current one, so no change is planned and no local consumer is affected"
