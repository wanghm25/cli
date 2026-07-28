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
	"github.com/larksuite/cli/internal/event/buslocal"
	"github.com/larksuite/cli/internal/event/model"
	larkgw "github.com/larksuite/cli/internal/event/platform/lark"
	subown "github.com/larksuite/cli/internal/event/subscription"
	"github.com/larksuite/cli/internal/output"
)

// updateSubscriptionAPI is the subset of the domain Gateway port this command
// calls: Get (the remote read this command always performs first, per the
// CLI-side read+write invariant, both to report remote_before/impact for
// --dry-run and — since update carries no EventKey — to learn the event type
// and current filter the new one is validated and compared against) and Patch
// (the actual write, which only changes the subscription's server-side filter).
// It matches app.UpdatePort's method set, so the command's gateway drives the
// use case directly. See listSubscriptionsAPI (list.go) for the test-seam
// rationale.
type updateSubscriptionAPI interface {
	Get(ctx context.Context, remoteSubscriptionID string) (*model.RemoteSubscription, error)
	Patch(ctx context.Context, remoteSubscriptionID string, spec subown.PatchSpec) (*model.RemoteSubscription, error)
}

// updateOpts holds `event subscription update`'s flag values.
type updateOpts struct {
	filter      string
	clearFilter bool
	dryRun      bool
	yes         bool
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
// A filter change is reversible with another update, so update stays a plain
// "write" and does not confirmation-gate the common case. It DOES gate the one
// case that disrupts a live consumer: when a running local consumer is bound to
// this subscription, a real filter change alters the events it receives, so
// update requires --yes (returning a ConfirmationRequiredError without it) to
// make an agent/human acknowledge the impact. With no local consumer affected,
// no confirmation is needed and the flow is unchanged.
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

SAFETY: a filter change is reversible with another update, so update is a
plain write in the common case. It requires --yes ONLY when a running local
consumer is bound to this subscription (a filter change alters the events it
receives) — without --yes it then returns a confirmation-required error (exit
code 10); with no local consumer affected it never prompts. Use --dry-run to
preview the change (and any affected consumers) without patching anything.
Resource-data delivery (include_resource_data) cannot be changed here at all;
to change it, delete the subscription and create a new one after human
confirmation.`,
		Example: `  lark-cli event subscription update sub_xxx --clear-filter --dry-run --as bot --json
  lark-cli event subscription update sub_xxx --filter '{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"message_type","op":"eq","value":"text"}}]}}' --yes --as bot --json`,
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
	cmd.Flags().BoolVar(&o.yes, "yes", false,
		"Acknowledge that a running local consumer bound to this subscription will be affected by the filter change; required only when one is (and a human has confirmed)")
	cmd.Flags().BoolVar(&o.asJSON, "json", false, "Emit the result as JSON (for AI / scripts)")
	addAsFlag(cmd)
	// update stays static risk "write": it is a plain write in the common case,
	// and cmdpolicy gates on this static tier. The --yes requirement is CONDITIONAL
	// (only when a local consumer is affected) and is enforced at runtime by the
	// command itself (a ConfirmationRequiredError), not by framework high-risk-write
	// gating — which would otherwise force --yes on every update.
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

	// Best-effort local-consumer facts: a down/unreachable bus yields none, so
	// no consumer is ever reported affected and the update proceeds ungated.
	return applyUpdate(ctx, client, f.IOStreams.Out, remoteSubscriptionID, identity, o, queryLocalConsumers())
}

// applyUpdate is update's testable core: it hands the request to the
// SubscriptionUseCase's Update flow (the mandatory remote read, the desired-
// filter build + validation, the no-op guard, --dry-run, and the Patch) and
// renders the domain outcome — the --dry-run preview, the unchanged no-op, or
// the patched result. It never itself reads remote state, validates the filter,
// decides the no-op, or issues the Patch; the use case owns all of that.
//
// localConsumers is the already-queried, best-effort set of running local
// consumers (empty when the bus is down). The ones bound to this subscription
// are the REAL local impact: they populate the --dry-run local_impact, and they
// arm the --yes gate — a `confirm` hook the use case fires only when a real
// filter change is imminent (never on a dry-run or no-op), returning a
// ConfirmationRequiredError when a consumer is affected and --yes was not given.
// Exercised against a fake updateSubscriptionAPI + canned consumers in tests.
func applyUpdate(ctx context.Context, svc updateSubscriptionAPI, out io.Writer, remoteSubscriptionID string, identity core.Identity, o updateOpts, localConsumers []buslocal.Consumer) error {
	affectedConsumers := matchLocalConsumers(localConsumers, remoteSubscriptionID)

	// confirm gates the real write path only: the use case calls it after the
	// no-op guard, so reaching it already means a genuine filter change. When a
	// local consumer is bound, that change alters its event stream — require an
	// explicit --yes; otherwise proceed ungated (unchanged flow).
	confirm := func() error {
		if len(affectedConsumers) > 0 && !o.yes {
			return errUpdateConfirmationRequired(remoteSubscriptionID, identity, affectedConsumers)
		}
		return nil
	}

	outcome, err := app.NewSubscriptionUseCase().Update(ctx, svc, remoteSubscriptionID, o.filter, o.clearFilter, o.dryRun, confirm)
	if err != nil {
		return err
	}

	switch outcome.Kind {
	case app.UpdatePreview:
		beforeRow := mapRemoteSubscription(outcome.Before)
		// A real change (not a no-op) affects local consumers only when one is
		// actually running for this subscription.
		affected := !outcome.NoChange && len(affectedConsumers) > 0
		result := buildMutationDryRunResult("update", remoteSubscriptionID, identity, &beforeRow,
			updatePlannedAction(outcome.NoChange), affected, updateImpactNote(outcome.NoChange, affectedConsumers),
			updateDryRunNextAction(remoteSubscriptionID, o.clearFilter, outcome.NoChange))
		if affected {
			result.LocalImpact.Consumers = affectedConsumers
		}
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
		// A real filter change just landed. If running local consumers are bound
		// to this subscription, proactively tell their bus(es) so it degrades them
		// (remote_subscription_conflict / next_action=get) NOW, rather than leaving
		// them to wait for the platform's own updated_v1 push. Best-effort and
		// fire-and-forget: a down/unreachable bus is skipped silently (the platform
		// updated_v1 is the backstop), so update still succeeds. Fires only here —
		// never on a no-op (UpdateNoop) or a --dry-run (UpdatePreview).
		notifyAffectedBuses(remoteSubscriptionID, affectedConsumers)
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

// updateImpactNote returns the honest local-consumer impact note for update's
// --dry-run, grounded in the REAL bus query (matched = the running consumers
// bound to this subscription):
//   - no-op: nothing changes, so no consumer is affected;
//   - real change with a running consumer: it WILL receive the new stream and a
//     real run requires --yes to proceed;
//   - real change with none running: the change is safe now (nothing to disrupt).
func updateImpactNote(noChange bool, matched []localConsumerInfo) string {
	switch {
	case noChange:
		return updateNoopLocalImpactNote
	case len(matched) > 0:
		return updateAffectedLocalImpactNote
	default:
		return updateNoLocalConsumerImpactNote
	}
}

const updateAffectedLocalImpactNote = "a running local consumer is bound to this subscription; a filter change alters the events it receives, so it must be re-synced — check `lark-cli event status` and restart it. Running without --dry-run requires --yes to acknowledge this impact."

const updateNoLocalConsumerImpactNote = "a filter change alters the delivered event stream, but no local consumer is currently bound to this subscription, so none is affected; a later `event consume` picks up the updated filter"

const updateNoopLocalImpactNote = "the requested filter already matches the current one, so no change is planned and no local consumer is affected"

// errUpdateConfirmationRequired implements update's CONDITIONAL confirmation
// gate: a real filter change on a subscription with a running local consumer
// disrupts that consumer's event stream, so without --yes it returns a
// confirmation-required error (category confirmation, exit code 10) naming the
// affected consumer(s). Distinct from delete's ALWAYS-on gate — update's fires
// only when a consumer is actually affected.
func errUpdateConfirmationRequired(remoteSubscriptionID string, identity core.Identity, matched []localConsumerInfo) error {
	return errs.NewConfirmationRequiredError(errs.RiskHighRiskWrite, "event subscription update",
		"updating remote Subscription %s affects a running local consumer and requires confirmation", remoteSubscriptionID).
		WithHint("a filter change on %s (identity=%s) alters the event stream delivered to %d running local consumer(s): %s; re-run the same command with --yes after a human confirms, then re-sync the consumer(s) — check `lark-cli event status`", remoteSubscriptionID, identity, len(matched), formatLocalConsumers(matched))
}
