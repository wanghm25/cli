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
	"github.com/larksuite/cli/internal/event/model"
	larkgw "github.com/larksuite/cli/internal/event/platform/lark"
	"github.com/larksuite/cli/internal/output"
)

// deleteSubscriptionAPI is the subset of the platform/lark SubscriptionGateway
// this command calls: Get (the remote read this command always performs first,
// per the CLI-side read+write invariant, both to report
// remote_before/impact for --dry-run and to describe the affected
// subscription in the confirmation-required Hint) and Delete (the actual
// write). See listSubscriptionsAPI (list.go) for the test-seam rationale.
type deleteSubscriptionAPI interface {
	Get(ctx context.Context, remoteSubscriptionID string) (*model.RemoteSubscription, error)
	Delete(ctx context.Context, remoteSubscriptionID string) error
}

// deleteOpts holds `event subscription delete`'s flag values.
type deleteOpts struct {
	dryRun bool
	yes    bool
	asJSON bool
}

// NewCmdDelete builds `event subscription delete <remote_subscription_id>`.
// Like create/renew/reactivate, delete
// requires BOTH event:subscription:read and event:subscription:write:
// it always reads the current remote state first (Get), both to
// report remote_before/impact for --dry-run and to describe what is about
// to be deleted in the confirmation-required Hint.
//
// delete is a high-risk write on a shared remote resource:
// without --yes it returns a typed ConfirmationRequiredError
// (category confirmation, exit code 10) instead of proceeding. Deleting the
// remote Subscription is explicitly NOT a substitute for stopping a local
// `event consume` process that may still be using it — that message is
// carried in the confirmation Hint and in --dry-run's local_impact note.
func NewCmdDelete(f *cmdutil.Factory) *cobra.Command {
	var o deleteOpts
	cmd := &cobra.Command{
		Use:   "delete <remote_subscription_id>",
		Short: "Delete a remote event Subscription",
		Long: `Delete an existing remote Subscription by its remote_subscription_id.

IDENTITY: --as user|bot|auto, resolved to one effective identity (no
per-template check — this command carries no EventKey context).

SCOPE: requires BOTH event:subscription:read and event:subscription:write:
this command always reads the current remote state first, both to report
impact for --dry-run and to describe what is about to be deleted when
confirmation is required.

OUTPUT: {operation, remote_subscription_id, deleted: true, subscription{
...last known state...}, next_action}.

NEXT STEP: deleting the remote Subscription is NOT a substitute for
stopping a local 'event consume' process that may still be using it — stop
that separately with 'lark-cli event stop'.

SAFETY: this is a high-risk write on a resource other identities/processes
may share — without --yes it returns a confirmation-required error (exit
code 10) instead of proceeding; pass --yes only after a human has
confirmed. Use --dry-run to preview the plan without deleting anything.`,
		Example: `  lark-cli event subscription delete sub_xxx --dry-run --as bot --json
  lark-cli event subscription delete sub_xxx --yes --as bot --json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDelete(cmd, f, args[0], o)
		},
	}

	cmd.Flags().BoolVar(&o.dryRun, "dry-run", false,
		"Preview the plan (identity/scope preflight + remote read + impact analysis) without deleting anything")
	cmd.Flags().BoolVar(&o.yes, "yes", false, "Confirm this high-risk write (required unless --dry-run); only pass this after a human has confirmed")
	cmd.Flags().BoolVar(&o.asJSON, "json", false, "Emit the result as JSON (for AI / scripts)")
	addAsFlag(cmd)
	// delete is confirmation-gated: without --yes it returns a
	// ConfirmationRequiredError (RiskHighRiskWrite, exit code 10). The static
	// annotation must match that gate so --help/schema tell an Agent the truth —
	// framework-level confirmation gating acts only on high-risk-write. (renew/
	// reactivate stay "write": a filter/TTL change is reversible and ungated.)
	cmdutil.SetRisk(cmd, "high-risk-write")

	return cmd
}

func runDelete(cmd *cobra.Command, f *cmdutil.Factory, remoteSubscriptionID string, o deleteOpts) error {
	if strings.TrimSpace(remoteSubscriptionID) == "" {
		return errEmptyRemoteSubscriptionID()
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
	uat, scopesVerified, err := resolveUATAndCheckScopes(ctx, f, cfg.AppID, identity, subscriptionMutationScopes)
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

	before, err := getSubscription(ctx, client, remoteSubscriptionID)
	if err != nil {
		return err
	}

	// Best-effort local-consumer facts: a down/unreachable bus yields none, so
	// the impact truthfully reports "no known local consumer" without failing.
	affectedConsumers := matchLocalConsumers(queryLocalConsumers(), remoteSubscriptionID)

	if o.dryRun {
		result := buildDeleteDryRun(remoteSubscriptionID, identity, before, affectedConsumers, scopesVerified)
		if o.asJSON {
			output.PrintJson(f.IOStreams.Out, result)
			return nil
		}
		writeMutationDryRunText(f.IOStreams.Out, result)
		return nil
	}

	if err := applyDelete(ctx, client, remoteSubscriptionID, identity, before, affectedConsumers, o.yes); err != nil {
		return err
	}
	result := buildDeleteResult(remoteSubscriptionID, before)
	if o.asJSON {
		output.PrintJson(f.IOStreams.Out, result)
		return nil
	}
	writeDeleteResultText(f.IOStreams.Out, result)
	return nil
}

// applyDelete is the write-capable half of delete's non-dry-run path: the
// confirmation gate, then the actual Delete call — factored out from
// runDelete (which only parses flags and builds the real network-capable
// client) so this decision sequence is directly testable against a fake
// deleteSubscriptionAPI, mirroring update.go's applyUpdate. before is the
// remote state runDelete already fetched via Get for --dry-run/
// remote_before/Hint purposes; this does not re-fetch it. matched is the REAL
// running local consumer(s) bound to this subscription (best-effort; empty when
// the bus is down), named in the confirmation Hint so the operator sees exactly
// what a delete disrupts.
func applyDelete(ctx context.Context, svc deleteSubscriptionAPI, remoteSubscriptionID string, identity core.Identity, before *subscriptionRow, matched []localConsumerInfo, yes bool) error {
	if !yes {
		return errDeleteConfirmationRequired(remoteSubscriptionID, identity, before, matched)
	}
	return doDeleteSubscription(ctx, svc, remoteSubscriptionID)
}

// buildDeleteDryRun is delete's testable --dry-run builder: the shared mutation
// preview plus the REAL local impact — local_consumer_affected and the named
// consumer(s) come from matched (the running consumers bound to this
// subscription), so the preview truthfully reflects what a delete disrupts
// instead of the old hardcoded "no local impact". Empty matched (no bus / none
// bound) reports no local impact.
func buildDeleteDryRun(remoteSubscriptionID string, identity core.Identity, before *subscriptionRow, matched []localConsumerInfo, scopesVerified bool) *mutationDryRunResult {
	result := buildMutationDryRunResult("delete", remoteSubscriptionID, identity, scopesVerified, before,
		"delete", len(matched) > 0, deleteImpactNote(matched),
		fmt.Sprintf("run with --yes (after a human confirms) to permanently delete remote_subscription_id=%s; this does not stop any local `event consume` process", remoteSubscriptionID))
	if len(matched) > 0 {
		result.LocalImpact.Consumers = matched
	}
	return result
}

// doDeleteSubscription issues the actual Delete call via the gateway. Unlike
// Patch/Renew/Reactivate, a Delete carries no subscription payload — there is
// nothing to unwrap on success, so the gateway's Delete (and this) returns only
// an error. Any error it returns (transport, or an already-classified business
// failure) is passed through unchanged.
func doDeleteSubscription(ctx context.Context, svc deleteSubscriptionAPI, remoteSubscriptionID string) error {
	return svc.Delete(ctx, remoteSubscriptionID)
}

// errDeleteConfirmationRequired implements the high-risk-write
// confirmation gate for delete: category confirmation (exit code 10),
// risk high-risk-write, and a Hint naming the affected
// remote_subscription_id/event_key/identity/current state plus the REAL
// running local consumer(s) bound to it (from the best-effort bus query; the
// pids clause is omitted when none is running) and the required "not a
// substitute for stopping local consumers" caveat.
func errDeleteConfirmationRequired(remoteSubscriptionID string, identity core.Identity, before *subscriptionRow, matched []localConsumerInfo) error {
	consumerClause := ""
	if len(matched) > 0 {
		consumerClause = fmt.Sprintf(" %d local consumer(s) are currently bound to it and will stop receiving events: %s;", len(matched), formatLocalConsumers(matched))
	}
	return errs.NewConfirmationRequiredError(errs.RiskHighRiskWrite, "event subscription delete",
		"deleting remote Subscription %s requires confirmation", remoteSubscriptionID).
		WithHint("this permanently deletes shared remote Subscription %s (event_key=%s, identity=%s, current state=%s);%s it is not a substitute for stopping local consumers — stop any local `event consume` process using it separately with `lark-cli event stop`; re-run the same command with --yes after a human has confirmed", remoteSubscriptionID, before.EventKey, identity, before.Remote.State, consumerClause)
}

// deleteImpactNote is delete's --dry-run local_impact note, grounded in the
// REAL bus query: it names the running consumer(s) a delete disrupts when any
// is bound, and otherwise states the delete has no local impact. Either way it
// keeps the invariant caveat that delete is not a substitute for stopping local
// consumers.
func deleteImpactNote(matched []localConsumerInfo) string {
	if len(matched) > 0 {
		return fmt.Sprintf("a running local consumer is bound to this subscription (%s); deleting it stops that consumer from receiving events. `event subscription delete` only removes the remote Subscription — it is not a substitute for stopping local consumers; stop the `event consume` process separately with `lark-cli event stop`.", formatLocalConsumers(matched))
	}
	return deleteLocalImpactNote
}

const deleteLocalImpactNote = "`event subscription delete` only removes the remote Subscription; it never stops a local `event consume` process — it is not a substitute for stopping local consumers, which must be stopped separately."

// deleteResult is delete's own non-dry-run success JSON shape — distinct
// from the shared mutationResult (renew/reactivate.go) because
// DeleteSubscriptionResp carries no fresh SubscriptionDetail to echo back;
// Subscription here is the last known state, captured by the
// Get this command always performs before deleting.
type deleteResult struct {
	Operation            string          `json:"operation"`
	RemoteSubscriptionID string          `json:"remote_subscription_id"`
	Deleted              bool            `json:"deleted"`
	Subscription         subscriptionRow `json:"subscription"`
	NextAction           string          `json:"next_action"`
}

func buildDeleteResult(remoteSubscriptionID string, before *subscriptionRow) *deleteResult {
	return &deleteResult{
		Operation:            "delete",
		RemoteSubscriptionID: remoteSubscriptionID,
		Deleted:              true,
		Subscription:         *before,
		NextAction:           "the remote Subscription has been deleted; if a local `event consume` process is still using it, stop it separately with `lark-cli event stop`",
	}
}

func writeDeleteResultText(out io.Writer, result *deleteResult) {
	fmt.Fprintf(out, "Deleted remote Subscription %s\n", result.RemoteSubscriptionID)
	fmt.Fprintf(out, "Next: %s\n", result.NextAction)
}
