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

// deleteSubscriptionAPI is the subset of *eventlib.SubscriptionClient this
// command calls: Get (the remote read this command always performs first,
// per the CLI-side read+write invariant, both to report
// remote_before/impact for --dry-run and to describe the affected
// subscription in the confirmation-required Hint) and Delete (the actual
// write). See listSubscriptionsAPI (list.go) for the test-seam rationale.
type deleteSubscriptionAPI interface {
	Get(ctx context.Context, req *larkeventv1.GetSubscriptionReq) (*larkeventv1.GetSubscriptionResp, error)
	Delete(ctx context.Context, req *larkeventv1.DeleteSubscriptionReq) (*larkeventv1.DeleteSubscriptionResp, error)
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
	cmdutil.SetRisk(cmd, "write")

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

	before, err := getSubscription(ctx, client, remoteSubscriptionID)
	if err != nil {
		return err
	}

	if o.dryRun {
		result := buildMutationDryRunResult("delete", remoteSubscriptionID, identity, before,
			"delete", deleteLocalImpactNote,
			fmt.Sprintf("run with --yes (after a human confirms) to permanently delete remote_subscription_id=%s; this does not stop any local `event consume` process", remoteSubscriptionID))
		if o.asJSON {
			output.PrintJson(f.IOStreams.Out, result)
			return nil
		}
		writeMutationDryRunText(f.IOStreams.Out, result)
		return nil
	}

	if err := applyDelete(ctx, client, remoteSubscriptionID, identity, before, o.yes); err != nil {
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
// remote_before/Hint purposes; this does not re-fetch it.
func applyDelete(ctx context.Context, svc deleteSubscriptionAPI, remoteSubscriptionID string, identity core.Identity, before *subscriptionRow, yes bool) error {
	if !yes {
		return errDeleteConfirmationRequired(remoteSubscriptionID, identity, before)
	}
	return doDeleteSubscription(ctx, svc, remoteSubscriptionID)
}

// doDeleteSubscription issues the actual Delete call. Unlike Patch/Renew/
// Reactivate, DeleteSubscriptionResp carries no Data/SubscriptionDetail at
// all — there is nothing to unwrap on success, so this returns
// only an error. Any error svc.Delete returns (transport, or an
// already-classified typed business failure from
// SubscriptionClient.Delete) is passed through unchanged.
func doDeleteSubscription(ctx context.Context, svc deleteSubscriptionAPI, remoteSubscriptionID string) error {
	req := larkeventv1.NewDeleteSubscriptionReqBuilder().SubscriptionId(remoteSubscriptionID).Build()
	_, err := svc.Delete(ctx, req)
	return err
}

// errDeleteConfirmationRequired implements the high-risk-write
// confirmation gate for delete: category confirmation (exit code 10),
// risk high-risk-write, and a Hint naming the affected
// remote_subscription_id/event_key/identity/current state (pids omitted —
// no bus/local-consumer registry exists yet to know them) plus
// the required "not a substitute for stopping local consumers" caveat.
func errDeleteConfirmationRequired(remoteSubscriptionID string, identity core.Identity, before *subscriptionRow) error {
	return errs.NewConfirmationRequiredError(errs.RiskHighRiskWrite, "event subscription delete",
		"deleting remote Subscription %s requires confirmation", remoteSubscriptionID).
		WithHint("this permanently deletes shared remote Subscription %s (event_key=%s, identity=%s, current state=%s); it is not a substitute for stopping local consumers — stop any local `event consume` process using it separately with `lark-cli event stop`; re-run the same command with --yes after a human has confirmed", remoteSubscriptionID, before.EventKey, identity, before.Remote.State)
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
