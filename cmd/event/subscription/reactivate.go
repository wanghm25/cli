// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/larksuite/cli/internal/cmdutil"
	eventlib "github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/model"
	larkgw "github.com/larksuite/cli/internal/event/platform/lark"
	"github.com/larksuite/cli/internal/output"
)

// reactivateSubscriptionAPI is the subset of the platform/lark
// SubscriptionGateway this command calls: Get (the remote read this command
// always performs first, per the CLI-side read+write invariant, to report
// remote_before/impact for --dry-run) and Reactivate (the actual write,
// which resumes delivery on a suspended subscription). See
// listSubscriptionsAPI (list.go) for the test-seam rationale.
type reactivateSubscriptionAPI interface {
	Get(ctx context.Context, remoteSubscriptionID string) (*model.RemoteSubscription, error)
	Reactivate(ctx context.Context, remoteSubscriptionID string) (*model.RemoteSubscription, error)
}

// reactivateOpts holds `event subscription reactivate`'s flag values.
type reactivateOpts struct {
	dryRun bool
	asJSON bool
}

// NewCmdReactivate builds `event subscription reactivate
// <remote_subscription_id>`. Like create/renew/
// delete, reactivate requires BOTH event:subscription:read and
// event:subscription:write: this command always reads the
// current remote state first (Get) to report remote_before/impact for
// --dry-run — a CLI-side design invariant, not an OAPI requirement.
//
// Unlike delete, reactivate is not a high-risk confirmation-gated
// action — it does not expose --yes and
// never returns a ConfirmationRequiredError. There is no `suspend` command:
// the server exposes no such operation, so the only path back from
// suspended is reactivate.
func NewCmdReactivate(f *cmdutil.Factory) *cobra.Command {
	var o reactivateOpts
	cmd := &cobra.Command{
		Use:   "reactivate <remote_subscription_id>",
		Short: "Resume delivery on a suspended remote event Subscription",
		Long: `Reactivate an existing remote Subscription by its remote_subscription_id,
resuming delivery after it was suspended.

IDENTITY: --as user|bot|auto, resolved to one effective identity (no
per-template check — this command carries no EventKey context).

SCOPE: requires BOTH event:subscription:read and event:subscription:write:
this command always reads the current remote state first to report impact
for --dry-run.

OUTPUT: {operation, remote_subscription_id, subscription{...}, next_action}.

NEXT STEP: a local 'event consume' process may still need to be (re)started
separately — reactivate only resumes remote delivery, it never starts,
stops, or changes a local consumer. Run 'lark-cli event status' to check.

SAFETY: reactivate only resumes remote delivery — it is NOT a high-risk
confirmation-gated action and does not accept --yes. There is no 'suspend'
command (the platform exposes none); the only path back from suspended is
this command. Use --dry-run to preview the plan without reactivating
anything.`,
		Example: `  lark-cli event subscription reactivate sub_xxx --dry-run --as bot --json
  lark-cli event subscription reactivate sub_xxx --as bot --json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runReactivate(cmd, f, args[0], o)
		},
	}

	cmd.Flags().BoolVar(&o.dryRun, "dry-run", false,
		"Preview the plan (identity/scope preflight + remote read + impact analysis) without reactivating anything")
	cmd.Flags().BoolVar(&o.asJSON, "json", false, "Emit the result as JSON (for AI / scripts)")
	addAsFlag(cmd)
	cmdutil.SetRisk(cmd, "write")

	return cmd
}

func runReactivate(cmd *cobra.Command, f *cmdutil.Factory, remoteSubscriptionID string, o reactivateOpts) error {
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
	cmdCtx := eventlib.CommandContext{Profile: cfg.ProfileName, Identity: identity}
	return applyReactivate(ctx, client, f.IOStreams.Out, remoteSubscriptionID, cmdCtx, o, before, scopesVerified)
}

// applyReactivate is reactivate's testable core: given the already-read remote
// state (before), it renders the --dry-run preview, the already-active NO-OP, or
// the real Reactivate + result. The no-op path must NOT call Reactivate — it
// mirrors the --dry-run's "noop" plan via the shared reactivateIsNoop predicate,
// so the preview and the execution can never disagree about whether a redundant
// Reactivate happens. It never reads remote state itself; runReactivate's
// getSubscription supplies before. Exercised against a fake reactivateSubscriptionAPI
// in tests.
func applyReactivate(ctx context.Context, svc reactivateSubscriptionAPI, out io.Writer, remoteSubscriptionID string, cmdCtx eventlib.CommandContext, o reactivateOpts, before *subscriptionRow, scopesVerified bool) error {
	if o.dryRun {
		// Plan from the OBSERVED remote state: an already-active subscription is a
		// no-op, not a fresh reactivation.
		plannedAction, nextAction := mutationDryRunPlan("reactivate", remoteSubscriptionID, before.Remote.State, cmdCtx)
		result := buildMutationDryRunResult("reactivate", remoteSubscriptionID, cmdCtx.Identity, scopesVerified, before,
			plannedAction, false, reactivateLocalImpactNote, nextAction)
		if o.asJSON {
			output.PrintJson(out, result)
			return nil
		}
		writeMutationDryRunText(out, result)
		return nil
	}

	switch classifyMutation("reactivate", before.Remote.State) {
	case mutationBlocked:
		// A state outside {suspended, active} (expired, empty, or any unknown
		// value) — fail closed rather than issue a Reactivate the platform would
		// reject with an unclassified error. The --dry-run showed this same
		// "blocked" plan (shared classifyMutation predicate).
		return errMutationBlockedState("reactivate", remoteSubscriptionID, before.Remote.State)
	case mutationNoop:
		// Already active — mirror the --dry-run's "noop": a real run must not issue
		// a redundant Reactivate the preview promised was unnecessary. Echo the
		// record we just read (before is already the mapped row).
		result := &mutationResult{
			Operation:            "reactivate",
			RemoteSubscriptionID: before.RemoteSubscriptionID,
			Subscription:         *before,
			NextAction:           fmt.Sprintf("remote_subscription_id=%s is already active; no reactivation was needed", remoteSubscriptionID),
		}
		if o.asJSON {
			output.PrintJson(out, result)
			return nil
		}
		writeMutationResultText(out, "No change to", result)
		return nil
	}

	sub, err := doReactivateSubscription(ctx, svc, remoteSubscriptionID)
	if err != nil {
		return err
	}
	result := buildMutationResult("reactivate", *sub,
		fmt.Sprintf("run `%s event subscription get %s --as %s --json` to confirm it is active again; start a local consumer with `%s event consume <refined EventKey> --as %s` if none is running", cmdCtx.CLIHead(), remoteSubscriptionID, cmdCtx.Identity, cmdCtx.CLIHead(), cmdCtx.Identity))
	if o.asJSON {
		output.PrintJson(out, result)
		return nil
	}
	writeMutationResultText(out, "Reactivated", result)
	return nil
}

// doReactivateSubscription issues the actual Reactivate call via the gateway,
// which builds the request, unwraps the response, and validates its required
// fields (a success response with no subscription / an empty
// remote_subscription_id -> a typed InvalidResponse). Any error it returns
// (transport, an already-classified business failure, or that validation) is
// passed through unchanged.
func doReactivateSubscription(ctx context.Context, svc reactivateSubscriptionAPI, remoteSubscriptionID string) (*model.RemoteSubscription, error) {
	return svc.Reactivate(ctx, remoteSubscriptionID)
}

const reactivateLocalImpactNote = "`event subscription reactivate` only resumes remote delivery; it never starts, stops, or changes a local `event consume` process — a local consumer may still need to be (re)started separately."
