// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/event/model"
	larkgw "github.com/larksuite/cli/internal/event/platform/lark"
	"github.com/larksuite/cli/internal/output"
)

// renewSubscriptionAPI is the subset of the platform/lark SubscriptionGateway
// this command calls: Get (the remote read this command always performs first,
// per the CLI-side read+write invariant, to report remote_before/
// impact for --dry-run) and Renew (the actual write, which only extends the
// subscription's TTL). See listSubscriptionsAPI (list.go) for the test-seam
// rationale.
type renewSubscriptionAPI interface {
	Get(ctx context.Context, remoteSubscriptionID string) (*model.RemoteSubscription, error)
	Renew(ctx context.Context, remoteSubscriptionID string) (*model.RemoteSubscription, error)
}

// renewOpts holds `event subscription renew`'s flag values.
type renewOpts struct {
	dryRun bool
	asJSON bool
}

// NewCmdRenew builds `event subscription renew <remote_subscription_id>`.
// Like create/reactivate/delete, renew requires
// BOTH event:subscription:read and event:subscription:write:
// this command always reads the current remote state first (Get) to report
// remote_before/impact for --dry-run — a CLI-side design invariant, not an
// OAPI requirement (a bare Renew call needs no prior read at the API
// level).
//
// Unlike delete, renew is not a high-risk confirmation-gated action
// — it does not expose --yes and never returns
// a ConfirmationRequiredError.
func NewCmdRenew(f *cmdutil.Factory) *cobra.Command {
	var o renewOpts
	cmd := &cobra.Command{
		Use:   "renew <remote_subscription_id>",
		Short: "Renew (extend the TTL of) a remote event Subscription",
		Long: `Renew an existing remote Subscription by its remote_subscription_id,
extending its TTL.

IDENTITY: --as user|bot|auto, resolved to one effective identity (no
per-template check — this command carries no EventKey context).

SCOPE: requires BOTH event:subscription:read and event:subscription:write:
this command always reads the current remote state first to report impact
for --dry-run (a CLI-side design choice, not a platform requirement).

OUTPUT: {operation, remote_subscription_id, subscription{...}, next_action}
— check subscription.remote.expire_time (unix seconds) for the new TTL.

NEXT STEP: 'lark-cli event subscription get <remote_subscription_id> --json'
to confirm the new expire_time.

SAFETY: renew only extends TTL — it is NOT a high-risk confirmation-gated
action and does not accept --yes. Use --dry-run to preview the plan without
renewing anything. Note: the bus also renews automatically (best-effort,
single attempt, no retry) on receiving an expiration reminder for a
Subscription with a matching active local consumer — see 'lark-cli event
status' for whether that already happened.`,
		Example: `  lark-cli event subscription renew sub_xxx --dry-run --as bot --json
  lark-cli event subscription renew sub_xxx --as bot --json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRenew(cmd, f, args[0], o)
		},
	}

	cmd.Flags().BoolVar(&o.dryRun, "dry-run", false,
		"Preview the plan (identity/scope preflight + remote read + impact analysis) without renewing anything")
	cmd.Flags().BoolVar(&o.asJSON, "json", false, "Emit the result as JSON (for AI / scripts)")
	addAsFlag(cmd)
	cmdutil.SetRisk(cmd, "write")

	return cmd
}

func runRenew(cmd *cobra.Command, f *cmdutil.Factory, remoteSubscriptionID string, o renewOpts) error {
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
	client, err := larkgw.NewSubscriptionGateway(sdk, identity, uat)
	if err != nil {
		return err
	}

	before, err := getSubscription(ctx, client, remoteSubscriptionID)
	if err != nil {
		return err
	}

	if o.dryRun {
		result := buildMutationDryRunResult("renew", remoteSubscriptionID, identity, before,
			"renew", false, renewLocalImpactNote,
			fmt.Sprintf("run without --dry-run to renew remote_subscription_id=%s", remoteSubscriptionID))
		if o.asJSON {
			output.PrintJson(f.IOStreams.Out, result)
			return nil
		}
		writeMutationDryRunText(f.IOStreams.Out, result)
		return nil
	}

	sub, err := doRenewSubscription(ctx, client, remoteSubscriptionID)
	if err != nil {
		return err
	}
	result := buildMutationResult("renew", *sub,
		fmt.Sprintf("run `lark-cli event subscription get %s --as %s --json` to confirm the new expire_time", remoteSubscriptionID, identity))
	if o.asJSON {
		output.PrintJson(f.IOStreams.Out, result)
		return nil
	}
	writeMutationResultText(f.IOStreams.Out, "Renewed", result)
	return nil
}

// doRenewSubscription issues the actual Renew call via the gateway, which
// builds the request, unwraps the response, and validates its required fields (a
// success response with no subscription / an empty remote_subscription_id -> a
// typed InvalidResponse). Any error it returns (transport, an already-classified
// business failure, or that validation) is passed through unchanged.
func doRenewSubscription(ctx context.Context, svc renewSubscriptionAPI, remoteSubscriptionID string) (*model.RemoteSubscription, error) {
	return svc.Renew(ctx, remoteSubscriptionID)
}

const renewLocalImpactNote = "`event subscription renew` only extends the remote Subscription's TTL; it never starts, stops, or changes a local `event consume` process."
