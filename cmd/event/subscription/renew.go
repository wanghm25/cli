// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
	eventlib "github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/output"
)

// renewSubscriptionAPI is the subset of *eventlib.SubscriptionClient this
// command calls: Get (the remote read this command always performs first,
// per spec §3.5's CLI-side read+write invariant, to report remote_before/
// impact for --dry-run) and Renew (the actual write, which only extends the
// subscription's TTL). See listSubscriptionsAPI (list.go) for the test-seam
// rationale.
type renewSubscriptionAPI interface {
	Get(ctx context.Context, req *larkeventv1.GetSubscriptionReq) (*larkeventv1.GetSubscriptionResp, error)
	Renew(ctx context.Context, req *larkeventv1.RenewSubscriptionReq) (*larkeventv1.RenewSubscriptionResp, error)
}

// renewOpts holds `event subscription renew`'s flag values (spec §3.1).
type renewOpts struct {
	dryRun bool
	asJSON bool
}

// NewCmdRenew builds `event subscription renew <remote_subscription_id>`
// (spec §3.2.5/§3.6). Like create/update/reactivate/delete, renew requires
// BOTH event:subscription:read and event:subscription:write (spec §3.5):
// this command always reads the current remote state first (Get) to report
// remote_before/impact for --dry-run — a CLI-side design invariant, not an
// OAPI requirement (a bare Renew call needs no prior read at the API
// level).
//
// Unlike update/delete, renew is not a high-risk confirmation-gated action
// (spec §3.7: "renew 仅延 TTL") — it does not expose --yes and never returns
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
	client, err := eventlib.NewSubscriptionClient(sdk, identity, uat)
	if err != nil {
		return err
	}

	before, err := getSubscription(ctx, client, remoteSubscriptionID)
	if err != nil {
		return err
	}

	if o.dryRun {
		result := buildMutationDryRunResult("renew", remoteSubscriptionID, identity, before,
			"renew", renewLocalImpactNote,
			fmt.Sprintf("run without --dry-run to renew remote_subscription_id=%s", remoteSubscriptionID))
		if o.asJSON {
			output.PrintJson(f.IOStreams.Out, result)
			return nil
		}
		writeMutationDryRunText(f.IOStreams.Out, result)
		return nil
	}

	detail, err := doRenewSubscription(ctx, client, remoteSubscriptionID)
	if err != nil {
		return err
	}
	result := buildMutationResult("renew", detail,
		fmt.Sprintf("run `lark-cli event subscription get %s --as %s --json` to confirm the new expire_time", remoteSubscriptionID, identity))
	if o.asJSON {
		output.PrintJson(f.IOStreams.Out, result)
		return nil
	}
	writeMutationResultText(f.IOStreams.Out, "Renewed", result)
	return nil
}

// doRenewSubscription issues the actual Renew call and unwraps its
// response. Any error svc.Renew returns (transport, or an already-classified
// typed business failure from Task 7's SubscriptionClient.Renew) is passed
// through unchanged.
func doRenewSubscription(ctx context.Context, svc renewSubscriptionAPI, remoteSubscriptionID string) (*larkeventv1.SubscriptionDetail, error) {
	req := larkeventv1.NewRenewSubscriptionReqBuilder().SubscriptionId(remoteSubscriptionID).Build()
	resp, err := svc.Renew(ctx, req)
	if err != nil {
		return nil, err
	}
	var detail *larkeventv1.SubscriptionDetail
	if resp != nil && resp.Data != nil {
		detail = resp.Data.Subscription
	}
	if detail == nil {
		return nil, errs.NewInternalError(errs.SubtypeInvalidResponse,
			"subscription renew reported success but returned no subscription data")
	}
	return detail, nil
}

const renewLocalImpactNote = "`event subscription renew` only extends the remote Subscription's TTL; it never starts, stops, or changes a local `event consume` process."
