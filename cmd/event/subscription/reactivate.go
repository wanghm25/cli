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

// reactivateSubscriptionAPI is the subset of *eventlib.SubscriptionClient
// this command calls: Get (the remote read this command always performs
// first, per spec §3.5's CLI-side read+write invariant, to report
// remote_before/impact for --dry-run) and Reactivate (the actual write,
// which resumes delivery on a suspended subscription). See
// listSubscriptionsAPI (list.go) for the test-seam rationale.
type reactivateSubscriptionAPI interface {
	Get(ctx context.Context, req *larkeventv1.GetSubscriptionReq) (*larkeventv1.GetSubscriptionResp, error)
	Reactivate(ctx context.Context, req *larkeventv1.ReactivateSubscriptionReq) (*larkeventv1.ReactivateSubscriptionResp, error)
}

// reactivateOpts holds `event subscription reactivate`'s flag values (spec
// §3.1).
type reactivateOpts struct {
	dryRun bool
	asJSON bool
}

// NewCmdReactivate builds `event subscription reactivate
// <remote_subscription_id>` (spec §3.2.6/§3.6). Like create/update/renew/
// delete, reactivate requires BOTH event:subscription:read and
// event:subscription:write (spec §3.5): this command always reads the
// current remote state first (Get) to report remote_before/impact for
// --dry-run — a CLI-side design invariant, not an OAPI requirement.
//
// Unlike update/delete, reactivate is not a high-risk confirmation-gated
// action (spec §3.7: "reactivate 为恢复") — it does not expose --yes and
// never returns a ConfirmationRequiredError. There is no `suspend` command:
// the server exposes no such operation, so the only path back from
// suspended is reactivate.
func NewCmdReactivate(f *cmdutil.Factory) *cobra.Command {
	var o reactivateOpts
	cmd := &cobra.Command{
		Use:   "reactivate <remote_subscription_id>",
		Short: "Resume delivery on a suspended remote event Subscription",
		Long: `Reactivate an existing remote Subscription by its remote_subscription_id,
resuming delivery after it was suspended. Requires BOTH the
event:subscription:read and event:subscription:write scopes: this command
always reads the current remote state first to report impact for --dry-run.

reactivate only resumes remote delivery — it is not a high-risk
confirmation-gated action and does not accept --yes.

Use --dry-run to preview the plan without reactivating anything.`,
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
		result := buildMutationDryRunResult("reactivate", remoteSubscriptionID, identity, before,
			"reactivate", reactivateLocalImpactNote,
			fmt.Sprintf("run without --dry-run to reactivate remote_subscription_id=%s", remoteSubscriptionID))
		if o.asJSON {
			output.PrintJson(f.IOStreams.Out, result)
			return nil
		}
		writeMutationDryRunText(f.IOStreams.Out, result)
		return nil
	}

	detail, err := doReactivateSubscription(ctx, client, remoteSubscriptionID)
	if err != nil {
		return err
	}
	result := buildMutationResult("reactivate", detail,
		fmt.Sprintf("run `lark-cli event subscription get %s --as %s --json` to confirm it is active again; start a local consumer with `lark-cli event consume <refined EventKey> --as %s` if none is running", remoteSubscriptionID, identity, identity))
	if o.asJSON {
		output.PrintJson(f.IOStreams.Out, result)
		return nil
	}
	writeMutationResultText(f.IOStreams.Out, "Reactivated", result)
	return nil
}

// doReactivateSubscription issues the actual Reactivate call and unwraps
// its response. Any error svc.Reactivate returns (transport, or an
// already-classified typed business failure from Task 7's
// SubscriptionClient.Reactivate) is passed through unchanged.
func doReactivateSubscription(ctx context.Context, svc reactivateSubscriptionAPI, remoteSubscriptionID string) (*larkeventv1.SubscriptionDetail, error) {
	req := larkeventv1.NewReactivateSubscriptionReqBuilder().SubscriptionId(remoteSubscriptionID).Build()
	resp, err := svc.Reactivate(ctx, req)
	if err != nil {
		return nil, err
	}
	var detail *larkeventv1.SubscriptionDetail
	if resp != nil && resp.Data != nil {
		detail = resp.Data.Subscription
	}
	if detail == nil {
		return nil, errs.NewInternalError(errs.SubtypeInvalidResponse,
			"subscription reactivate reported success but returned no subscription data")
	}
	return detail, nil
}

const reactivateLocalImpactNote = "`event subscription reactivate` only resumes remote delivery; it never starts, stops, or changes a local `event consume` process — a local consumer may still need to be (re)started separately."
