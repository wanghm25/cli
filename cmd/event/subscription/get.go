// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/event/model"
	larkgw "github.com/larksuite/cli/internal/event/platform/lark"
	"github.com/larksuite/cli/internal/output"
)

// getSubscriptionAPI is the subset of the platform/lark SubscriptionGateway
// this command calls. See listSubscriptionsAPI (list.go) for the rationale —
// same test-seam pattern, one method.
type getSubscriptionAPI interface {
	Get(ctx context.Context, remoteSubscriptionID string) (*model.RemoteSubscription, error)
}

// NewCmdGet builds `event subscription get <remote_subscription_id>`:
// read-only, requires event:subscription:read.
func NewCmdGet(f *cmdutil.Factory) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "get <remote_subscription_id>",
		Short: "Show one remote event Subscription",
		Long: `Show the current remote state of one Subscription by its
remote_subscription_id (as returned by 'event subscription list' or
'create').

IDENTITY: --as user|bot|auto, resolved to one effective identity (no
per-template check — this command carries no EventKey context).

SCOPE: event:subscription:read.

OUTPUT: {remote_subscription_id, event_key, target_resource, identity,
payload_options, remote{state, expire_time, suspension_reason, create_time,
update_time}}. The 'local' field is reserved for a future change once
local-consumer association ('event consume' <-> remote subscription)
exists; it is always omitted for now.

NEXT STEP: 'lark-cli event status' shows any LOCAL consumer currently bound
to this same remote_subscription_id, if one is running.

SAFETY: read-only; never writes, never requires --yes.`,
		Example: `  lark-cli event subscription get sub_xxx --as bot --json`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runGet(cmd, f, args[0], asJSON)
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "Emit the subscription detail as JSON (for AI / scripts)")
	addAsFlag(cmd)
	cmdutil.SetRisk(cmd, "read")
	return cmd
}

func runGet(cmd *cobra.Command, f *cmdutil.Factory, remoteSubscriptionID string, asJSON bool) error {
	if strings.TrimSpace(remoteSubscriptionID) == "" {
		return errs.NewValidationError(errs.SubtypeInvalidArgument,
			"remote_subscription_id must not be empty").
			WithParam("remote_subscription_id").
			WithHint("pass the remote_subscription_id from `lark-cli event subscription list --json`")
	}

	ctx := cmd.Context()
	cfg, err := f.Config()
	if err != nil {
		return err
	}

	identity, err := resolveEffectiveIdentity(cmd, f)
	if err != nil {
		return err
	}

	uat, err := resolveUATAndCheckScopes(ctx, f, cfg.AppID, identity, subscriptionReadScopes)
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

	row, err := getSubscription(ctx, client, remoteSubscriptionID)
	if err != nil {
		return err
	}

	if asJSON {
		output.PrintJson(f.IOStreams.Out, row)
		return nil
	}
	writeGetText(f.IOStreams.Out, row)
	return nil
}

// getSubscription calls svc.Get and maps the domain RemoteSubscription into
// this package's stable JSON row shape. svc is the getSubscriptionAPI test seam
// so this is unit-tested against a fake, without a real *lark.Client.
//
// Any error svc.Get returns is passed through unchanged: by the time this
// function is reached, the platform/lark gateway has already turned a transport
// failure, an OAPI business failure (e.g. an unknown/nonexistent
// remote_subscription_id), or a wire anomaly (a success response carrying no
// subscription / an empty remote_subscription_id -> typed InvalidResponse) into
// a typed errs.* error — this command layer must not swallow, downgrade, or
// re-wrap it, so it never renders an all-empty row as if it were real data.
func getSubscription(ctx context.Context, svc getSubscriptionAPI, remoteSubscriptionID string) (*subscriptionRow, error) {
	sub, err := svc.Get(ctx, remoteSubscriptionID)
	if err != nil {
		return nil, err
	}
	row := mapRemoteSubscription(*sub)
	return &row, nil
}

func writeGetText(out io.Writer, row *subscriptionRow) {
	fmt.Fprintf(out, "Remote Subscription ID: %s\n", row.RemoteSubscriptionID)
	fmt.Fprintf(out, "Event Key:               %s\n", row.EventKey)
	if row.TargetResource != "" {
		fmt.Fprintf(out, "Target Resource:         %s\n", row.TargetResource)
	}
	if row.Identity != "" {
		fmt.Fprintf(out, "Identity:                %s\n", row.Identity)
	}
	state := row.Remote.State
	if state == "" {
		state = "-"
	}
	fmt.Fprintf(out, "State:                   %s\n", state)
	if row.Remote.ExpireTime != nil {
		fmt.Fprintf(out, "Expire Time:             %s (unix seconds)\n", strconv.Itoa(*row.Remote.ExpireTime))
	}
	if row.Remote.SuspensionReason != "" {
		fmt.Fprintf(out, "Suspension Reason:       %s\n", row.Remote.SuspensionReason)
	}
	if row.PayloadOptions != nil {
		fmt.Fprintf(out, "Include Resource Data:   %v\n", row.PayloadOptions.IncludeResourceData)
	}
	if len(row.Filter) > 0 {
		fmt.Fprintf(out, "Filter:                  %s\n", row.Filter)
	}
}
