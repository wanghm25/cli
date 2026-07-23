// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"text/tabwriter"

	"github.com/spf13/cobra"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/internal/cmdutil"
	eventlib "github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/output"
)

// listSubscriptionsAPI is the subset of *eventlib.SubscriptionClient this
// command calls. It exists purely as a test seam: tests substitute a fake
// implementing just this method, so the request-building/response-mapping
// logic (listSubscriptions) is exercised without a real *lark.Client or
// network call (the whole management plane must be
// testable via a fake client). eventlib.NewSubscriptionClient's return
// value satisfies this interface structurally.
type listSubscriptionsAPI interface {
	List(ctx context.Context, req *larkeventv1.ListSubscriptionReq) (*larkeventv1.ListSubscriptionResp, error)
}

// listOpts holds `event subscription list`'s flag values.
type listOpts struct {
	state     string
	eventKey  string
	pageSize  int
	pageToken string
	asJSON    bool
}

// NewCmdList builds `event subscription list`: read-only,
// requires event:subscription:read.
func NewCmdList(f *cmdutil.Factory) *cobra.Command {
	var o listOpts
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List remote event Subscriptions",
		Long: `List the platform's persistent remote Subscription resources visible to
the effective identity.

Use --state/--event-key to filter, and --page-size/--page-token to page
through results. Use 'event subscription get <remote_subscription_id>' for
full detail on one entry.

IDENTITY: --as user|bot|auto, resolved to one effective identity (no
per-template check — this command carries no EventKey context). user and
bot see different remote Subscriptions (each identity only sees its own
authority's records).

SCOPE: event:subscription:read.

ALLOWED VALUES: --state active|suspended|expired (server-defined; passed
through verbatim, not validated client-side).

OUTPUT: {subscriptions[]: {remote_subscription_id, event_key,
target_resource, identity, payload_options, remote{state, expire_time,
suspension_reason, create_time, update_time}}, has_more, next_page_token,
next_action}.

NEXT STEP: 'lark-cli event subscription get <remote_subscription_id> --json'
for one entry's full detail.

SAFETY: read-only; never writes, never requires --yes.`,
		Example: `  lark-cli event subscription list --as bot --json
  lark-cli event subscription list --state suspended --as bot --json
  lark-cli event subscription list --event-key im.message.created_v1 --page-size 20 --as bot --json`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runList(cmd, f, o)
		},
	}

	cmd.Flags().StringVar(&o.state, "state", "", "Filter by remote subscription state: active | suspended | expired")
	cmd.Flags().StringVar(&o.eventKey, "event-key", "", "Filter by EventKey (matched against the OAPI event_type; use 'event schema' to look up a key's event_type)")
	cmd.Flags().IntVar(&o.pageSize, "page-size", 0, "Max subscriptions per page (0 = server default)")
	cmd.Flags().StringVar(&o.pageToken, "page-token", "", "Resume from this page token (from a previous list response's next_page_token)")
	cmd.Flags().BoolVar(&o.asJSON, "json", false, "Emit the subscription list as JSON (for AI / scripts)")
	addAsFlag(cmd)
	cmdutil.SetRisk(cmd, "read")

	return cmd
}

func runList(cmd *cobra.Command, f *cmdutil.Factory, o listOpts) error {
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
	client, err := eventlib.NewSubscriptionClient(sdk, identity, uat)
	if err != nil {
		return err
	}

	result, err := listSubscriptions(ctx, client, o)
	if err != nil {
		return err
	}

	if o.asJSON {
		output.PrintJson(f.IOStreams.Out, result)
		return nil
	}
	writeListText(f.IOStreams.Out, result)
	return nil
}

// listResult is `event subscription list --json`'s top-level shape (task-8
// brief): subscriptions[] + has_more/next_page_token + next_action.
type listResult struct {
	Subscriptions []subscriptionRow `json:"subscriptions"`
	HasMore       bool              `json:"has_more"`
	NextPageToken string            `json:"next_page_token,omitempty"`
	NextAction    string            `json:"next_action,omitempty"`
}

// listSubscriptions builds the SDK ListSubscriptionReq from o, calls
// svc.List, and maps the response into this package's stable JSON shape.
// svc is the listSubscriptionsAPI test seam so this mapping is unit-tested
// against a fake, without a real *lark.Client.
func listSubscriptions(ctx context.Context, svc listSubscriptionsAPI, o listOpts) (*listResult, error) {
	builder := larkeventv1.NewListSubscriptionReqBuilder()
	if o.state != "" {
		builder = builder.State(o.state)
	}
	if o.eventKey != "" {
		builder = builder.EventType(o.eventKey)
	}
	if o.pageToken != "" {
		builder = builder.PageToken(o.pageToken)
	}
	if o.pageSize > 0 {
		builder = builder.PageSize(o.pageSize)
	}

	resp, err := svc.List(ctx, builder.Build())
	if err != nil {
		return nil, err
	}

	result := &listResult{Subscriptions: []subscriptionRow{}}
	if resp == nil || resp.Data == nil {
		return result, nil
	}
	for _, item := range resp.Data.Items {
		result.Subscriptions = append(result.Subscriptions, mapSubscriptionDetail(item))
	}
	if resp.Data.HasMore != nil {
		result.HasMore = *resp.Data.HasMore
	}
	if resp.Data.PageToken != nil {
		result.NextPageToken = *resp.Data.PageToken
	}
	if result.HasMore && result.NextPageToken != "" {
		result.NextAction = fmt.Sprintf("run `lark-cli event subscription list --page-token %s --json` for the next page", result.NextPageToken)
	}
	return result, nil
}

func writeListText(out io.Writer, result *listResult) {
	if len(result.Subscriptions) == 0 {
		fmt.Fprintln(out, "No remote subscriptions found.")
		return
	}
	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "REMOTE_SUBSCRIPTION_ID\tEVENT_KEY\tIDENTITY\tSTATE\tEXPIRE_TIME")
	for _, s := range result.Subscriptions {
		expire := "-"
		if s.Remote.ExpireTime != nil {
			expire = strconv.Itoa(*s.Remote.ExpireTime)
		}
		identity := s.Identity
		if identity == "" {
			identity = "-"
		}
		state := s.Remote.State
		if state == "" {
			state = "-"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", s.RemoteSubscriptionID, s.EventKey, identity, state, expire)
	}
	w.Flush()
	if result.HasMore {
		fmt.Fprintf(out, "\nMore results available; use --page-token %s to continue.\n", result.NextPageToken)
	}
}
