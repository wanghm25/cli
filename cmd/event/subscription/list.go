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

	"github.com/larksuite/cli/internal/cmdutil"
	eventlib "github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/buslocal"
	larkgw "github.com/larksuite/cli/internal/event/platform/lark"
	subown "github.com/larksuite/cli/internal/event/subscription"
	"github.com/larksuite/cli/internal/output"
)

// listSubscriptionsAPI is the subset of the domain Gateway port this command
// calls. It exists purely as a test seam: tests substitute a fake implementing
// just this method, so the request-building/response-mapping logic
// (listSubscriptions) is exercised without a real *lark.Client or network call
// (the whole management plane must be testable via a fake gateway).
// *larkgw.Gateway (the platform/lark adapter) satisfies this interface
// structurally.
type listSubscriptionsAPI interface {
	List(ctx context.Context, params subown.ListParams) (*subown.SubscriptionPage, error)
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
  lark-cli event subscription list --event-key im.message.example_v1 --page-size 20 --as bot --json`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runList(cmd, f, o)
		},
	}

	cmd.Flags().StringVar(&o.state, "state", "", "Filter by remote subscription state: active | suspended | expired")
	cmd.Flags().StringVar(&o.eventKey, "event-key", "", "Filter by EventKey: accepts a materialized event_key straight from list output, or a raw event_type; resolved to the OAPI event_type for the server-side filter")
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

	uat, _, err := resolveUATAndCheckScopes(ctx, f, cfg.AppID, identity, subscriptionReadScopes)
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

	// Command-rendering context: the RESOLVED profile + identity, so the
	// next-page action targets the same app + identity, not the default (see
	// eventlib.CommandContext). cfg.ProfileName round-trips as a --profile value.
	cmdCtx := eventlib.CommandContext{Profile: cfg.ProfileName, Identity: identity}

	// Best-effort local-consumer facts: a down/unreachable bus yields none and
	// never fails this read-only command.
	result, err := listSubscriptions(ctx, client, o, cmdCtx, queryLocalConsumers())
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

// listSubscriptions builds the gateway ListParams from o, calls svc.List, and
// maps the page into this package's stable JSON shape. svc is the
// listSubscriptionsAPI test seam so this mapping is unit-tested against a fake,
// without a real *lark.Client. --event-key filters on the OAPI event_type
// server-side. localConsumers is the already-queried (best-effort) set of
// running local consumers; each row is additively annotated with the one(s)
// bound to its remote_subscription_id (nil/none leaves `local` omitted).
func listSubscriptions(ctx context.Context, svc listSubscriptionsAPI, o listOpts, cmdCtx eventlib.CommandContext, localConsumers []buslocal.Consumer) (*listResult, error) {
	page, err := svc.List(ctx, subown.ListParams{
		State:     o.state,
		EventType: listEventTypeFilter(o.eventKey),
		PageToken: o.pageToken,
		PageSize:  o.pageSize,
	})
	if err != nil {
		return nil, err
	}

	result := &listResult{Subscriptions: []subscriptionRow{}}
	if page == nil {
		return result, nil
	}
	for _, item := range page.Items {
		row := mapRemoteSubscription(item)
		row.Local = localViewFor(localConsumers, row.RemoteSubscriptionID)
		result.Subscriptions = append(result.Subscriptions, row)
	}
	result.HasMore = page.HasMore
	result.NextPageToken = page.NextPageToken
	if result.HasMore && result.NextPageToken != "" {
		result.NextAction = listNextAction(o, cmdCtx, result.NextPageToken)
	}
	return result, nil
}

// listEventTypeFilter resolves the --event-key value to the OAPI event_type to
// filter on. It accepts BOTH a raw event_type AND a materialized event_key from
// a previous list's output (e.g. "im.message.created_v1/chat-id/oc_xxx"): a
// resolvable key maps to its registered event_type; anything else (a bare
// refined base, or a server-defined type this CLI does not model) passes through
// verbatim so the filter still works. Empty stays empty (no filter). This makes
// the list output's `event_key` directly re-composable into `--event-key`.
func listEventTypeFilter(eventKey string) string {
	if eventKey == "" {
		return ""
	}
	if resolved, err := eventlib.ResolveEventKey(eventKey); err == nil && resolved.Definition != nil {
		return resolved.Definition.EventType
	}
	return eventKey
}

// listNextAction builds the fully-composable next-page command, carrying EVERY
// query-determining input from THIS invocation — not just --page-token but the
// filters AND the global app/identity context (via cmdCtx.CLIHead's --profile +
// cmdCtx.AsFlag's --as) — so the suggested command reproduces the same query,
// against the same app + identity, on the next page. --event-key echoes the
// caller's original input (which listEventTypeFilter accepts on the way back in).
func listNextAction(o listOpts, cmdCtx eventlib.CommandContext, nextToken string) string {
	cmd := cmdCtx.CLIHead() + " event subscription list --page-token " + nextToken
	if o.state != "" {
		cmd += " --state " + o.state
	}
	if o.eventKey != "" {
		cmd += " --event-key " + o.eventKey
	}
	if o.pageSize > 0 {
		cmd += fmt.Sprintf(" --page-size %d", o.pageSize)
	}
	cmd += cmdCtx.AsFlag()
	cmd += " --json"
	return "run `" + cmd + "` for the next page"
}

func writeListText(out io.Writer, result *listResult) {
	if len(result.Subscriptions) == 0 {
		fmt.Fprintln(out, "No remote subscriptions found.")
		return
	}
	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "REMOTE_SUBSCRIPTION_ID\tEVENT_KEY\tIDENTITY\tSTATE\tEXPIRE_TIME\tLOCAL")
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
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", s.RemoteSubscriptionID, s.EventKey, identity, state, expire, formatLocalConsumersColumn(s.Local))
	}
	w.Flush()
	if result.HasMore && result.NextAction != "" {
		fmt.Fprintf(out, "\nMore results available — %s\n", result.NextAction)
	}
}
