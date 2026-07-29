// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"github.com/spf13/cobra"

	"github.com/larksuite/cli/cmd/event/subscription"
	"github.com/larksuite/cli/internal/cmdutil"
)

func NewCmdEvents(f *cmdutil.Factory) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "event",
		Short: "Consume and manage real-time events, including per-resource (refined) subscriptions",
		Long: `Unified event consumption system. Use 'event consume <EventKey>' to start consuming events.

Two shapes of EventKey:
  - legacy : consume directly, e.g. 'event consume im.message.receive_v1'.
  - refined (per-resource): the key must be materialized with a resource
    selector before it can be consumed or subscribed, e.g.
    'im.message.example_v1/chat-id/oc_xxx'. Check 'event schema <key> --json'
    for refined_subscription:true + key_templates before using one.

Subcommands:
  list          Discover EventKeys (marks refined_subscription:true + key_templates for refined ones)
  schema        Inspect one EventKey's params / output schema / templates
  consume       Start consuming events for an EventKey (streams NDJSON)
  status        Show local bus daemon + consumer status
  stop          Stop the local bus daemon
  subscription  Manage remote Subscription resources behind a refined EventKey
                (list/get/create/update/renew/reactivate/delete, keyed by
                remote_subscription_id) — independent of any local 'consume'
                process; see 'event subscription --help'.

SAFETY: Refined EventKey subscriptions are remote resources. Commands that
create, update, reactivate, or delete them can affect later consumers. Prefer
--help or --dry-run first when in doubt.

NEXT STEP: 'lark-cli event list' to see what's available, then
'lark-cli event schema <EventKey> --json' for details.`,
		// Without SilenceUsage, RunE errors print the full flag help banner.
		SilenceUsage: true,
	}

	cmd.AddCommand(NewCmdConsume(f))
	cmd.AddCommand(NewCmdList(f))
	cmd.AddCommand(NewCmdSchema(f))
	cmd.AddCommand(NewCmdStatus(f))
	cmd.AddCommand(NewCmdStop(f))
	cmd.AddCommand(NewCmdBus(f))
	cmd.AddCommand(subscription.NewCmdSubscription(f))

	return cmd
}
