// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"strings"

	"github.com/spf13/cobra"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
)

// updateOpts holds `event subscription update`'s flag values.
type updateOpts struct {
	includeResourceData bool
}

// NewCmdUpdate builds `event subscription update <remote_subscription_id>`.
//
// The platform's update (Patch) API only accepts a new event filter now —
// this CLI does not support filter updates yet, and include_resource_data
// was never part of that request body to begin with: resource-data delivery
// (and the encryption it implies) is decided once, when a Subscription is
// created, and the platform gives no way to change it afterward in either
// direction. So this command has no successful path today.
// --include-resource-data stays as the one recognized flag purely so a
// caller reaching for it — this command's own previous behavior — gets a
// specific, actionable rejection instead of a generic "unknown flag" error.
func NewCmdUpdate(f *cmdutil.Factory) *cobra.Command {
	var o updateOpts
	cmd := &cobra.Command{
		Use:   "update <remote_subscription_id>",
		Short: "Update a remote event Subscription (no fields are updatable yet)",
		Long: `Update an existing remote Subscription identified by
remote_subscription_id.

This command has no successful path today: the platform's update API only
accepts a new event filter, and this CLI does not support filter updates
yet.

--include-resource-data is kept as the one recognized flag so trying to use
it fails with a specific, actionable error instead of "unknown flag":
resource-data delivery (and the encryption it implies) is decided once, when
a Subscription is created, and cannot be changed via update in either
direction.

This rejection is entirely local — it never resolves an identity, checks
scopes, reads remote state, or otherwise touches the network.

ALLOWED VALUES: --include-resource-data true|false — both are rejected.

NEXT STEP: to actually change resource-data delivery, a human must confirm,
then run 'event subscription delete <remote_subscription_id>' followed by
'event subscription create' with the desired --include-resource-data value.`,
		Example: `  lark-cli event subscription update sub_xxx --include-resource-data false`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runUpdate(cmd, args[0], o)
		},
	}

	cmd.Flags().BoolVar(&o.includeResourceData, "include-resource-data", false,
		"New value for whether to include resource data in delivered events. Always rejected: resource-data delivery is decided when a subscription is created and cannot be changed via update; delete + recreate after human confirmation instead.")
	cmdutil.SetRisk(cmd, "write")

	return cmd
}

// runUpdate is pure local validation: it never resolves an identity, checks
// scopes, reads remote state, or builds a network-capable client. There is
// currently nothing this command can successfully apply, so every path
// through it ends in a typed rejection.
func runUpdate(cmd *cobra.Command, remoteSubscriptionID string, o updateOpts) error {
	if strings.TrimSpace(remoteSubscriptionID) == "" {
		return errEmptyRemoteSubscriptionID()
	}
	if !cmd.Flags().Changed("include-resource-data") {
		return errs.NewValidationError(errs.SubtypeInvalidArgument,
			"--include-resource-data must be passed explicitly (true or false); it is the only flag this command currently recognizes").
			WithParam("--include-resource-data").
			WithHint("retry with --include-resource-data=true or --include-resource-data=false")
	}
	// include_resource_data / encryption is decided once, at create time;
	// the platform's update API has no field for it at all (Patch is
	// filter-only), so update can neither add it nor remove it. This is a
	// structural "update can never do this" fact about the request itself,
	// independent of identity, scope, or remote state — a rejected request
	// never causes any network activity.
	return errUpdateCannotSwitchEncryption(remoteSubscriptionID, o.includeResourceData)
}

// errUpdateCannotSwitchEncryption reports the CLI's permanent, by-design
// refusal to change include_resource_data (and the encryption it implies)
// on an existing remote Subscription via update, in either direction:
// `encrypt`/`encrypt_key` are create-only, and the platform's update API
// carries no field that could ever touch them. desiredIncludeResourceData is
// the value the caller actually asked for, echoed into the delete+recreate
// example command so the guidance is directionally correct either way.
//
// The CLI will not leave a Subscription in an inconsistent state (resource
// data toggled while its key can be neither added nor removed) — changing
// it, in either direction, is therefore only ever done by deleting the
// Subscription and creating a new one (or creating a separate new
// Subscription), after a human confirms.
func errUpdateCannotSwitchEncryption(remoteSubscriptionID string, desiredIncludeResourceData bool) error {
	return errs.NewValidationError(errs.SubtypeFailedPrecondition,
		"cannot change include_resource_data on an existing subscription via update").
		WithParam("--include-resource-data").
		WithHint("resource-data delivery (and the encryption it implies) is decided when a subscription is created and cannot be changed via update; after human confirmation, delete this subscription and create a new one (or create a separate new subscription) — e.g. `lark-cli event subscription delete %s` then `lark-cli event subscription create <refined-event-key> --include-resource-data=%t`", remoteSubscriptionID, desiredIncludeResourceData)
}
