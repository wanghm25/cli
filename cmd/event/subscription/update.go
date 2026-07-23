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
	"github.com/larksuite/cli/internal/core"
	eventlib "github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/output"
)

// updateSubscriptionAPI is the subset of *eventlib.SubscriptionClient this
// command calls: Get (the remote read this command always performs first —
// the CLI-side invariant that read is hard-required alongside write
// for every mutating subscription command, used here both to detect a
// suspended target before ever calling Patch and to report remote_before/
// impact for --dry-run) and Patch (the actual write). See listSubscriptionsAPI
// (list.go) for the test-seam rationale — same pattern, this file's own pair
// of methods.
type updateSubscriptionAPI interface {
	Get(ctx context.Context, req *larkeventv1.GetSubscriptionReq) (*larkeventv1.GetSubscriptionResp, error)
	Patch(ctx context.Context, req *larkeventv1.PatchSubscriptionReq) (*larkeventv1.PatchSubscriptionResp, error)
}

// updateOpts holds `event subscription update`'s flag values.
type updateOpts struct {
	includeResourceData bool
	dryRun              bool
	yes                 bool
	asJSON              bool
}

// NewCmdUpdate builds `event subscription update <remote_subscription_id>`.
// The candidate SDK's Patch call only ever touches
// payload_options (the Patch body has no encrypt field), so
// --include-resource-data is update's only mutable input — and, since a
// caller must pass an explicit intent for that one field, it is required
// rather than defaulted (unlike create's, which defaults to false).
//
// Like create, update requires BOTH event:subscription:read and
// event:subscription:write: it always reads the target's
// current remote state first (Get) — both to refuse updating a suspended
// subscription (typed failed_precondition guiding `reactivate`
// instead of silently no-op'ing or corrupting state) and to report
// remote_before/impact for --dry-run.
//
// Unlike create/renew/reactivate, update is a high-risk write on a shared
// remote resource: without --yes it returns a typed
// ConfirmationRequiredError (category confirmation, exit code 10) instead of
// proceeding; --yes asserts a human already confirmed.
func NewCmdUpdate(f *cmdutil.Factory) *cobra.Command {
	var o updateOpts
	cmd := &cobra.Command{
		Use:   "update <remote_subscription_id>",
		Short: "Update a remote event Subscription's payload options",
		Long: `Update the payload_options of an existing remote Subscription by its
remote_subscription_id. The candidate SDK's Patch call only ever touches
payload_options — --include-resource-data is the only mutable field, and it
must be passed explicitly (true or false); there is nothing else to update.

IDENTITY: --as user|bot|auto, resolved to one effective identity (no
per-template check — this command carries no EventKey context).

SCOPE: requires BOTH event:subscription:read and event:subscription:write:
this command always reads the current remote state first, both to detect a
suspended target (which update refuses to touch — run 'event subscription
reactivate <remote_subscription_id>' first) and to report impact for
--dry-run.

ALLOWED VALUES: --include-resource-data true|false (required — no default is
silently applied). --include-resource-data=true is refused: encryption is set
only at create time and cannot be added, changed, or removed by update, so
switching include_resource_data / encryption on an existing
Subscription is not supported here — a typed failed_precondition guides you to
delete + recreate (after a human confirms) instead.

OUTPUT: {operation, remote_subscription_id, subscription{...}, next_action}.

NEXT STEP: run 'event subscription reactivate <remote_subscription_id>'
first if the target is suspended — update refuses to touch a suspended
Subscription.

SAFETY: this is a high-risk write on a resource other identities/processes
may share — without --yes it returns a confirmation-required error (exit
code 10) instead of proceeding; pass --yes only after a human has
confirmed. Use --dry-run to preview the plan without changing anything.`,
		Example: `  lark-cli event subscription update sub_xxx --include-resource-data false --dry-run --as bot --json
  lark-cli event subscription update sub_xxx --include-resource-data false --yes --as bot --json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runUpdate(cmd, f, args[0], o)
		},
	}

	cmd.Flags().BoolVar(&o.includeResourceData, "include-resource-data", false,
		"New value for whether to include resource data in delivered events (required: pass explicitly). true is refused: encryption is set only at create time, so switching include_resource_data / encryption on an existing subscription is not supported via update — delete + recreate (after a human confirms) instead.")
	cmd.Flags().BoolVar(&o.dryRun, "dry-run", false,
		"Preview the plan (identity/scope preflight + remote read + impact analysis) without updating anything")
	cmd.Flags().BoolVar(&o.yes, "yes", false, "Confirm this high-risk write (required unless --dry-run); only pass this after a human has confirmed")
	cmd.Flags().BoolVar(&o.asJSON, "json", false, "Emit the result as JSON (for AI / scripts)")
	addAsFlag(cmd)
	cmdutil.SetRisk(cmd, "write")

	return cmd
}

func runUpdate(cmd *cobra.Command, f *cmdutil.Factory, remoteSubscriptionID string, o updateOpts) error {
	if strings.TrimSpace(remoteSubscriptionID) == "" {
		return errEmptyRemoteSubscriptionID()
	}
	if !cmd.Flags().Changed("include-resource-data") {
		return errs.NewValidationError(errs.SubtypeInvalidArgument,
			"--include-resource-data must be passed explicitly (true or false); Patch only changes payload_options, so there is nothing to update without it").
			WithParam("--include-resource-data").
			WithHint("retry with --include-resource-data=true or --include-resource-data=false")
	}
	// The CLI refuses to switch
	// include_resource_data / encryption ON via update. This is a pure local
	// check, independent of identity/scope/remote state, so a rejected request
	// never causes any network activity at all.
	if o.includeResourceData {
		return errUpdateCannotSwitchEncryption(remoteSubscriptionID)
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
		result := buildMutationDryRunResult("update", remoteSubscriptionID, identity, before,
			updatePlannedAction(before), updateLocalImpactNote, updateDryRunNextAction(remoteSubscriptionID, identity, before))
		if o.asJSON {
			output.PrintJson(f.IOStreams.Out, result)
			return nil
		}
		writeMutationDryRunText(f.IOStreams.Out, result)
		return nil
	}

	detail, err := applyUpdate(ctx, client, remoteSubscriptionID, identity, before, o.includeResourceData, o.yes)
	if err != nil {
		return err
	}
	result := buildMutationResult("update", detail, updateNextAction(remoteSubscriptionID, identity))
	if o.asJSON {
		output.PrintJson(f.IOStreams.Out, result)
		return nil
	}
	writeMutationResultText(f.IOStreams.Out, "Updated", result)
	return nil
}

// applyUpdate is the write-capable half of update's non-dry-run path: the
// suspended guard, then the confirmation gate, then the actual Patch call —
// factored out from runUpdate (which only parses flags and builds the real
// network-capable client) so this decision sequence is directly testable
// against a fake updateSubscriptionAPI, mirroring create.go's
// createOrReuseSubscription. before is the remote state runUpdate already
// fetched via Get for --dry-run/remote_before purposes; this does not
// re-fetch it.
//
// The suspended guard is a hard block regardless of yes — there is no point
// confirming a write that cannot proceed. It runs before the
// confirmation gate so a suspended target never even reaches "requires
// confirmation", and Patch is never called in either failing case.
func applyUpdate(ctx context.Context, svc updateSubscriptionAPI, remoteSubscriptionID string, identity core.Identity, before *subscriptionRow, includeResourceData bool, yes bool) (*larkeventv1.SubscriptionDetail, error) {
	if before.Remote.State == "suspended" {
		return nil, errUpdateSuspended(remoteSubscriptionID, identity, before.Remote.SuspensionReason)
	}
	if !yes {
		return nil, errUpdateConfirmationRequired(remoteSubscriptionID, identity, before)
	}
	return doPatchSubscription(ctx, svc, remoteSubscriptionID, includeResourceData)
}

// doPatchSubscription issues the actual Patch call and unwraps its
// response. Any error svc.Patch returns (transport, or an already-classified
// typed business failure from SubscriptionClient.Patch) is passed
// through unchanged.
func doPatchSubscription(ctx context.Context, svc updateSubscriptionAPI, remoteSubscriptionID string, includeResourceData bool) (*larkeventv1.SubscriptionDetail, error) {
	body := larkeventv1.NewPatchSubscriptionReqBodyBuilder().
		PayloadOptions(larkeventv1.NewPayloadOptionsBuilder().IncludeResourceData(includeResourceData).Build()).
		Build()
	req := larkeventv1.NewPatchSubscriptionReqBuilder().SubscriptionId(remoteSubscriptionID).Body(body).Build()

	resp, err := svc.Patch(ctx, req)
	if err != nil {
		return nil, err
	}
	var detail *larkeventv1.SubscriptionDetail
	if resp != nil && resp.Data != nil {
		detail = resp.Data.Subscription
	}
	if detail == nil {
		return nil, errs.NewInternalError(errs.SubtypeInvalidResponse,
			"subscription update reported success but returned no subscription data")
	}
	return detail, nil
}

// errUpdateCannotSwitchEncryption reports the CLI's refusal to switch
// include_resource_data or
// encryption on an existing remote Subscription via update. `encrypt` is a
// Create-only field (Patch carries no encrypt), so update could never add an
// encrypt_key to accompany a newly-enabled include_resource_data=true — and
// the CLI will not leave a Subscription in an inconsistent state (resource
// data toggled while its key can be neither added nor removed). Enabling
// resource-data/encryption, or rotating a key, is therefore only ever done by
// deleting the Subscription and creating a new one (or creating a separate new
// Subscription), after a human confirms.
//
// This is a permanent
// by-design rejection, not a "not yet supported / retry later" defer, so it
// deliberately drops the old resource_data_encryption_deferred reason. Like
// that gate it is a pure local check (no identity/scope/remote read), so a
// rejected request never touches the network.
func errUpdateCannotSwitchEncryption(remoteSubscriptionID string) error {
	return errs.NewValidationError(errs.SubtypeFailedPrecondition,
		"cannot change include_resource_data or encryption on an existing subscription via update").
		WithParam("--include-resource-data").
		WithHint("encryption is set only when a subscription is created and can never be added, changed, or removed afterward; to switch include_resource_data or enable/rotate encryption a human must confirm, then delete this subscription and create a new one (or create a separate new subscription) — e.g. `lark-cli event subscription delete %s` then `lark-cli event subscription create <refined-event-key> --include-resource-data=true`", remoteSubscriptionID)
}

// errUpdateSuspended implements the suspended guard: Patch is
// never called against a suspended target; the caller is guided to
// `reactivate` instead of silently no-op'ing or attempting a write the
// server would reject anyway.
func errUpdateSuspended(remoteSubscriptionID string, identity core.Identity, reason string) error {
	return errs.NewValidationError(errs.SubtypeFailedPrecondition,
		"remote Subscription %s is suspended (suspension_reason=%s); it cannot be updated while suspended",
		remoteSubscriptionID, reason).
		WithParam("remote_subscription_id").
		WithHint("run `lark-cli event subscription reactivate %s --as %s` first, then retry update", remoteSubscriptionID, identity)
}

// errUpdateConfirmationRequired implements the high-risk-write
// confirmation gate for update: category confirmation (exit code 10, see
// internal/output/exitcode.go's ExitCodeForCategory), risk high-risk-write,
// and a Hint naming the affected remote_subscription_id/event_key/identity/
// current state (pids are omitted: no bus/local-consumer registry
// exists yet to know them, mirroring mapSubscriptionDetail's Local field
// staying nil/omitted for the same reason — see subscription.go's doc
// comment on subscriptionRow).
func errUpdateConfirmationRequired(remoteSubscriptionID string, identity core.Identity, before *subscriptionRow) error {
	return errs.NewConfirmationRequiredError(errs.RiskHighRiskWrite, "event subscription update",
		"updating remote Subscription %s requires confirmation", remoteSubscriptionID).
		WithHint("this changes payload_options on shared remote Subscription %s (event_key=%s, identity=%s, current state=%s); re-run the same command with --yes after a human has confirmed", remoteSubscriptionID, before.EventKey, identity, before.Remote.State)
}

// updatePlannedAction is --dry-run's planned_change.action value: "update"
// normally, or the informational "blocked_suspended" when the target is
// suspended. Like create.go's own conflict/
// suspended dry-run handling, dry-run always reports the plan
// informationally rather than erroring on remote business state — only the
// preflight steps themselves (identity/scope/flag-shape/encryption-check, already
// passed by the time this is called) are real dry-run failures. The real
// (non-dry-run) run's equivalent case DOES error — see errUpdateSuspended,
// called from runUpdate's non-dry-run branch only.
func updatePlannedAction(before *subscriptionRow) string {
	if before.Remote.State == "suspended" {
		return "blocked_suspended"
	}
	return "update"
}

const updateLocalImpactNote = "`event subscription update` only changes payload_options on the remote Subscription; it never starts, stops, or changes a local `event consume` process."

func updateDryRunNextAction(remoteSubscriptionID string, identity core.Identity, before *subscriptionRow) string {
	if before.Remote.State == "suspended" {
		return fmt.Sprintf("this update would be rejected while suspended; run `lark-cli event subscription reactivate %s --as %s` first", remoteSubscriptionID, identity)
	}
	return fmt.Sprintf("run without --dry-run (and with --yes after a human confirms) to apply this change to remote_subscription_id=%s", remoteSubscriptionID)
}

func updateNextAction(remoteSubscriptionID string, identity core.Identity) string {
	return fmt.Sprintf("run `lark-cli event subscription get %s --as %s --json` to confirm the updated payload_options", remoteSubscriptionID, identity)
}
