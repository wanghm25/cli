// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

// Package subscription implements `lark-cli event subscription` — the
// management-plane commands for remote event Subscription resources.
//
// This file holds the root `subscription` command group plus the pieces
// shared by every subcommand: --as identity resolution + local scope
// pre-check, and the CLI-facing JSON row shape for one remote
// SubscriptionDetail (used by both `list` and `get`).
package subscription

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/auth"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/credential"
	eventlib "github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/model"
)

// EventKeyUnavailable is the explicit marker emitted in the event_key field of
// a subscription row (and status's remote_subscription) when a remote
// subscription's event_type + target_resource cannot be reversed to exactly one
// registered, executable EventKey (catalog.ReverseResolve reported ok=false).
// It is deliberately a fixed, non-key-shaped token so an AI consumer can tell an
// executable EventKey apart from "no canonical key is available" without having
// to infer it from an empty string — and so a raw event_type is never presented
// where an executable event_key is expected. A real EventKey is a dotted
// identifier and can never collide with this literal.
const EventKeyUnavailable = "unavailable"

// NewCmdSubscription builds the `event subscription` command group: the
// read-only list/get pair plus the mutating create/update/renew/
// reactivate/delete subcommands.
func NewCmdSubscription(f *cmdutil.Factory) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "subscription",
		Short: "Manage remote event Subscriptions behind a refined EventKey",
		Long: `Manage the platform's persistent remote Subscription resources that back
refined (per-resource) event delivery — as opposed to 'event consume', which
starts a local process that consumes already-delivered events.

Use 'list' / 'get <remote_subscription_id>' to inspect what is currently
subscribed remotely, 'create <refined EventKey>' to create (or idempotently
reuse) one, 'update <remote_subscription_id>' to change its server-side
event filter (--filter/--clear-filter; include_resource_data is not
updatable), 'renew'/'reactivate' to extend its TTL or resume delivery, and
'delete <remote_subscription_id>' to remove it.

IDENTITY: --as user|bot|auto on every subcommand, resolved to one effective
identity per call — user and bot tokens are never mixed within one call.
Every subcommand EXCEPT 'create' (list/get/update/renew/reactivate/delete)
carries no EventKey/template context, so there is no extra per-template
identity check for them; only 'create' enforces one, since it is the sole
subcommand that takes an EventKey (see 'event subscription create --help').

SCOPE: list/get need event:subscription:read only. create/update/renew/
reactivate/delete need BOTH event:subscription:read AND
event:subscription:write — every one of those always reads remote state
first (for idempotency / conflict / impact analysis) before it may write,
so read is required even where the underlying platform call alone would
not strictly need it.

SAFETY: 'delete' is a high-risk write on a resource other identities/
processes may share and requires --yes after a human confirms — without
it, a typed confirmation-required error (exit code 10); create/update/
renew/reactivate never prompt for confirmation (a filter change, like a
renew or reactivate, is reversible). Every subcommand except list/get
supports --dry-run (parse + identity + scope preflight + a remote read +
impact analysis, zero writes). 'delete' removing the remote Subscription is
NOT a substitute for stopping a local 'event consume' process still bound
to it — stop that separately with 'lark-cli event stop'.

NEXT STEP: after 'create' succeeds, run 'lark-cli event consume <refined
EventKey>' to actually start receiving events — creating/updating a
Subscription here never starts, stops, or changes a local consumer.`,
		Example: `  lark-cli event subscription list --as bot --json
  lark-cli event subscription get sub_xxx --as bot --json
  lark-cli event subscription create im.message.example_v1/chat-id/oc_xxx --dry-run --as bot --json
  lark-cli event subscription update sub_xxx --clear-filter --dry-run --as bot --json
  lark-cli event subscription delete sub_xxx --dry-run --as bot --json`,
		SilenceUsage: true,
	}

	cmd.AddCommand(NewCmdList(f))
	cmd.AddCommand(NewCmdGet(f))
	cmd.AddCommand(NewCmdCreate(f))
	cmd.AddCommand(NewCmdUpdate(f))
	cmd.AddCommand(NewCmdRenew(f))
	cmd.AddCommand(NewCmdReactivate(f))
	cmd.AddCommand(NewCmdDelete(f))

	return cmd
}

// subscriptionReadScopes are the scopes required by both list and get.
var subscriptionReadScopes = []string{"event:subscription:read"}

// subscriptionEncryptKeyReadScopes is the scope required to fetch a
// subscription's encrypt_key via the platform/lark gateway's GetEncryptKey —
// event:encrypt_key:read is a distinct scope that neither subscriptionReadScopes
// nor subscriptionMutationScopes implies.
// create.go's createRequiredScopes wires this in for
// --include-resource-data=true: under the ManagementCreate policy the Planner
// probes GetEncryptKey to classify an existing include_resource_data=true match,
// so it needs this scope on top of subscriptionMutationScopes.
var subscriptionEncryptKeyReadScopes = []string{"event:encrypt_key:read"}

// addAsFlag registers the --as flag shared by every subscription
// subcommand that resolves an identity at all — list/get/update/renew/
// reactivate/delete carry no EventKey/KeyTemplate context, so --as resolves
// to a single effective identity with no per-template AuthTypes check —
// unlike `consume`/`create`, which do enforce one (cmd/event/consume.go's
// resolveIdentity).
func addAsFlag(cmd *cobra.Command) {
	cmd.Flags().String("as", "auto", "identity type: user | bot | auto")
	_ = cmd.RegisterFlagCompletionFunc("as", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{"user", "bot", "auto"}, cobra.ShellCompDirectiveNoFileComp
	})
}

// resolveEffectiveIdentity resolves --as to a single concrete identity
// (core.AsUser or core.AsBot) and rejects anything else — including a
// literal "auto" leaking through (f.ResolveAs always resolves it to a
// concrete identity first) and any other garbage flag value.
//
// It also enforces the administrator's configured strict-mode identity
// policy (f.CheckStrictMode) immediately after f.ResolveAs, mirroring every
// other --as call site in this CLI: cmd/api/api.go's apiRun,
// cmd/service/service.go's serviceMethodRun, and cmd/whoami/whoami.go's
// whoamiRun all call CheckStrictMode right after ResolveAs, before any
// further identity check. Factory.ResolveAs preserves an explicit --as
// through strict mode by design (internal/cmdutil/factory.go's own comment
// on that branch) specifically so the caller can reject it here — skipping
// this call would let an explicit --as of the disallowed type reach the API
// whenever a credential of that type still happened to be resolvable (e.g.
// a cached user token under a bot-only strict-mode account), silently
// bypassing the policy.
func resolveEffectiveIdentity(cmd *cobra.Command, f *cmdutil.Factory) (core.Identity, error) {
	flagAs := core.Identity(cmd.Flag("as").Value.String())
	as := f.ResolveAs(cmd.Context(), cmd, flagAs)
	if err := f.CheckStrictMode(cmd.Context(), as); err != nil {
		return "", err
	}
	if err := f.CheckIdentity(as, []string{"user", "bot"}); err != nil {
		return "", err
	}
	return as, nil
}

// resolveUATAndCheckScopes resolves the effective identity's access token
// and performs a local, best-effort scope pre-check against required.
// It returns the user access token to bind into the platform/lark subscription
// gateway — empty for bot identity, which the gateway ignores.
//
// Scope data being unavailable is not treated as "missing": the check is
// skipped and the real OAPI call is left to surface the authoritative
// permission error. This mirrors the established local-precheck idiom used
// by every other scope-gated command in this CLI (e.g.
// shortcuts/common/runner.go's checkScopePrereqs, cmd/event/consume.go's
// preflightScopes): this CLI's default credential provider never populates
// TokenResult.Scopes for a bot/tenant token
// (internal/credential/default_provider.go's doResolveTAT always returns
// Scopes==""), so in practice this fast local path only ever fires for user
// identity; bot permission failures are still caught — just by the real
// API call's error classification (internal/errclass) instead of here.
func resolveUATAndCheckScopes(ctx context.Context, f *cmdutil.Factory, appID string, as core.Identity, required []string) (string, error) {
	result, err := f.Credential.ResolveToken(ctx, credential.NewTokenSpec(as, appID))
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return "", err
		}
		// Best-effort: the gateway's identity binding itself fails closed
		// (typed AuthenticationError) for user identity when no token is
		// available; for bot identity the real API call surfaces any real
		// auth problem instead.
		return "", nil
	}
	if result == nil {
		return "", nil
	}
	if result.Scopes != "" {
		if missing := auth.MissingScopes(result.Scopes, required); len(missing) > 0 {
			return "", errs.NewPermissionError(errs.SubtypeMissingScope,
				"missing required scope(s) for event subscription (as %s): %s", as, strings.Join(missing, ", ")).
				WithIdentity(string(as)).
				WithMissingScopes(missing...).
				WithHint("%s", scopeRemediationHint(as, missing))
		}
	}
	return result.Token, nil
}

// scopeRemediationHint returns an identity-appropriate fix for missing
// scopes. Bot: tenant scopes come from the app manifest/console grant, not
// a per-user OAuth flow, so point at the console. User: the user's own
// token lacks the scope until re-authorized, so direct them to re-login
// with the scope requested (mirrors cmd/event/consume.go's
// scopeRemediationHint, reimplemented locally since that one is unexported
// in the sibling `event` package).
func scopeRemediationHint(as core.Identity, missing []string) string {
	joined := strings.Join(missing, " ")
	if as.IsBot() {
		return fmt.Sprintf("grant scope(s) %q to this app in the developer console's permission management, then retry `lark-cli event subscription ... --as bot`", joined)
	}
	return fmt.Sprintf("run `lark-cli auth login --scope %q` in the background. It blocks and outputs a verification URL — retrieve the URL and open it in a browser to complete login, then retry `lark-cli event subscription ... --as user`", joined)
}

// ---- shared remote Subscription JSON row (list items + get's single result) ----

// payloadOptionsView is the CLI-facing shape of
// SubscriptionDetail.PayloadOptions.
type payloadOptionsView struct {
	IncludeResourceData bool `json:"include_resource_data"`
}

// remoteState is the CLI-facing shape of a SubscriptionDetail's remote-side
// state, faithfully carrying every remote field available so the remote
// subscription data is emitted faithfully.
// SuspensionReason reuses the same "suspension_reason" name used for
// per-consumer state in the (not-yet-built) bus runtime, rather than
// inventing a second name for the same concept.
type remoteState struct {
	State            string `json:"state,omitempty"`
	ExpireTime       *int   `json:"expire_time,omitempty"`
	SuspensionReason string `json:"suspension_reason,omitempty"`
	CreateTime       *int   `json:"create_time,omitempty"`
	UpdateTime       *int   `json:"update_time,omitempty"`
}

// subscriptionRow is the CLI-facing JSON shape for one remote Subscription
// record — shared by `list`'s subscriptions[] entries and `get`'s single
// result.
//
// Local surfaces the running local `event consume` process(es), if any,
// currently bound to this remote subscription — discovered best-effort from the
// local bus (buslocal, the same mechanism `event status` uses) and matched by
// remote_subscription_id. It stays nil/omitted when no local consumer is known
// (no bus reachable, or none bound), so the field is purely additive.
type subscriptionRow struct {
	RemoteSubscriptionID string              `json:"remote_subscription_id"`
	EventKey             string              `json:"event_key"`
	EventType            string              `json:"event_type"`
	TargetResource       string              `json:"target_resource,omitempty"`
	// Identity is the subscription owner's authority as the exact `--as` token
	// ("bot" / "user"), so it can be copied straight into `--as <value>` on a
	// follow-up command. The owning user's open_id (lost from this composable
	// token) is preserved separately in UserOpenID.
	Identity   string `json:"identity,omitempty"`
	UserOpenID string `json:"user_open_id,omitempty"`

	PayloadOptions *payloadOptionsView `json:"payload_options,omitempty"`
	// Filter is the remote server-side event filter as canonical JSON, present
	// only when the remote subscription actually carries one (omitted for an
	// unfiltered subscription). Faithfully surfaced from the remote record; the
	// CLI does not interpret it here.
	Filter json.RawMessage    `json:"filter,omitempty"`
	Remote remoteState        `json:"remote"`
	Local  *localConsumerView `json:"local,omitempty"` // running local consumer(s); see doc comment above.
}

// mapRemoteSubscription converts one domain RemoteSubscription (projected from
// the SDK by the platform/lark gateway) into the CLI's stable JSON row shape.
//
// EventKey: a RemoteSubscription carries only event_type + target_resource, not
// an EventKey. This reconstructs the canonical, EXECUTABLE EventKey via
// catalog.ReverseResolve (the inverse of the forward key resolution `event
// consume`/`create` perform) so the event_key field is directly runnable by an
// AI/script — a legacy subscription reverses to its plain key, a refined one to
// its materialized "<base>/<segment>/<value>" key. When the pair cannot be
// reversed to exactly one registered key (ReverseResolve reports ok=false),
// event_key is the explicit EventKeyUnavailable marker rather than the raw
// event_type: presenting event_type where an executable event_key is expected
// would hand an AI a string `event schema`/`event consume` cannot accept. The
// raw event_type stays available in its own event_type field either way.
func mapRemoteSubscription(sub model.RemoteSubscription) subscriptionRow {
	row := subscriptionRow{
		RemoteSubscriptionID: sub.ID.String(),
		EventType:            sub.EventType,
		TargetResource:       sub.TargetResource,
		// Emit the `--as`-composable token ("bot"/"user"), not the compact
		// "app"/"user:<open_id>" matching spelling, and keep the open_id in its own
		// field so nothing is lost.
		Identity:   sub.Authority.AsToken(),
		UserOpenID: sub.Authority.OpenID,
	}
	// Only a real subscription (one carrying an event_type) gets an event_key: a
	// zero/absent record has no key at all and stays "" rather than being
	// labelled unavailable. A present-but-unreversible event_type yields the
	// explicit marker.
	if sub.EventType != "" {
		if key, ok := eventlib.ReverseResolve(sub.EventType, sub.TargetResource); ok {
			row.EventKey = key
		} else {
			row.EventKey = EventKeyUnavailable
		}
	}
	if sub.PayloadOptionsPresent {
		row.PayloadOptions = &payloadOptionsView{IncludeResourceData: boolVal(sub.IncludeResourceData)}
	}
	// Surface the remote filter as canonical JSON only when the subscription
	// actually carries one; an unfiltered subscription leaves this omitted.
	if !sub.Filter.IsEmpty() {
		if canonical, err := sub.Filter.Canonicalize(); err == nil {
			row.Filter = canonical
		}
	}
	row.Remote = remoteState{
		State:            sub.State,
		ExpireTime:       sub.ExpireTime,
		CreateTime:       sub.CreateTime,
		UpdateTime:       sub.UpdateTime,
		SuspensionReason: sub.SuspensionReason,
	}
	return row
}

func strVal(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func boolVal(b *bool) bool {
	return b != nil && *b
}

// ---- shared pieces for update/renew/reactivate/delete ----
//
// Unlike create (which keys off a refined EventKey with no remote identity
// yet, and so branches into create/reuse/conflict/suspended), these four
// commands all key off an already-existing remote_subscription_id: their
// --dry-run "parse/identity/scope preflight/remote read/impact analysis"
// shape is therefore identical across all of them, differing only
// in the operation name, the planned_change.action string, and the
// local_impact note text — so it is built and rendered once here rather
// than four times. list/get/create keep their own bespoke shapes unchanged.

// errEmptyRemoteSubscriptionID rejects an empty remote_subscription_id
// before any identity/scope/network work — the shared counterpart of
// get.go's runGet's own inlined identical check (kept as-is there per this
// change's "do not disturb list/get/create" constraint; this helper exists
// so update/renew/reactivate/delete do not each repeat it).
func errEmptyRemoteSubscriptionID() error {
	return errs.NewValidationError(errs.SubtypeInvalidArgument,
		"remote_subscription_id must not be empty").
		WithParam("remote_subscription_id").
		WithHint("pass the remote_subscription_id from `lark-cli event subscription list --json`")
}

// mutationPreflight is the `preflight` sub-object shared by update/renew/
// reactivate/delete's --dry-run JSON. Unlike create's
// createPreflight, these commands carry no EventKey/KeyTemplate context
// (they resolve --as to a single identity with no per-template
// AuthTypes check), so there is no MatchedTemplate field.
type mutationPreflight struct {
	Identity string `json:"identity"`
	ScopesOK bool   `json:"scopes_ok"`
}

// mutationDryRunResult is the shared --dry-run JSON shape for update/renew/
// reactivate/delete (operation/dry_run/required_scopes/
// preflight/remote_before/planned_change/local_impact/next_action). All four
// always have a non-nil RemoteBefore
// by the time this is built: unlike create's target (which may
// legitimately not exist yet), these commands operate on an id the caller
// already believes exists, and the shared getSubscription (get.go) helper
// that supplies RemoteBefore already returns a typed error rather than a
// nil detail when the remote side disagrees.
type mutationDryRunResult struct {
	Operation            string            `json:"operation"`
	DryRun               bool              `json:"dry_run"`
	RemoteSubscriptionID string            `json:"remote_subscription_id"`
	RequiredScopes       []string          `json:"required_scopes"`
	Preflight            mutationPreflight `json:"preflight"`
	RemoteBefore         *subscriptionRow  `json:"remote_before"`
	PlannedChange        plannedChange     `json:"planned_change"`
	LocalImpact          localImpact       `json:"local_impact"`
	NextAction           string            `json:"next_action"`
}

// buildMutationDryRunResult builds the shared --dry-run result. identity has
// already passed the scope preflight by the time this is called, so
// Preflight.ScopesOK is unconditionally true here — mirroring create.go's
// buildDryRunResult's own ScopesOK comment. plannedAction is the
// planned_change.action value (e.g. "renew", or a blocked-state variant
// such as "blocked_suspended" — the dry-run always reports the plan
// informationally rather than erroring on remote business state; only the
// preflight steps themselves are real dry-run failures, mirroring create's
// own conflict/suspended dry-run handling).
//
// localConsumerAffected is caller-supplied rather than hardcoded because the
// four operations differ: renew/reactivate/delete never change the delivered
// event stream, so they pass false; a real update changes the server-side
// filter and therefore the stream any local consumer receives, so it passes
// true and lets impactNote carry the "re-sync a running consumer" guidance
// (update cannot cheaply tell whether one is actually running, so it discloses
// the possible impact rather than asserting none).
func buildMutationDryRunResult(operation, remoteSubscriptionID string, identity core.Identity, before *subscriptionRow, plannedAction string, localConsumerAffected bool, impactNote, nextAction string) *mutationDryRunResult {
	return &mutationDryRunResult{
		Operation:            operation,
		DryRun:               true,
		RemoteSubscriptionID: remoteSubscriptionID,
		RequiredScopes:       subscriptionMutationScopes,
		Preflight: mutationPreflight{
			Identity: string(identity),
			ScopesOK: true,
		},
		RemoteBefore: before,
		PlannedChange: plannedChange{
			Action:               plannedAction,
			RemoteSubscriptionID: remoteSubscriptionID,
		},
		LocalImpact: localImpact{
			LocalConsumerAffected: localConsumerAffected,
			Note:                  impactNote,
		},
		NextAction: nextAction,
	}
}

func writeMutationDryRunText(out io.Writer, result *mutationDryRunResult) {
	fmt.Fprintf(out, "[dry-run] planned change: %s\n", result.PlannedChange.Action)
	fmt.Fprintf(out, "Remote Subscription ID: %s\n", result.RemoteSubscriptionID)
	if result.RemoteBefore != nil {
		fmt.Fprintf(out, "Current remote state:    %s\n", result.RemoteBefore.Remote.State)
	}
	// Surface the REAL affected local consumer(s) discovered from the bus, so the
	// text preview agrees with local_impact in the JSON. Shown only when this
	// operation actually affects a running local consumer.
	if len(result.LocalImpact.Consumers) > 0 {
		fmt.Fprintf(out, "Local consumers affected: %s\n", formatLocalConsumers(result.LocalImpact.Consumers))
	}
	fmt.Fprintf(out, "Next: %s\n", result.NextAction)
}

// mutationResult is the shared non-dry-run success JSON shape for
// update/renew/reactivate: each of those SDK calls (Patch/Renew/Reactivate)
// returns a fresh SubscriptionDetail to echo back. update also renders its
// no-op success (the requested filter already matched, so nothing was
// patched) through this same shape, echoing the subscription it read.
// delete has its own deleteResult in delete.go — DeleteSubscriptionResp
// carries no Data/SubscriptionDetail at all, so there is nothing fresh to
// map here.
type mutationResult struct {
	Operation            string          `json:"operation"`
	RemoteSubscriptionID string          `json:"remote_subscription_id"`
	Subscription         subscriptionRow `json:"subscription"`
	NextAction           string          `json:"next_action"`
}

func buildMutationResult(operation string, sub model.RemoteSubscription, nextAction string) *mutationResult {
	row := mapRemoteSubscription(sub)
	return &mutationResult{
		Operation:            operation,
		RemoteSubscriptionID: row.RemoteSubscriptionID,
		Subscription:         row,
		NextAction:           nextAction,
	}
}

func writeMutationResultText(out io.Writer, verb string, result *mutationResult) {
	fmt.Fprintf(out, "%s remote Subscription %s\n", verb, result.RemoteSubscriptionID)
	fmt.Fprintf(out, "State: %s\n", result.Subscription.Remote.State)
	fmt.Fprintf(out, "Next: %s\n", result.NextAction)
}
