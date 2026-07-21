// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

// Package subscription implements `lark-cli event subscription` — the
// management-plane commands for the candidate SDK's remote Subscription
// resource (design spec §3,
// docs/superpowers/specs/2026-07-21-oapi-event-subscribe-design.md).
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
	"strings"

	"github.com/spf13/cobra"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/auth"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/credential"
)

// NewCmdSubscription builds the `event subscription` command group (spec
// §3.1). This change (Task 8) wires the read-only list/get pair;
// create/update/renew/reactivate/delete land in later changes as siblings
// registered here, without touching list/get.
func NewCmdSubscription(f *cmdutil.Factory) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "subscription",
		Short: "Manage remote event Subscriptions",
		Long: `Manage the platform's persistent remote Subscription resources that back
refined (per-resource) event delivery — as opposed to 'event consume', which
starts a local process that consumes already-delivered events.

Use 'list' / 'get <remote_subscription_id>' to inspect what is currently
subscribed remotely.`,
		SilenceUsage: true,
	}

	cmd.AddCommand(NewCmdList(f))
	cmd.AddCommand(NewCmdGet(f))

	return cmd
}

// subscriptionReadScopes are the scopes required by both list and get
// (spec §3.5/§7).
var subscriptionReadScopes = []string{"event:subscription:read"}

// addAsFlag registers the --as flag shared by every subscription
// subcommand. Per spec §2.8's last paragraph, list/get/update/renew/
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
// and performs a local, best-effort scope pre-check against required (spec
// §3.5). It returns the user access token to bind into
// eventlib.NewSubscriptionClient — empty for bot identity, which the client
// ignores.
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
		// Best-effort: eventlib.NewSubscriptionClient itself fails closed
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
// state, faithfully carrying every remote field this task has available
// (the task brief: "emit the remote subscription data faithfully").
// SuspensionReason reuses the vocabulary the design spec already assigns to
// per-consumer state in the (not-yet-built) bus runtime (§5.5's
// "suspension_reason"), rather than inventing a second name for the same
// concept.
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
// Local is always omitted in this change: local-consumer association
// (which local `event consume` process, if any, is currently bound to this
// remote subscription) is a Phase-C concern — the bus runtime that would
// supply that data does not exist yet (design spec §4/§5). Rather than fake
// or guess at it, this field is left nil/omitted; a later change populates
// it once the bus runtime exists, additively, without changing this
// field's name or position.
type subscriptionRow struct {
	RemoteSubscriptionID string              `json:"remote_subscription_id"`
	EventKey             string              `json:"event_key"`
	EventType            string              `json:"event_type"`
	TargetResource       string              `json:"target_resource,omitempty"`
	Identity             string              `json:"identity,omitempty"`
	PayloadOptions       *payloadOptionsView `json:"payload_options,omitempty"`
	Remote               remoteState         `json:"remote"`
	Local                json.RawMessage     `json:"local,omitempty"` // Phase-C placeholder; see doc comment above.
}

// mapSubscriptionDetail converts one SDK SubscriptionDetail into the CLI's
// stable JSON row shape.
//
// EventKey: SubscriptionDetail has no field named event_key — the SDK only
// carries EventType + TargetResource (design spec §0.4). This task maps
// EventKey to EventType verbatim (a legacy/plain EventKey IS its OAPI
// event_type, per spec §1.1's own naming table) and additionally surfaces
// TargetResource as its own field, rather than fabricating a materialized
// refined-key string (e.g. "im.message.created_v1/chat-id/oc_xxx"): doing
// that faithfully requires a reverse KeyTemplate lookup (registry base +
// PathSegment reconstruction from the resource query string) that does not
// exist yet, and a wrong guess would emit a key-shaped string that `event
// schema`/`event consume` would not actually recognize. See
// task-8-report.md for the full rationale.
func mapSubscriptionDetail(d *larkeventv1.SubscriptionDetail) subscriptionRow {
	if d == nil {
		return subscriptionRow{}
	}
	row := subscriptionRow{
		RemoteSubscriptionID: strVal(d.SubscriptionId),
		EventType:            strVal(d.EventType),
		TargetResource:       strVal(d.TargetResource),
		Identity:             formatAuthority(d.Authority),
	}
	row.EventKey = row.EventType
	if d.PayloadOptions != nil {
		row.PayloadOptions = &payloadOptionsView{IncludeResourceData: boolVal(d.PayloadOptions.IncludeResourceData)}
	}
	row.Remote = remoteState{
		State:      strVal(d.State),
		ExpireTime: d.ExpireTime,
		CreateTime: d.CreateTime,
		UpdateTime: d.UpdateTime,
	}
	if d.Suspension != nil {
		row.Remote.SuspensionReason = strVal(d.Suspension.Code)
	}
	return row
}

// formatAuthority renders a SubscriptionDetail's Authority using this
// spec's own compact identity vocabulary (§1.1's naming table: "user:ou_xxx"
// / "app"). Authority.Type is an open string, not a closed enum (spec
// §0.4's closing line: "无任何 ... 常量，全部为字符串"), so an unrecognized
// value is passed through verbatim rather than dropped.
func formatAuthority(a *larkeventv1.Authority) string {
	if a == nil || a.Type == nil || *a.Type == "" {
		return ""
	}
	switch *a.Type {
	case "user":
		if id := strVal(a.OpenId); id != "" {
			return "user:" + id
		}
		return "user"
	case "app":
		return "app"
	default:
		return *a.Type
	}
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
