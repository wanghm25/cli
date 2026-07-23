// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/appmeta"
	"github.com/larksuite/cli/internal/auth"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/credential"
	eventlib "github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/consume"
	"github.com/larksuite/cli/internal/event/transport"
	"github.com/larksuite/cli/internal/output"
	"github.com/larksuite/cli/internal/validate"
)

type consumeCmdOpts struct {
	params    []string
	jqExpr    string
	quiet     bool
	outputDir string

	maxEvents int
	timeout   time.Duration
	dryRun    bool

	// includeResourceData: default false is a no-op
	// (unchanged behavior — a refined consume's remote Subscription is still
	// created/reused with resource data disabled). On a refined key, true is
	// SUPPORTED: it creates an ENCRYPTED remote Subscription and
	// flows through to RunRefined (RefinedOptions.IncludeResourceData). On an
	// ORDINARY (non-refined) key, true stays a typed invalid_argument (the flag
	// has no remote-Subscription concept to apply to there) — checked before
	// any side effect.
	includeResourceData bool
}

func NewCmdConsume(f *cmdutil.Factory) *cobra.Command {
	var o consumeCmdOpts

	cmd := &cobra.Command{
		Use:   "consume <EventKey>",
		Short: "Start consuming events for an EventKey",
		Long: `Start consuming real-time events for the given EventKey.

The consume command connects to the event bus daemon (starting it if needed),
subscribes to the specified EventKey, and streams processed events to stdout.

Output is one JSON object per line (NDJSON). Pipe through 'jq .' if you need
pretty-printed formatting.

Use 'event list' to see all available EventKeys.
Use 'event schema <EventKey>' for parameter details.

REFINED EVENTKEYS: a key with refined_subscription:true ('event schema <key>
--json') must be materialized with a resource selector before it can be
consumed, e.g. 'im.message.created_v1/chat-id/oc_xxx' (see
key_templates[].example in its schema). The bare base key is rejected with a
hint pointing at 'event schema'.

IDENTITY: --as user|bot|auto. For a refined key the resolved identity must
be one the MATCHED template accepts specifically, which can be narrower than
the key's own declared identities (e.g. an 'owner/me' template only accepts
'user' even though its base key allows user+bot) — a mismatch is a typed
error naming the allowed identities, never a silent switch.

SCOPE: a refined key's consume additionally requires BOTH
event:subscription:read and event:subscription:write on the resolved
identity's token (it reads remote state before it may create, reuse, or
reactivate a Subscription). A legacy key only needs its own declared scopes
(see 'event schema <key>').

OUTPUT: stdout is always business-event NDJSON only. For a refined key,
diagnostic lines (remote_subscription_id, whether this run created it, the
ready marker) go to stderr — never stdout.

NEXT STEP: manage the remote Subscription this run created/reused via
'lark-cli event subscription get|update|renew|reactivate|delete
<remote_subscription_id>'; run 'lark-cli event status' to see its current
remote_state.

SAFETY: for a refined key, consuming has write-level side effects (create,
reuse, or reactivate a remote Subscription) even though this command reads
as pure observe — run with --dry-run first to preview the plan with zero
writes. Unlike a legacy key, exiting a refined consumer never deletes the
remote Subscription (there is no cleanup hook): it is TTL-persistent and
reused by the next matching consume/create — delete it explicitly via
'event subscription delete' if you no longer want it to exist.

INCLUDE-RESOURCE-DATA: --include-resource-data (default false) mirrors the
same-named flag on 'event subscription create'. false (the default) is a
no-op: a refined consume's remote Subscription is still created/reused with
resource data disabled, exactly as before. Passing
--include-resource-data=true on a refined key creates an ENCRYPTED remote
Subscription: the CLI generates a per-subscription encrypt_key (OS CSPRNG,
in-memory only, never printed/logged/persisted — there is no --encrypt-key
flag) and submits it atomically with the Create; the bus then fetches the key
via GetEncryptKey to decrypt events. This additionally requires scope
event:encrypt_key:read on the resolved identity — the consumer confirms the
key is retrievable (prewarm) before it reports ready, and refuses to start
(typed decrypt_key_unavailable, no half-registered consumer) if it is not.
Passing --include-resource-data=true on an ORDINARY (non-refined) key is
always rejected as typed invalid_argument: the flag only ever controls a
refined key's remote Subscription, so it can never silently no-op there.`,
		Example: `  lark-cli event consume im.message.receive_v1 --as bot                        # legacy key: unlimited stream
  lark-cli event schema im.message.created_v1 --json                            # refined key: find its templates first
  lark-cli event consume im.message.created_v1/chat-id/oc_xxx --dry-run --as bot  # preview the refined plan, zero writes
  lark-cli event consume im.message.created_v1/chat-id/oc_xxx --as bot          # apply the plan, then stream`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runConsume(cmd, f, args[0], o)
		},
	}

	cmd.Flags().StringArrayVarP(&o.params, "param", "p", nil, "Key=value parameter (repeatable)")
	cmd.Flags().StringVar(&o.jqExpr, "jq", "", "JQ expression to filter output")
	cmd.Flags().BoolVar(&o.quiet, "quiet", false, "Suppress informational messages on stderr")
	cmd.Flags().StringVar(&o.outputDir, "output-dir", "", "Write each event as a file in this directory (relative paths only; absolute paths and ~ are rejected to prevent path traversal)")
	cmd.Flags().IntVar(&o.maxEvents, "max-events", 0, "Exit after N successful emits (0 = unlimited). Multi-worker EventKeys may emit up to workers-1 past N before all workers stop. Bounded runs ignore stdin EOF.")
	cmd.Flags().DurationVar(&o.timeout, "timeout", 0, "Exit after DURATION (e.g. 30s, 2m). 0 = no timeout. Timeout is a normal exit (code 0; stderr 'reason: timeout'). Bounded runs ignore stdin EOF.")
	cmd.Flags().String("as", "auto", "identity type: user | bot | auto (must match EventKey's declared AuthTypes)")
	_ = cmd.RegisterFlagCompletionFunc("as", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{"user", "bot", "auto"}, cobra.ShellCompDirectiveNoFileComp
	})
	cmd.Flags().BoolVar(&o.dryRun, "dry-run", false,
		"Preview the refined-subscription remote-write plan (probe + plan only) without applying it, starting the bus, or writing anything remote. No-op for legacy (non-refined) EventKeys, which never write remote state at all.")
	cmd.Flags().BoolVar(&o.includeResourceData, "include-resource-data", false,
		"Include resource data in a refined key's remote Subscription (mirrors 'event subscription create'). false (the default) is a no-op. true creates an ENCRYPTED subscription (CLI-generated per-subscription encrypt_key, never printed/logged; requires scope event:encrypt_key:read) and prewarms the key before the consumer reports ready. Always rejected as invalid_argument on an ordinary (non-refined) key.")
	cmdutil.SetRisk(cmd, "read")

	return cmd
}

func runConsume(cmd *cobra.Command, f *cmdutil.Factory, eventKey string, o consumeCmdOpts) error {
	// Pipe-close (e.g. `... | head -n 1`) must reach the EPIPE error path in the loop, not SIGPIPE-kill.
	ignoreBrokenPipe()

	cfg, err := f.Config()
	if err != nil {
		return err
	}

	paramMap, err := parseParams(o.params)
	if err != nil {
		return err
	}

	resolved, err := eventlib.ResolveEventKey(eventKey)
	if err != nil {
		// ResolveEventKey's own "base is not registered" wording differs from
		// the pre-existing "unknown EventKey: <key>" contract (locked by
		// tests/cli_e2e/event/event_consume_error_test.go); map that one case
		// back onto it. Every other rejection (bare base key, legacy+suffix,
		// bad template segment, ...) is returned unchanged.
		if !eventKeyBaseRegistered(eventKey) {
			return unknownEventKeyErr(eventKey)
		}
		return err
	}
	if resolved.IsRefined {
		// --include-resource-data=true is SUPPORTED on
		// a refined key — it creates an ENCRYPTED remote Subscription. The
		// flag flows through to
		// RunRefined (RefinedOptions.IncludeResourceData), which reconciles
		// against the encryption conflict matrix, generates + injects a fresh
		// CSPRNG encrypt_key on create, and prewarms the key before ready.
		// This is a materialized refined key: drive the refined
		// startup chain — ProbeBusEligibility
		// (read-only) -> PlanRemoteSubscription (List/Get) -> [--dry-run
		// exits here] -> ApplyRemoteSubscriptionPlan (the ONLY remote write)
		// -> StartOrConnectBus -> HelloV2. The legacy branch below
		// (keyDef/identity resolution through consume.Run) is UNTOUCHED and
		// never reached for a refined key.
		return runRefinedConsume(cmd, f, cfg, paramMap, resolved, o)
	}
	keyDef := resolved.Definition

	// --include-resource-data has no remote-Subscription
	// concept to apply to on an ordinary (legacy) EventKey — reject before
	// identity resolution or any other side effect, mirroring the refined
	// branch's own before-side-effect placement above.
	if o.includeResourceData {
		return errIncludeResourceDataNotApplicable(eventKey)
	}

	identity, err := resolveIdentity(cmd, f, keyDef)
	if err != nil {
		return err
	}

	if o.jqExpr != "" {
		if err := output.ValidateJqExpression(o.jqExpr); err != nil {
			return errs.NewValidationError(errs.SubtypeInvalidArgument, "%s", err).
				WithParam("--jq").
				WithCause(err).
				WithHint("see `lark-cli event consume --help` EXAMPLES for common patterns, or `lark-cli event schema %s` for valid field paths", eventKey)
		}
	}

	outputDir := o.outputDir
	if outputDir != "" {
		safePath, err := sanitizeOutputDir(outputDir)
		if err != nil {
			return err
		}
		outputDir = safePath
	}

	domain := core.ResolveEndpoints(cfg.Brand).Open

	// Surface auth errors before forking the bus daemon.
	if _, err := resolveTenantToken(cmd.Context(), f, cfg.AppID); err != nil {
		return err
	}

	apiClient, err := f.NewAPIClient()
	if err != nil {
		return err
	}
	runtime := &consumeRuntime{client: apiClient, accessIdentity: identity}
	// botRuntime pins AsBot: /app_versions rejects UAT (99991668) and /connection is app-level.
	botRuntime := &consumeRuntime{client: apiClient, accessIdentity: core.AsBot}

	// Weak-dependency fetch: failures leave appVer==nil and downgrade preflight to a no-op.
	preflightErrOut := f.IOStreams.ErrOut
	if o.quiet {
		preflightErrOut = io.Discard
	}
	appVer, appVerErr := appmeta.FetchCurrentPublished(cmd.Context(), botRuntime, cfg.AppID)
	switch {
	case appVerErr != nil:
		fmt.Fprintf(preflightErrOut, "[event] skipped console precheck: %s\n", describeAppMetaErr(appVerErr))
	case appVer == nil:
		fmt.Fprintln(preflightErrOut, "[event] skipped console precheck: app has no published version")
	}

	// Callback subscriptions live in application/get, not app_versions; fetch the
	// callback 底账 only for callback-type EventKeys. Weak dependency: on error,
	// leave subscribedCallbacks nil so the callback precheck skips.
	var subscribedCallbacks []string
	if keyDef.SubscriptionType == eventlib.SubTypeCallback {
		cbs, cbErr := appmeta.FetchSubscribedCallbacks(cmd.Context(), botRuntime, cfg.AppID)
		if cbErr != nil {
			fmt.Fprintf(preflightErrOut, "[event] skipped console precheck: %s\n", describeAppMetaErr(cbErr))
		} else {
			subscribedCallbacks = cbs
		}
	}

	pf := &preflightCtx{
		factory:             f,
		appID:               cfg.AppID,
		brand:               cfg.Brand,
		eventKey:            eventKey,
		identity:            identity,
		keyDef:              keyDef,
		appVer:              appVer,
		subscribedCallbacks: subscribedCallbacks,
	}
	if err := preflightEventTypes(pf); err != nil {
		return err
	}
	if err := preflightScopes(cmd.Context(), pf); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		select {
		case <-sigCh:
			if !o.quiet && f.IOStreams.IsTerminal {
				fmt.Fprintln(f.IOStreams.ErrOut, "\nShutting down...")
			}
			cancel()
		case <-ctx.Done():
		}
	}()

	errOut := f.IOStreams.ErrOut
	if o.quiet {
		errOut = io.Discard
	}

	// Non-TTY unbounded consumers use stdin EOF as shutdown for subprocess callers.
	// Bounded runs already have --max-events/--timeout as their lifecycle control.
	if shouldWatchStdinEOF(f.IOStreams.IsTerminal, o.maxEvents, o.timeout) {
		watchStdinEOF(os.Stdin, cancel, errOut)
	}

	if err := consume.Run(ctx, transport.New(), cfg.AppID, cfg.ProfileName, domain, consume.Options{
		EventKey:        eventKey,
		Params:          paramMap,
		JQExpr:          o.jqExpr,
		Quiet:           o.quiet,
		OutputDir:       outputDir,
		Runtime:         runtime,
		Out:             f.IOStreams.Out,
		ErrOut:          errOut,
		RemoteAPIClient: botRuntime,
		MaxEvents:       o.maxEvents,
		Timeout:         o.timeout,
		IsTTY:           f.IOStreams.IsTerminal,
	}); err != nil {
		return err
	}
	return nil
}

// errIncludeResourceDataNotApplicable returns the
// typed rejection: --include-resource-data only ever controls a
// refined-subscription EventKey's remote Subscription (see `event
// subscription create --include-resource-data`) — an ordinary (legacy)
// EventKey has no remote Subscription resource at all, so passing the flag
// against one is a caller mistake. Subtype is invalid_argument: unlike a
// refined key (where --include-resource-data=true is now SUPPORTED and creates
// an encrypted subscription), an ordinary key can never have a remote
// Subscription for the flag to apply to, so this rejection is permanent by
// design — not a temporary gate.
func errIncludeResourceDataNotApplicable(eventKey string) error {
	return errs.NewValidationError(errs.SubtypeInvalidArgument,
		"--include-resource-data does not apply to EventKey %q: it is not a refined-subscription key, and --include-resource-data only controls a refined key's remote Subscription", eventKey).
		WithParam("--include-resource-data").
		WithHint("drop --include-resource-data for this EventKey, or pass a materialized refined EventKey instead if you need resource-data control (see `lark-cli event schema %s --json` key_templates)", eventKey)
}

// eventKeyBaseRegistered reports whether eventKey itself, or the segment
// before its first "/", names a registered EventKey definition — mirroring
// the exact-match-then-split order ResolveEventKey applies internally.
// It exists solely to distinguish
// ResolveEventKey's "the base isn't registered at all" failure from every
// other typed rejection it can return (bare refined base key / legacy key
// with a path suffix / unknown template path segment / ...), so runConsume
// can keep surfacing the pre-existing "unknown EventKey" message+hint
// (tests/cli_e2e/event/event_consume_error_test.go) for the former while
// passing every other ResolveEventKey error through unchanged.
func eventKeyBaseRegistered(eventKey string) bool {
	if _, ok := eventlib.Lookup(eventKey); ok {
		return true
	}
	base, _, _ := strings.Cut(eventKey, "/")
	_, ok := eventlib.Lookup(base)
	return ok
}

// runRefinedConsume is cmd/event/consume.go's fork-seam target for a
// materialized refined EventKey. Unlike the legacy branch,
// none of the preflight below has run yet at this point in runConsume (the
// IsRefined check happens before keyDef/identity resolution) — so this
// resolves the minimal additional values consume.RunRefined needs (identity,
// an identity-bound SubscriptionClient, the local API client, domain, signal
// handling) and drives the refined startup chain: ProbeBusEligibility ->
// PlanRemoteSubscription -> [--dry-run exit] -> ApplyRemoteSubscriptionPlan
// (the ONLY remote write) -> StartOrConnectBus -> HelloV2.
// It deliberately mirrors, rather than shares code with, the legacy
// preflight further down runConsume — the legacy branch must stay
// byte-identical, and refined's own console-precheck/scopes preflight is
// intentionally not part of this seam.
func runRefinedConsume(cmd *cobra.Command, f *cmdutil.Factory, cfg *core.CliConfig, paramMap map[string]string, resolved eventlib.ResolvedEventKey, o consumeCmdOpts) error {
	identity, err := resolveIdentity(cmd, f, resolved.Definition)
	if err != nil {
		return err
	}
	// Write-safety: resolveIdentity calls
	// f.ResolveAs + f.CheckIdentity but never f.CheckStrictMode, unlike every
	// sibling --as write command — cmd/event/subscription/subscription.go's
	// resolveEffectiveIdentity (same signature/pattern mirrored here
	// verbatim), cmd/api/api.go's apiRun, cmd/service/service.go's
	// serviceMethodRun, and cmd/whoami/whoami.go's whoamiRun all call
	// CheckStrictMode right after ResolveAs, before any further identity
	// check. Factory.ResolveAs deliberately preserves an explicit --as
	// through strict mode specifically so the caller can reject it here
	// (internal/cmdutil/factory.go's own comment on that branch) — skipping
	// this call would let an explicit --as of the disallowed type reach
	// PlanRemoteSubscription/ApplyRemoteSubscriptionPlan (the ONLY remote
	// write this chain performs) whenever a credential of that type still
	// happened to be resolvable, silently bypassing the administrator's
	// configured identity policy. Deliberately NOT added inside
	// resolveIdentity itself: that function is shared with the legacy
	// (non-refined) consume.Run path, which must stay byte-identical — this
	// call is local to runRefinedConsume so only the refined write path
	// gains the gate. Must run BEFORE CheckTemplateAuthTypes and before any
	// client/subClient construction below.
	if err := f.CheckStrictMode(cmd.Context(), identity); err != nil {
		return err
	}
	// Write-safety: resolveIdentity only checked
	// the BASE key's AuthTypes; the matched KeyTemplate can be narrower
	// (e.g. the shipped im.message.created_v1/owner/me template is
	// user-only even though its base key allows user+bot — see
	// eventlib.CheckTemplateAuthTypes's own doc comment). This must run
	// BEFORE any client/subClient construction below and before
	// consume.RunRefined's Plan/Apply — a template-narrowed identity must
	// never reach the ONLY remote write this chain performs. Mirrors
	// cmd/event/subscription/create.go's own equivalent check (same shared
	// func), which guards the sibling write path.
	if err := eventlib.CheckTemplateAuthTypes(identity, resolved); err != nil {
		return err
	}

	outputDir := o.outputDir
	if outputDir != "" {
		safePath, err := sanitizeOutputDir(outputDir)
		if err != nil {
			return err
		}
		outputDir = safePath
	}

	domain := core.ResolveEndpoints(cfg.Brand).Open

	apiClient, err := f.NewAPIClient()
	if err != nil {
		return err
	}
	runtime := &consumeRuntime{client: apiClient, accessIdentity: identity}
	// botRuntime pins AsBot: /open-apis/event/v1/connection is app-level,
	// same rationale as the legacy botRuntime a few lines below in runConsume.
	botRuntime := &consumeRuntime{client: apiClient, accessIdentity: core.AsBot}

	sdk, err := f.LarkClient()
	if err != nil {
		return err
	}
	uat, err := resolveIdentityUAT(cmd.Context(), f, cfg.AppID, identity)
	if err != nil {
		return err
	}
	subClient, err := eventlib.NewSubscriptionClient(sdk, identity, uat)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		select {
		case <-sigCh:
			if !o.quiet && f.IOStreams.IsTerminal {
				fmt.Fprintln(f.IOStreams.ErrOut, "\nShutting down...")
			}
			cancel()
		case <-ctx.Done():
		}
	}()

	errOut := f.IOStreams.ErrOut
	if o.quiet {
		errOut = io.Discard
	}
	// Non-TTY unbounded consumers use stdin EOF as shutdown, same as legacy.
	if shouldWatchStdinEOF(f.IOStreams.IsTerminal, o.maxEvents, o.timeout) {
		watchStdinEOF(os.Stdin, cancel, errOut)
	}

	return consume.RunRefined(ctx, transport.New(), cfg.AppID, cfg.ProfileName, domain, resolved, consume.RefinedOptions{
		Params:              paramMap,
		JQExpr:              o.jqExpr,
		Quiet:               o.quiet,
		OutputDir:           outputDir,
		Runtime:             runtime,
		Out:                 f.IOStreams.Out,
		ErrOut:              errOut,
		RemoteAPIClient:     botRuntime,
		MaxEvents:           o.maxEvents,
		Timeout:             o.timeout,
		IsTTY:               f.IOStreams.IsTerminal,
		DryRun:              o.dryRun,
		Identity:            identity,
		SubClient:           subClient,
		IncludeResourceData: o.includeResourceData,
	})
}

// resolveIdentityUAT resolves the user access token SubscriptionClient needs
// when identity is core.AsUser; for core.AsBot it returns "" without any
// call at all — eventlib.NewSubscriptionClient ignores uat for a bot
// identity (the SDK mints/caches its own tenant access token), mirroring
// resolveTenantToken's error handling for the equivalent bot-token lookup.
func resolveIdentityUAT(ctx context.Context, f *cmdutil.Factory, appID string, identity core.Identity) (string, error) {
	if identity != core.AsUser {
		return "", nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	result, err := f.Credential.ResolveToken(ctx, credential.NewTokenSpec(core.AsUser, appID))
	if err != nil {
		if _, ok := errs.ProblemOf(err); ok {
			return "", err
		}
		return "", errs.NewAuthenticationError(errs.SubtypeTokenMissing,
			"resolve user access token: %s", err).WithCause(err)
	}
	if result == nil || result.Token == "" {
		return "", errs.NewAuthenticationError(errs.SubtypeTokenMissing,
			"no user access token available for app %s", appID).
			WithHint("run `lark-cli auth login` to authorize as user")
	}
	return result.Token, nil
}

// resolveIdentity resolves the session identity and enforces keyDef.AuthTypes as a whitelist.
func resolveIdentity(cmd *cobra.Command, f *cmdutil.Factory, keyDef *eventlib.KeyDefinition) (core.Identity, error) {
	flagAs := core.Identity(cmd.Flag("as").Value.String())
	identity := f.ResolveAs(cmd.Context(), cmd, flagAs)
	if len(keyDef.AuthTypes) > 0 {
		if err := f.CheckIdentity(identity, keyDef.AuthTypes); err != nil {
			return "", err
		}
	}
	return identity, nil
}

type preflightCtx struct {
	factory  *cmdutil.Factory
	appID    string
	brand    core.LarkBrand
	eventKey string
	identity core.Identity
	keyDef   *eventlib.KeyDefinition
	appVer   *appmeta.AppVersion
	// subscribedCallbacks is the application/get 底账 for callback-type EventKeys;
	// nil means "not fetched / unavailable" → callback precheck skips (weak dependency).
	subscribedCallbacks []string
}

// preflightScopes compares required scopes against session-available scopes (user: UAT stored; bot: appVer.TenantScopes).
func preflightScopes(ctx context.Context, pf *preflightCtx) error {
	if len(pf.keyDef.Scopes) == 0 || pf.identity == "" {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	var storedScopes string
	switch {
	case pf.identity.IsBot():
		if pf.appVer == nil {
			return nil
		}
		storedScopes = strings.Join(pf.appVer.TenantScopes, " ")
	case pf.identity == core.AsUser:
		result, err := pf.factory.Credential.ResolveToken(ctx, credential.NewTokenSpec(pf.identity, pf.appID))
		if err != nil || result == nil || result.Scopes == "" {
			return nil //nolint:nilerr // best-effort: bus handshake will surface real auth error
		}
		storedScopes = result.Scopes
	default:
		return nil
	}

	missing := auth.MissingScopes(storedScopes, pf.keyDef.Scopes)
	if len(missing) == 0 {
		return nil
	}
	return errs.NewPermissionError(errs.SubtypeMissingScope,
		"missing required scopes for EventKey %s (as %s): %s",
		pf.eventKey, pf.identity, strings.Join(missing, ", ")).
		WithIdentity(string(pf.identity)).
		WithMissingScopes(missing...).
		WithHint("%s", scopeRemediationHint(pf.brand, pf.appID, pf.identity, missing))
}

// scopeRemediationHint returns an identity-appropriate fix for missing scopes.
// Bot: the scan-to-enable link adds the scopes to the app manifest, after which
// the tenant token carries them. User: the scan link only updates the app
// manifest — the user's own token still lacks the scopes until it is
// re-authorized — so direct the user to re-login instead.
func scopeRemediationHint(brand core.LarkBrand, appID string, identity core.Identity, missing []string) string {
	if identity.IsBot() {
		return fmt.Sprintf("grant these scopes by scanning: %s",
			addonsHintURL(brand, appID, missingScopeAddons(identity, missing)))
	}
	return fmt.Sprintf(
		"run `lark-cli auth login --scope \"%s\"` in the background. It blocks and outputs a verification URL — retrieve the URL and open it in a browser to complete login.",
		strings.Join(missing, " "))
}

// preflightEventTypes verifies every RequiredConsoleEvents entry is subscribed
// in the app's console 底账 — published app_versions for event subscriptions,
// application/get subscribed_callbacks for callback subscriptions.
func preflightEventTypes(pf *preflightCtx) error {
	if len(pf.keyDef.RequiredConsoleEvents) == 0 {
		return nil
	}

	var subscribed []string
	noun := "event types"
	if pf.keyDef.SubscriptionType == eventlib.SubTypeCallback {
		if pf.subscribedCallbacks == nil {
			return nil
		}
		subscribed = pf.subscribedCallbacks
		noun = "callbacks"
	} else {
		if pf.appVer == nil {
			return nil
		}
		subscribed = pf.appVer.EventTypes
	}

	have := make(map[string]bool, len(subscribed))
	for _, t := range subscribed {
		have[t] = true
	}
	var missing []string
	for _, t := range pf.keyDef.RequiredConsoleEvents {
		if !have[t] {
			missing = append(missing, t)
		}
	}
	if len(missing) == 0 {
		return nil
	}

	url := addonsHintURL(pf.brand, pf.appID, missingSubscriptionAddons(pf.keyDef.SubscriptionType, pf.identity, missing))
	return errs.NewValidationError(errs.SubtypeFailedPrecondition,
		"EventKey %s requires %s not subscribed in console: %s",
		pf.keyDef.Key, noun, strings.Join(missing, ", ")).
		WithHint("subscribe these %s by scanning: %s", noun, url)
}

// sanitizeOutputDir rejects absolute/parent-escaping paths and ~ (SafeOutputPath treats it as a literal dir name).
func sanitizeOutputDir(dir string) (string, error) {
	if strings.HasPrefix(dir, "~") {
		return "", errs.NewValidationError(errs.SubtypeInvalidArgument,
			"%s; use a relative path like ./output instead", errOutputDirTilde).
			WithParam("--output-dir").
			WithCause(errOutputDirTilde)
	}
	safe, err := validate.SafeOutputPath(dir)
	if err != nil {
		return "", errs.NewValidationError(errs.SubtypeInvalidArgument,
			"%s %q: %s", errOutputDirUnsafe, dir, err).
			WithParam("--output-dir").
			WithCause(errOutputDirUnsafe)
	}
	return safe, nil
}

// resolveTenantToken fetches the app's tenant access token.
func resolveTenantToken(ctx context.Context, f *cmdutil.Factory, appID string) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	result, err := f.Credential.ResolveToken(ctx, credential.NewTokenSpec(core.AsBot, appID))
	if err != nil {
		if _, ok := errs.ProblemOf(err); ok {
			return "", err
		}
		return "", errs.NewAuthenticationError(errs.SubtypeTokenMissing,
			"resolve tenant access token: %s", err).WithCause(err)
	}
	if result == nil || result.Token == "" {
		return "", errs.NewAuthenticationError(errs.SubtypeTokenMissing,
			"no tenant access token available for app %s", appID).
			WithHint("Check that app_secret is configured (lark-cli config show) and try 'lark-cli auth login'.")
	}
	return result.Token, nil
}

// Sentinels for errors.Is checks; call sites wrap them as typed ValidationError causes.
var (
	errInvalidParamFormat = errors.New("invalid --param format")                    //nolint:forbidigo // sentinel, typed at call sites
	errOutputDirTilde     = errors.New("--output-dir does not support ~ expansion") //nolint:forbidigo // sentinel, typed at call sites
	errOutputDirUnsafe    = errors.New("unsafe --output-dir")                       //nolint:forbidigo // sentinel, typed at call sites
)

func parseParams(raw []string) (map[string]string, error) {
	m := make(map[string]string)
	for _, kv := range raw {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || k == "" {
			return nil, errs.NewValidationError(errs.SubtypeInvalidArgument,
				"%s %q: expected key=value", errInvalidParamFormat, kv).
				WithParam("--param").
				WithCause(errInvalidParamFormat)
		}
		m[k] = v
	}
	return m, nil
}

// watchStdinEOF drains r until EOF, writes a diagnostic, then cancels; only safe in non-TTY mode.
func watchStdinEOF(r io.Reader, cancel context.CancelFunc, errOut io.Writer) {
	go func() {
		_, _ = io.Copy(io.Discard, r)
		fmt.Fprintln(errOut, "[event] stdin closed — shutting down. "+
			"consume treats stdin EOF as exit signal (wired for AI subprocess callers). "+
			"To keep running: pass --max-events/--timeout for bounded run, "+
			"or keep stdin open (e.g. `< /dev/tty` interactive, `< <(tail -f /dev/null)` script), "+
			"or stop via SIGTERM instead of closing stdin.")
		cancel()
	}()
}

// shouldWatchStdinEOF gates the stdin-EOF shutdown watcher: non-TTY unbounded runs only (<= 0 mirrors downstream's >0-is-bounded semantics, so negative bounds stay unbounded).
func shouldWatchStdinEOF(isTerminal bool, maxEvents int, timeout time.Duration) bool {
	return !isTerminal && maxEvents <= 0 && timeout <= 0
}
