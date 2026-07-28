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
	"github.com/larksuite/cli/internal/event/app"
	"github.com/larksuite/cli/internal/event/consume"
	"github.com/larksuite/cli/internal/event/model"
	larkgw "github.com/larksuite/cli/internal/event/platform/lark"
	subown "github.com/larksuite/cli/internal/event/subscription"
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

	// filter is the inline JSON server-side event filter, mirroring
	// `event subscription create --filter`. SUPPORTED on a refined key
	// (validated against the event type's filter schema, then applied to the
	// remote Subscription this run creates/reuses). On an ORDINARY (non-refined)
	// key it is rejected as invalid_argument — there is no remote Subscription
	// for it to control, so it must never silently no-op.
	filter string
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
consumed, e.g. 'im.message.example_v1/chat-id/oc_xxx' (see
key_templates[].example in its schema). The bare base key is rejected with a
hint pointing at 'event schema'.

SCOPE: a refined key's consume additionally requires BOTH
event:subscription:read and event:subscription:write on the resolved
identity's token (it reads remote state before it may create, reuse, or
reactivate a Subscription). A legacy key only needs its own declared scopes
(see 'event schema <key>').

SAFETY: for a refined key, consuming has write-level side effects (create,
reuse, or reactivate a remote Subscription) even though this command reads
as pure observe — run with --dry-run first to preview the plan with zero
writes. Unlike a legacy key, exiting a refined consumer never deletes the
remote Subscription (there is no cleanup hook): it is TTL-persistent and
reused by the next matching consume/create — delete it explicitly via
'event subscription delete' if you no longer want it to exist.

INCLUDE-RESOURCE-DATA: --include-resource-data (default false) mirrors the
same-named flag on 'event subscription create'. Passing
--include-resource-data=true on a refined key includes resource data in
delivered events and REQUIRES --as user (resource data is user-only;
--as bot/auto→bot is rejected as invalid_argument). Resource data is delivered
encrypted by the platform and decrypted by the CLI before output; agents do not
need to manage keys or decryption. This additionally requires scope
event:encrypt_key:read on the resolved identity. Passing
--include-resource-data=true on an ORDINARY
(non-refined) key is always rejected as typed invalid_argument: the flag only
ever controls a refined key's remote Subscription, so it can never silently
no-op there.`,
		Example: `  lark-cli event consume im.message.receive_v1 --as bot                        # legacy key: unlimited stream
  lark-cli event schema im.message.example_v1 --json                            # refined key: find its templates first
  lark-cli event consume im.message.example_v1/chat-id/oc_xxx --dry-run --as bot  # preview the refined plan, zero writes
  lark-cli event consume im.message.example_v1/chat-id/oc_xxx --as bot          # apply the plan, then stream`,
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
		"Include resource data in delivered events for a refined key. Requires --as user and scope event:encrypt_key:read; the platform delivers resource data encrypted and the CLI decrypts it before output. Always rejected as invalid_argument on an ordinary (non-refined) key.")
	cmd.Flags().StringVar(&o.filter, "filter", "",
		"Inline `json` event filter to apply server-side for a refined key; validated against this event type's filter schema (see 'event schema <key> --json'). Omit for no filter. Rejected as invalid_argument on an ordinary (non-refined) key.")
	// Static default: "write", not "read". A single risk_level annotation
	// can't vary by the EventKey argument (unknown until RunE resolves it),
	// and a refined key's consume startup chain has REAL write side effects
	// (create/reuse/reactivate a remote Subscription) — so the static tag
	// must reflect this command's worst case, never silently under-report it as
	// pure "read". The precise per-invocation value ("read" for an ordinary key)
	// is app.InvocationDescriptor.Risk; RunE never mutates this annotation — a
	// command must not rewrite its own Cobra risk mid-run.
	cmdutil.SetRisk(cmd, "write")

	// The framework reads that annotation BEFORE RunE parses the argument — for
	// pre-startup command pruning (a max_risk policy) and the --help "Risk:" line
	// — so refine it from the command line when THIS invocation's EventKey is
	// already visible there: a resolvable legacy key downgrades the display/gate
	// to "read" via the same app.InvocationDescriptor.Risk policy, a refined key
	// (or an unresolvable/absent one) leaves the conservative "write" untouched.
	applyArgAwareConsumeRisk(cmd, os.Args[1:])

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
	// Classify the invocation explicitly — refined (remote-subscription setup),
	// legacy real run, or legacy dry-run (no setup) — instead of branching on
	// isRefined/dry-run inline, and let the ConsumeUseCase dispatch it. RunE does
	// NOT mutate the command's static risk annotation; the per-invocation risk
	// policy is app.InvocationDescriptor.Risk.
	descriptor := app.DescribeConsume(resolved, o.dryRun)
	setup := consumeSetup{cmd: cmd, f: f, cfg: cfg, paramMap: paramMap, resolved: resolved, o: o}
	return app.NewConsumeUseCase(setup).Run(cmd.Context(), descriptor)
}

// consumeSetup is cmd/event's app.ConsumeSetup implementation: it closes over
// the resolved invocation inputs so the ConsumeUseCase can dispatch each
// SetupKind to its Factory-bound execution without the command's RunE branching
// on isRefined/dry-run itself.
type consumeSetup struct {
	cmd      *cobra.Command
	f        *cmdutil.Factory
	cfg      *core.CliConfig
	paramMap map[string]string
	resolved eventlib.ResolvedEventKey
	o        consumeCmdOpts
}

// RunRemoteSubscription drives a materialized refined key: the refined startup
// chain — ProbeBusEligibility (read-only) -> PlanRemoteSubscription (List/Get)
// -> [--dry-run exits here] -> ApplyRemoteSubscriptionPlan (the ONLY remote
// write) -> StartOrConnectBus -> HelloV2. --include-resource-data=true is
// SUPPORTED here (it creates an ENCRYPTED remote Subscription; the flag flows
// through to consume.RunRefined, which generates + injects a fresh CSPRNG
// encrypt_key on create and lets the bus fetch it at Hello time). The legacy
// path is never reached for a refined key.
func (s consumeSetup) RunRemoteSubscription(context.Context) error {
	return runRefinedConsume(s.cmd, s.f, s.cfg, s.paramMap, s.resolved, s.o)
}

// RunNoSetup drives a legacy key under --dry-run: a legacy key has no
// remote-Subscription plan for a dry-run to preview, so — after the legacy-only
// flag rejections (which apply regardless of --dry-run) — this reports that and
// exits with ZERO side effects: no identity resolution, no bus, no consume.Run.
func (s consumeSetup) RunNoSetup(context.Context) error {
	if err := s.rejectLegacyOnlyFlags(); err != nil {
		return err
	}
	fmt.Fprintln(s.f.IOStreams.ErrOut, "[event] dry-run: legacy EventKey has no remote subscription plan — nothing to preview")
	return nil
}

// RunLegacy drives an ordinary key's real run: the legacy-only flag rejections,
// then the console/scope preflight and the bus consume start (runLegacyConsume).
func (s consumeSetup) RunLegacy(context.Context) error {
	if err := s.rejectLegacyOnlyFlags(); err != nil {
		return err
	}
	return runLegacyConsume(s.cmd, s.f, s.cfg, s.paramMap, s.resolved, s.o)
}

// rejectLegacyOnlyFlags rejects the two flags that only ever control a refined
// key's remote Subscription — --include-resource-data and --filter — when they
// are passed for an ordinary (legacy) key. Shared by the legacy real run and the
// legacy dry-run, since neither has a remote Subscription for them to apply to,
// and fired before any identity resolution or other side effect. resolved.
// MaterializedKey is the legacy input verbatim (ResolveEventKey never rewrites a
// legacy key), matching the raw argument this rejection previously named.
func (s consumeSetup) rejectLegacyOnlyFlags() error {
	if s.o.includeResourceData {
		return errIncludeResourceDataNotApplicable(s.resolved.MaterializedKey)
	}
	if s.o.filter != "" {
		return errFilterNotApplicable(s.resolved.MaterializedKey)
	}
	return nil
}

// runLegacyConsume is the ordinary (legacy) EventKey real-run path: identity
// resolution + console/scope preflight + the bus consume start. Its behavior and
// error ordering are unchanged from runConsume's old legacy branch; it is never
// reached for a refined key or a legacy dry-run.
func runLegacyConsume(cmd *cobra.Command, f *cmdutil.Factory, cfg *core.CliConfig, paramMap map[string]string, resolved eventlib.ResolvedEventKey, o consumeCmdOpts) error {
	eventKey := resolved.MaterializedKey
	keyDef := resolved.Definition

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

// errFilterNotApplicable returns the typed rejection for --filter on an
// ordinary (legacy) EventKey: --filter only ever controls a
// refined-subscription EventKey's remote Subscription (see `event subscription
// create --filter`) — an ordinary key has no remote Subscription for a
// server-side filter to apply to, so passing it is a caller mistake. Subtype is
// invalid_argument: this is permanent by design, not a temporary gate.
func errFilterNotApplicable(eventKey string) error {
	return errs.NewValidationError(errs.SubtypeInvalidArgument,
		"--filter does not apply to EventKey %q: it is not a refined-subscription key, and --filter only controls a refined key's remote Subscription", eventKey).
		WithParam("--filter").
		WithHint("drop --filter for this EventKey, or pass a materialized refined EventKey instead if you need server-side filtering (see `lark-cli event schema %s --json` key_templates)", eventKey)
}

// errIncludeResourceDataRequiresUser rejects --include-resource-data=true on a
// non-user identity for a refined key: resource data is a user-only platform
// capability, so a bot/app subscription cannot carry it. Typed invalid_argument
// (a caller mistake, not a transient state), fired before the encrypt_key scope
// preflight and before any remote write.
func errIncludeResourceDataRequiresUser(materializedKey string, identity core.Identity) error {
	return errs.NewValidationError(errs.SubtypeInvalidArgument,
		"--include-resource-data requires --as user for %s: resource data is only supported for a user subscription, not %s", materializedKey, identity).
		WithParam("--include-resource-data").
		WithHint("re-run with --as user, or drop --include-resource-data to consume %s without resource data", materializedKey)
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
// an identity-bound subscription Controller, the local API client, domain,
// signal handling) and drives the refined startup chain: ProbeBusEligibility ->
// PlanRemoteSubscription -> [--dry-run exit] -> ApplyRemoteSubscriptionPlan
// (the ONLY remote write) -> StartOrConnectBus -> HelloV2.
// It deliberately mirrors, rather than shares code with, the legacy
// preflight further down runConsume — the legacy branch must stay
// byte-identical, and refined's own console-precheck/scopes preflight is
// intentionally not part of this seam.
func runRefinedConsume(cmd *cobra.Command, f *cmdutil.Factory, cfg *core.CliConfig, paramMap map[string]string, resolved eventlib.ResolvedEventKey, o consumeCmdOpts) error {
	// Validate --filter against this event type's filter capability first (a
	// cheap, purely local check). Empty input is "no filter". A failure is
	// already a typed invalid_argument on --filter — returned unchanged, before
	// identity/scope resolution or any remote write. Mirrors
	// cmd/event/subscription/create.go's own filter validation.
	reqFilter, err := eventlib.ParseAndValidateFilter(o.filter, eventlib.FilterMetaFor(resolved.Definition.EventType))
	if err != nil {
		return err
	}

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
	// (e.g. the shipped im.message.example_v1/owner/me template is
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
	// include_resource_data is a user-only platform capability: a bot/app
	// subscription cannot carry it. Reject before the encrypt_key scope
	// preflight and before Plan/Apply (the ONLY remote write this chain
	// performs) so an encrypted bot subscription is never attempted.
	if o.includeResourceData && identity != core.AsUser {
		return errIncludeResourceDataRequiresUser(resolved.MaterializedKey, identity)
	}
	// Scope preflight: --include-resource-data=true needs
	// event:encrypt_key:read to ever consume the encrypted Subscription
	// this run may create or reuse. Must run BEFORE any client/subClient
	// construction below and before consume.RunRefined's own Plan/Apply —
	// see preflightEncryptKeyScope's own doc comment for why this can't
	// wait until the bus's Hello-time fetch. Runs even under --dry-run (mirrors
	// `event subscription create`'s own preflight placement, ahead of its
	// dry-run branch) so a preview surfaces the same rejection a real run
	// would hit, rather than only discovering it on a later real run.
	if o.includeResourceData {
		if err := preflightEncryptKeyScope(cmd.Context(), f, cfg.AppID, identity, resolved.MaterializedKey); err != nil {
			return err
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
	gateway, err := larkgw.NewSubscriptionGateway(sdk, identity, uat)
	if err != nil {
		return err
	}
	controller := subown.NewController(gateway)

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

	// The immutable, command-resolved owner: the EXPLICIT --profile/--as
	// selection (cfg already reflects --profile). This threads end-to-end into
	// the HelloV2 so the bus fixes the owner from THIS, never from a fresh
	// global-current read — the runtime "current" is used only by the bus gate.
	// A bot owner carries no user_open_id.
	owner := model.OwnerRef{
		AppID:    cfg.AppID,
		Identity: string(identity),
		Profile:  cfg.ProfileName,
	}
	if identity == core.AsUser {
		owner.UserOpenID = cfg.UserOpenId
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
		OwnerRef:            owner,
		Controller:          controller,
		IncludeResourceData: o.includeResourceData,
		Filter:              reqFilter,
	})
}

// refinedEncryptKeyReadScopes is the scope required to fetch a
// subscription's encrypt_key (the bus's Hello-time GetEncryptKey) —
// mirrors cmd/event/subscription/subscription.go's own (unexported, and in
// a different package) subscriptionEncryptKeyReadScopes; this package keeps
// its own copy rather than reaching across a package boundary for one scope
// string.
var refinedEncryptKeyReadScopes = []string{"event:encrypt_key:read"}

// preflightEncryptKeyScope is a local, best-effort check that the resolved
// identity's token already carries event:encrypt_key:read before
// runRefinedConsume does anything else with --include-resource-data=true.
// Without it, this scope would only be exercised by the bus's Hello-time
// GetEncryptKey fetch — which runs AFTER ApplyRemoteSubscriptionPlan (the
// remote Create) and the bus handshake. By then, a missing scope means the
// encrypted remote Subscription already exists (refined cleanup is nil: it is
// never auto-deleted) and simply cannot be consumed — an avoidable
// create-but-can't-consume outcome. This preflight catches it earlier,
// before that write ever happens, whenever the token's scopes are knowable
// locally.
//
// Mirrors cmd/event/subscription/subscription.go's own
// resolveUATAndCheckScopes: scope data being unavailable locally is not
// treated as "missing" — the check is silently skipped and the bus's own
// Hello-time GetEncryptKey fetch remains the authoritative check. This CLI's
// default
// credential provider never populates TokenResult.Scopes for a bot/tenant
// token (internal/credential/default_provider.go's doResolveTAT always
// returns Scopes==""), so in practice this only ever fires for a user
// identity — for a bot, the bus's Hello-time GetEncryptKey fetch remains the
// sole enforcement.
func preflightEncryptKeyScope(ctx context.Context, f *cmdutil.Factory, appID string, identity core.Identity, materializedKey string) error {
	result, err := f.Credential.ResolveToken(ctx, credential.NewTokenSpec(identity, appID))
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return nil //nolint:nilerr // best-effort: an unresolvable token here is not "missing scope" — the bus's Hello-time GetEncryptKey fetch remains authoritative
	}
	if result == nil || result.Scopes == "" {
		return nil
	}
	missing := auth.MissingScopes(result.Scopes, refinedEncryptKeyReadScopes)
	if len(missing) == 0 {
		return nil
	}
	return errs.NewPermissionError(errs.SubtypeMissingScope,
		"missing required scope for --include-resource-data=true (as %s): %s", identity, strings.Join(missing, ", ")).
		WithIdentity(string(identity)).
		WithMissingScopes(missing...).
		WithHint("grant/re-authorize scope `event:encrypt_key:read` for identity %s, then retry `lark-cli event consume %s --include-resource-data=true --as %s`; this scope is required for resource data delivery", identity, materializedKey, identity)
}

// resolveIdentityUAT resolves the user access token the subscription gateway
// needs when identity is core.AsUser; for core.AsBot it returns "" without any
// call at all — the gateway's identity binding ignores uat for a bot
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
