// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package consume

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/model"
	"github.com/larksuite/cli/internal/event/protocol"
	subown "github.com/larksuite/cli/internal/event/subscription"
	"github.com/larksuite/cli/internal/event/transport"
)

// refinedSubscriptionController is the subset of *subown.Controller the refined
// startup chain drives: Plan (the read-only Observe+classify PlanRemoteSubscription
// runs, safe under --dry-run) and Apply (the single remote write —
// Create/Reactivate/reuse). Declared as an interface so refined_test.go can drive
// the chain with a fake (or a real Controller over a fake platform/lark gateway)
// and no network.
//
// The consume front-end deliberately never fetches an encrypt_key itself: for an
// encrypted request the ConsumeBootstrap policy reuses an active match WITHOUT a
// front-end GetEncryptKey probe, and the bus fetches and confirms the key once at
// Hello time (internal/event/bus encryptKeyProvider.fetchAndSet) — the
// authoritative, fail-closed key gate.
type refinedSubscriptionController interface {
	Plan(ctx context.Context, policy subown.Policy, req subown.Request) (subown.SubscriptionPlan, error)
	Apply(ctx context.Context, plan subown.SubscriptionPlan, policy subown.Policy, req subown.Request) (subown.ApplyReceipt, error)
}

// RefinedOptions holds RunRefined's own parameters — deliberately separate
// from Options (which Run/consumeLoop/writeReadyMarker still use unchanged)
// so the legacy path is never perturbed by a refined-only field: refined
// fields must not be added to Options/Run.
type RefinedOptions struct {
	Params    map[string]string
	JQExpr    string
	Quiet     bool
	OutputDir string
	Runtime   event.APIClient
	Out       io.Writer // nil falls back to os.Stdout
	ErrOut    io.Writer // nil falls back to os.Stderr

	// RemoteAPIClient is used for ProbeBusEligibility's remote-connection
	// check AND (unchanged) inside EnsureBus/StartOrConnectBus's own copy of
	// that same check. nil disables both (probe skips it; EnsureBus already
	// treats nil the same way).
	RemoteAPIClient APIClient

	MaxEvents int
	Timeout   time.Duration
	IsTTY     bool

	// DryRun stops the chain after PlanRemoteSubscription: no Apply, no bus,
	// no remote write (security-relevant invariant).
	DryRun bool

	// IncludeResourceData: true creates an ENCRYPTED
	// remote Subscription — Plan reconciles against the encryption conflict
	// matrix deferring key confirmation to the bus (the front-end never fetches
	// the key), Apply generates a fresh CSPRNG encrypt_key
	// and submits it atomically with the Create, and the HelloV2 carries the
	// flag so the bus fetches that key once at registration (rejecting the
	// Hello with decrypt_key_unavailable if it cannot, so the consumer never
	// readies). false (the default) is the plaintext path, byte-for-byte
	// unchanged.
	IncludeResourceData bool

	// Filter is the already-parsed, already-validated server-side event filter
	// the caller requested (empty/nil when no --filter was given). It is used
	// here only to reconcile against an existing subscription's filter and to
	// build the Create body on an apply — local delivery is not yet filtered by
	// it.
	Filter *event.Filter

	// Identity is the already-resolved --as identity (
	// resolved by the caller, e.g. cmd/event/consume.go's resolveIdentity;
	// RunRefined never guesses or re-resolves it).
	Identity core.Identity

	// OwnerRef is the immutable, COMMAND-resolved owner this consumer registers
	// as — the explicit --profile/--as selection (AppID/Profile/UserOpenID),
	// captured once by the caller. The HelloV2 establishes the bus-side owner
	// from THIS, never from a fresh global-current read, so an explicit profile
	// threads end-to-end: the owner is used for plan/bind/registration, while
	// the runtime "current" identity is used ONLY by the bus's owner==current
	// gate. A bot owner carries an empty UserOpenID. Profile is diagnostic.
	OwnerRef model.OwnerRef

	// Controller is the already-constructed subscription Observe->Plan->Apply
	// owner (built by the caller over the identity-bound platform/lark gateway).
	// PlanRemoteSubscription calls Plan; ApplyRemoteSubscriptionPlan calls Apply.
	Controller refinedSubscriptionController
}

// refinedDeps are RunRefined's injectable seams for the 5 ordered stages
// so refined_test.go can assert strict
// order — probe < plan < apply < startBus < hello — and the dry-run
// short-circuit with a call-recorder, never touching a real network or bus.
// Production wiring is prodRefinedDeps below; tests substitute their own
// closures directly.
type refinedDeps struct {
	probe    func(ctx context.Context) error
	plan     func(ctx context.Context) (subown.SubscriptionPlan, error)
	apply    func(ctx context.Context, plan subown.SubscriptionPlan) (remoteSubscriptionID string, createdByThisAttempt bool, err error)
	startBus func(ctx context.Context) (net.Conn, error)
	hello    func(ctx context.Context, conn net.Conn, remoteSubscriptionID string) (*protocol.HelloAck, *bufio.Reader, error)
}

// RunRefined drives the refined-subscription `event consume` startup chain:
// ProbeBusEligibility (read-only) ->
// PlanRemoteSubscription (List/Get) -> [--dry-run exits here] ->
// ApplyRemoteSubscriptionPlan (the ONLY remote write: Create/Reactivate) ->
// StartOrConnectBus -> HelloV2 -> consumeLoop. This is the refined
// counterpart to Run; it reuses Run's own package-private helpers
// (EnsureBus, doHelloV2, consumeLoop, writeReadyMarker) directly rather than
// duplicating them. resolved is the caller's already-resolved refined
// EventKey (eventlib.ResolveEventKey, IsRefined true); opts.Identity is the
// already-resolved --as identity — RunRefined performs no identity
// resolution, config loading (beyond HelloV2's own current-profile lookup),
// or scope checking of its own.
func RunRefined(ctx context.Context, tr transport.IPC, appID, profileName, domain string, resolved event.ResolvedEventKey, opts RefinedOptions) error {
	return runRefinedChain(ctx, resolved, opts, prodRefinedDeps(tr, appID, profileName, domain, resolved, opts))
}

// prodRefinedDeps wires refinedDeps to the real implementations.
func prodRefinedDeps(tr transport.IPC, appID, profileName, domain string, resolved event.ResolvedEventKey, opts RefinedOptions) refinedDeps {
	eventType := resolved.Definition.EventType
	targetResource := resolved.TargetResource

	return refinedDeps{
		probe: func(ctx context.Context) error {
			return ProbeBusEligibility(ctx, tr, appID, opts.RemoteAPIClient, opts.ErrOut)
		},
		plan: func(ctx context.Context) (subown.SubscriptionPlan, error) {
			// The ConsumeBootstrap policy encodes the consume-specific decisions:
			// an encrypted request (IncludeResourceData=true) defers key
			// confirmation to the bus Hello — the authoritative, fail-closed key
			// gate — so an active include_resource_data=true match plans a reuse
			// WITHOUT the consume front-end ever calling GetEncryptKey (unlike
			// `event subscription create`'s ManagementCreate, which probes at Plan
			// time because it has no later key gate); and a compatible suspended
			// match plans a Reactivate rather than a Block. The requested filter is
			// compared against an existing subscription's as a reuse dimension (an
			// empty filter compares as "no filter", so an unfiltered consume against
			// an unfiltered match still reuses).
			return opts.Controller.Plan(ctx, subown.ConsumeBootstrap, refinedRequest(opts, eventType, targetResource))
		},
		apply: func(ctx context.Context, plan subown.SubscriptionPlan) (string, bool, error) {
			receipt, err := opts.Controller.Apply(ctx, plan, subown.ConsumeBootstrap, refinedRequest(opts, eventType, targetResource))
			if err != nil {
				return "", false, err
			}
			return receipt.RemoteID.String(), receipt.CreatedByAttempt, nil
		},
		startBus: func(ctx context.Context) (net.Conn, error) {
			return EnsureBus(ctx, tr, appID, profileName, domain, opts.RemoteAPIClient, opts.ErrOut)
		},
		hello: func(_ context.Context, conn net.Conn, remoteSubscriptionID string) (*protocol.HelloAck, *bufio.Reader, error) {
			localSubscriptionID := ComputeSubscriptionID(resolved.Definition, opts.Params)

			// Establish the owner the HelloV2 registers from the EXPLICIT,
			// command-resolved OwnerRef (the --profile/--as selection captured
			// once by the caller) — NOT a fresh global-current read. This is
			// what threads an explicit profile end-to-end: the bus fixes this
			// owner at registration and uses runtime "current" ONLY for its
			// owner==current gate. Previously this re-derived the GLOBAL current
			// profile here, which silently mixed an explicit --profile with
			// whoever happened to be the global current.
			profile, userOpenID := opts.OwnerRef.Profile, opts.OwnerRef.UserOpenID

			// scopeUserOpenID mirrors buildHelloV2's own bot-drops-UserOpenID
			// rule: a bot consumer's ConsumerScopeID
			// must be user-independent, matching what actually goes out on
			// the wire — otherwise it would vary with whoever happens to be
			// logged in on this host, fragmenting app-level fan-out for a
			// scope id that is supposed to be stable per (base key,
			// materialized key, authority type, app). User identity
			// behavior is unchanged: its scope id still includes the real
			// user_open_id.
			scopeUserOpenID := userOpenID
			if opts.Identity.IsBot() {
				scopeUserOpenID = ""
			}
			consumerScopeID := computeConsumerScopeID(resolved.Definition.Key, resolved.MaterializedKey,
				authorityTypeFor(opts.Identity), appID, scopeUserOpenID)

			hello := buildHelloV2(resolved, opts.Identity, localSubscriptionID, profile, userOpenID, remoteSubscriptionID, consumerScopeID, targetResource)
			// Tell the bus this is an ENCRYPTED subscription so it fetches the
			// encrypt_key once (under the owner==current gate) before acking.
			// The plaintext path leaves this false — no key fetch on the bus.
			hello.IncludeResourceData = opts.IncludeResourceData
			// Carry this consumer's requested server-side filter as declared
			// intent so the bus can compare a later remote filter change against
			// it. nil (no --filter) is omitted on the wire and read as "no filter".
			hello.Filter = opts.Filter
			return doHelloV2(conn, hello)
		},
	}
}

// runRefinedChain is the order-critical orchestrator: it is what
// refined_test.go drives directly with fake refinedDeps (no real network),
// so this function itself must never reach into tr/appID/etc. — every
// stage's real work lives behind the deps closures built in prodRefinedDeps.
func runRefinedChain(ctx context.Context, resolved event.ResolvedEventKey, opts RefinedOptions, deps refinedDeps) error {
	errOut := opts.ErrOut
	if errOut == nil {
		errOut = os.Stderr //nolint:forbidigo // library-caller fallback
	}

	// --timeout bounds the WHOLE session (Probe through consumeLoop), same
	// as Run's own placement before EnsureBus/doHello — "exit after
	// DURATION" is measured from command start, not just listening-phase.
	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	// Validate jq and normalize params before any side effects (mirrors
	// Run's own ordering rationale: reject bad input before Probe's remote
	// read, let alone Plan/Apply).
	if opts.JQExpr != "" {
		if _, err := CompileJQ(opts.JQExpr); err != nil {
			return err
		}
	}
	if resolved.Definition.NormalizeParams != nil {
		if err := resolved.Definition.NormalizeParams(ctx, opts.Runtime, opts.Params); err != nil {
			if _, ok := errs.ProblemOf(err); ok {
				return err
			}
			return errs.NewInternalError(errs.SubtypeUnknown,
				"normalize params for %s: %s", resolved.MaterializedKey, err).WithCause(err)
		}
	}
	if err := validateParams(resolved.Definition, opts.Params); err != nil {
		return err
	}

	// ---- 1. ProbeBusEligibility (read-only) ----
	if err := deps.probe(ctx); err != nil {
		return err
	}

	// ---- 2. PlanRemoteSubscription (List/Get only) ----
	plan, err := deps.plan(ctx)
	if err != nil {
		return err
	}

	// ---- 3. --dry-run exits HERE: no Apply, no bus, no remote write ----
	if opts.DryRun {
		writeRefinedDryRunPreview(errOut, resolved, plan)
		return nil
	}

	// Block and Indeterminate (an inconclusive remote scan that hit the page cap)
	// are the two plan outcomes Apply cannot safely resolve — create, reuse, and
	// reactivate all proceed below. A real run fails closed instead of guessing. A
	// compatible suspended match is never a Block under ConsumeBootstrap (it plans
	// a Reactivate), so a Block here is either a configuration conflict or a match
	// in an unrecognized remote state (fail-closed).
	switch plan.Action {
	case subown.ActionBlock:
		if subown.ConflictOnState(plan.ConflictFields) {
			return refinedUnknownStateError(resolved, opts.Identity, plan)
		}
		return refinedConflictError(resolved, opts.Identity, plan)
	case subown.ActionIndeterminate:
		return refinedIndeterminateError(resolved, opts.Identity)
	}

	// ---- 4. ApplyRemoteSubscriptionPlan (the ONLY remote write) ----
	remoteSubscriptionID, createdByThisAttempt, err := deps.apply(ctx, plan)
	if err != nil {
		return err
	}

	// ---- 5. StartOrConnectBus ----
	conn, err := deps.startBus(ctx)
	if err != nil {
		// Apply already succeeded (a remote write may have just happened) —
		// refined cleanup is nil: never auto-delete the remote subscription
		// just because the LOCAL bus failed to start. Preserve
		// remote_subscription_id/created_by_this_attempt so the caller can
		// retry (Plan will reuse it, not create a duplicate).
		return errApplyOkStartBusFailed(resolved, opts.Identity, remoteSubscriptionID, createdByThisAttempt, err)
	}
	defer conn.Close()

	// ---- 6. HelloV2 ----
	ack, br, err := deps.hello(ctx, conn, remoteSubscriptionID)
	if err != nil {
		// Same rationale as the startBus-fail branch above: Apply already
		// succeeded, so this must carry the same recovery Hint, not a
		// generic unactionable message.
		return errApplyOkHelloFailed(resolved, opts.Identity, remoteSubscriptionID, createdByThisAttempt, err)
	}
	// The bus rejects a refined Hello with a fixed reason token for several
	// distinct fail-closed conditions (it never reveals WHY beyond the token —
	// no oracle). Each maps to its OWN typed error with actionable, fully-local
	// guidance, instead of the generic rejectionError below whose hint
	// ("EventKey allows only one consumer") would misrepresent an identity, key,
	// or registration failure as a single-consumer conflict. The consumer never
	// remains registered for any of these (each reject closes the connection,
	// unwinding any registration), so there is nothing local to roll back.
	if ack != nil && ack.Rejected {
		switch ack.RejectReason {
		case protocol.RejectReasonDecryptKeyUnavailable:
			return refinedDecryptKeyUnavailableError(resolved, opts.Identity, remoteSubscriptionID, createdByThisAttempt)
		case protocol.RejectReasonBindFailed:
			return refinedBindFailedError(resolved, opts.Identity, remoteSubscriptionID, createdByThisAttempt)
		case protocol.RejectReasonIncompleteRefinedHello:
			return refinedIncompleteHelloError(resolved, opts.Identity, remoteSubscriptionID, createdByThisAttempt)
		}
	}
	if rejErr := rejectionError(ack, resolved.MaterializedKey); rejErr != nil {
		return applyOkRejectedError(resolved, opts.Identity, remoteSubscriptionID, createdByThisAttempt, rejErr)
	}

	consumeOpts := Options{
		EventKey:  resolved.MaterializedKey,
		Params:    opts.Params,
		JQExpr:    opts.JQExpr,
		Quiet:     opts.Quiet,
		OutputDir: opts.OutputDir,
		Runtime:   opts.Runtime,
		Out:       opts.Out,
		ErrOut:    opts.ErrOut,
		MaxEvents: opts.MaxEvents,
		Timeout:   opts.Timeout,
		IsTTY:     opts.IsTTY,
	}

	if !opts.Quiet {
		fmt.Fprintln(errOut, listeningText(consumeOpts))
		if !opts.IsTTY {
			fmt.Fprintln(errOut, stopHintText(consumeOpts))
		}
	}
	// remote_subscription_id/owner go to their own diagnostic line, never
	// the ready line itself (the stdout
	// contract — the ready marker format is frozen).
	if remoteSubscriptionID != "" {
		fmt.Fprintf(errOut, "[event] refined consumer bound to remote_subscription_id=%s (created_by_this_attempt=%t)\n", remoteSubscriptionID, createdByThisAttempt)
	}

	// Ready marker uses the FILLED template instance as event_key —
	// consumeOpts.EventKey is already resolved.MaterializedKey.
	writeReadyMarker(errOut, consumeOpts)

	startTime := time.Now()
	var lastForKey bool
	var emitted atomic.Int64
	// No PreConsume/cleanup for refined consumers (Apply already handled the
	// remote-provisioning role PreConsume plays for legacy keys), so unlike
	// Run there is no cleanup to run panic-safely here — refined cleanup is
	// nil by design.
	defer func() {
		if !opts.Quiet {
			reason := exitReason(ctx, emitted.Load(), consumeOpts)
			fmt.Fprintf(errOut, "[event] exited — received %d event(s) in %s (reason: %s)\n",
				emitted.Load(), truncateDuration(time.Since(startTime)), reason)
		}
	}()

	localSubscriptionID := ComputeSubscriptionID(resolved.Definition, opts.Params)
	return consumeLoop(ctx, conn, br, resolved.Definition, consumeOpts, localSubscriptionID, &lastForKey, &emitted)
}

// refinedRequest builds the subscription.Request for this refined consume from
// its resolved coordinates and options. The Controller (ConsumeBootstrap policy)
// turns it into the single remote write: Create for a fresh plan (generating a
// fresh encrypt_key for an encrypted request and injecting it atomically —
// fail-closed, never a plaintext fallback), Reactivate for a compatible suspended
// match (auto-resumed, so `event consume` gets the caller listening), or a no-op
// reuse of an existing compatible match. The generated key is never logged,
// persisted, returned, or sent to the bus over IPC (the bus fetches its own copy
// via GetEncryptKey at Hello time).
func refinedRequest(opts RefinedOptions, eventType, targetResource string) subown.Request {
	return subown.Request{
		EventType:           eventType,
		TargetResource:      targetResource,
		Identity:            opts.Identity,
		IncludeResourceData: opts.IncludeResourceData,
		Filter:              opts.Filter,
	}
}

// refinedDecryptKeyUnavailableError turns the bus's decrypt_key_unavailable
// Hello rejection into the structured error the consumer exits with instead of
// going ready. The bus fetches the encrypt_key once at Hello time (under the
// owner==current gate); when that fails it rejects the Hello before the
// consumer registers, so there is NO half-registered consumer to roll back.
// This becomes a failed_precondition guiding the operator to fix scope/identity
// (or delete+recreate). The bus never returns WHY it failed (no oracle) — only
// the fixed reason token — so this message is fully local, carrying no key
// material or raw fetch error.
func refinedDecryptKeyUnavailableError(resolved event.ResolvedEventKey, identity core.Identity, remoteSubscriptionID string, createdByThisAttempt bool) error {
	return errs.NewValidationError(errs.SubtypeFailedPrecondition,
		"cannot start consuming %s: the event bus could not obtain its subscription encrypt_key (decrypt_key_unavailable), so it could not decrypt this subscription's events",
		resolved.MaterializedKey).
		WithParam("--include-resource-data").
		WithHint("the consumer was NOT started. Ensure identity %s holds scope `event:encrypt_key:read` and is the owner of the subscription (switch --as/--profile if this identity is not the owner); or delete and recreate the subscription after human confirmation. %s",
			identity, applyOkRecoveryHint(resolved, identity, remoteSubscriptionID, createdByThisAttempt))
}

// refinedBindFailedError turns the bus's identity_bind_failed Hello rejection
// into a typed error. The bus binds a WS-ready user consumer to its
// subscription at Hello time under the owner==current gate; when that bind
// fails — the active profile/user is no longer the subscription owner, its user
// access token could not be minted, or the BindUser call failed — the bus
// rejects and closes the connection (unwinding any registration) so the
// consumer never readies. The bus returns only the fixed reason token (no
// oracle for which check failed), so this guidance is fully local.
func refinedBindFailedError(resolved event.ResolvedEventKey, identity core.Identity, remoteSubscriptionID string, createdByThisAttempt bool) error {
	return errs.NewValidationError(errs.SubtypeFailedPrecondition,
		"cannot start consuming %s: the event bus could not bind this subscription to identity %s (identity_bind_failed)",
		resolved.MaterializedKey, identity).
		WithParam("--as").
		WithHint("the consumer was NOT started. Confirm the active profile/user is the owner of the subscription (switch --as/--profile if it is not) and that its user access token is still valid (re-run `lark-cli auth login` to re-authorize if needed). %s",
			applyOkRecoveryHint(resolved, identity, remoteSubscriptionID, createdByThisAttempt))
}

// refinedIncompleteHelloError turns the bus's incomplete_refined_hello Hello
// rejection into a typed error. The bus rejects — before registering the
// consumer — a refined Hello that carries a remote_subscription_id but no
// target_resource. A refined consumer always resolves a target_resource from
// its key, so this signals an internal inconsistency rather than an
// operator-fixable condition.
func refinedIncompleteHelloError(resolved event.ResolvedEventKey, identity core.Identity, remoteSubscriptionID string, createdByThisAttempt bool) error {
	return errs.NewInternalError(errs.SubtypeUnknown,
		"cannot start consuming %s: the event bus rejected the registration as incomplete (incomplete_refined_hello) — the refined Hello carried no target_resource",
		resolved.MaterializedKey).
		WithHint("the consumer was NOT started. This is an internal inconsistency (a refined consumer should always resolve a target_resource); please report it if it persists. %s",
			applyOkRecoveryHint(resolved, identity, remoteSubscriptionID, createdByThisAttempt))
}

// refinedConflictError mirrors cmd/event/subscription/create.go's own
// conflictError for an ActionBlock plan — the configuration-conflict outcome
// runRefinedChain always turns into a typed error on a real run (create,
// reuse, and reactivate all proceed to Apply instead).
func refinedConflictError(resolved event.ResolvedEventKey, identity core.Identity, plan subown.SubscriptionPlan) error {
	id := ""
	if plan.Before != nil {
		id = plan.Before.ID.String()
	}
	hint := fmt.Sprintf("run `lark-cli event subscription get %s --as %s --json` to inspect remote_subscription_id=%s, then either accept its existing configuration or delete it before consuming", id, identity, id)
	if subown.ConflictOnFilter(plan.ConflictFields) {
		// A filter difference is resolvable in place — change it with `update`
		// rather than deleting a possibly-shared subscription. Guide inspect ->
		// preview -> apply -> re-run consume; the filter values are never named.
		hint = fmt.Sprintf("run `lark-cli event subscription get %s --as %s --json` to inspect remote_subscription_id=%s, preview the change with `lark-cli event subscription update %s --filter <json> --dry-run --as %s`, apply it with `lark-cli event subscription update %s --filter <json> --as %s`, then re-run consume — a filter change is reversible, so there is no need to delete a possibly-shared subscription", id, identity, id, id, identity, id, identity)
	}
	return errs.NewValidationError(errs.SubtypeFailedPrecondition,
		"an active remote subscription already exists for %s with a conflicting configuration (remote_subscription_id=%s)",
		resolved.MaterializedKey, id).
		WithParam("event_key").
		WithParams(plan.ConflictFields...).
		WithHint("%s", hint)
}

// refinedUnknownStateError mirrors cmd/event/subscription/create.go's
// unknownStateError: a match exists in a remote state this CLI cannot classify
// (not active/suspended/expired/deleted), so bootstrapping fails closed rather
// than risk duplicating a still-live subscription. It guides inspect-and-decide.
func refinedUnknownStateError(resolved event.ResolvedEventKey, identity core.Identity, plan subown.SubscriptionPlan) error {
	id := ""
	state := ""
	if plan.Before != nil {
		id = plan.Before.ID.String()
		state = plan.Before.State
	}
	return errs.NewValidationError(errs.SubtypeFailedPrecondition,
		"a remote subscription already exists for %s in an unrecognized remote state %q (remote_subscription_id=%s); consume fails closed rather than risk duplicating it",
		resolved.MaterializedKey, state, id).
		WithParam("event_key").
		WithParams(plan.ConflictFields...).
		WithHint("run `lark-cli event subscription get %s --as %s --json` to inspect remote_subscription_id=%s and its state, then reactivate, delete, or wait as appropriate before re-running consume", id, identity, id)
}

// refinedIndeterminateError turns an ActionIndeterminate plan (the remote
// subscription list scan hit its page cap without a definitive answer) into a
// fail-closed typed error on a real run: bootstrapping a subscription now could
// duplicate an existing one beyond the pages read, so the consumer is NOT started.
func refinedIndeterminateError(resolved event.ResolvedEventKey, identity core.Identity) error {
	return errs.NewValidationError(errs.SubtypeFailedPrecondition,
		"cannot start consuming %s: could not determine whether a matching remote subscription already exists — the subscription list scan reached its %d-page cap before finding a match or exhausting all results",
		resolved.MaterializedKey, event.MaxSubscriptionListPages).
		WithParam("event_key").
		WithHint("the consumer was NOT started. Run `lark-cli event subscription list --as %s --json` to inspect existing subscriptions (this is NOT confirmed absence, only \"no match within the pages read\"), then retry once you have confirmed none matches", identity)
}

// applyOkRecoveryHint builds the recovery guidance shared by EVERY
// post-Apply failure: once Apply has succeeded, a remote write may have just
// happened, and refined cleanup is nil — no
// later failure in this chain (local bus won't start, HelloV2
// transport/decode error, or the bus rejecting the handshake) may ever
// delete it. All three must therefore surface the SAME actionable
// remote_subscription_id / created_by_this_attempt / next_action triple so
// retrying is understood to safely reconcile/reuse the existing remote
// subscription rather than risk creating a duplicate. Extracted so the three
// call sites (errApplyOkStartBusFailed, errApplyOkHelloFailed,
// applyOkRejectedError) construct byte-identical wording instead of each
// maintaining its own copy.
func applyOkRecoveryHint(resolved event.ResolvedEventKey, identity core.Identity, remoteSubscriptionID string, createdByThisAttempt bool) string {
	return fmt.Sprintf("remote_subscription_id=%s created_by_this_attempt=%t next_action=retry `lark-cli event consume %s --as %s` (the remote subscription was left as-is; retrying will reconcile/reuse it, not create a duplicate)",
		remoteSubscriptionID, createdByThisAttempt, resolved.MaterializedKey, identity)
}

// errApplyOkStartBusFailed implements the explicit
// apply-succeeded-but-bus-failed contract: typed InternalError, Hint carries
// remote_subscription_id + created_by_this_attempt + next_action. This never
// deletes the remote subscription it (maybe) just created — refined cleanup
// is nil: a transient local bus failure must not undo a
// successful remote write. The safe recovery is to retry consume, which
// will Plan-reuse the same remote subscription rather than create a
// duplicate.
func errApplyOkStartBusFailed(resolved event.ResolvedEventKey, identity core.Identity, remoteSubscriptionID string, createdByThisAttempt bool, cause error) error {
	return errs.NewInternalError(errs.SubtypeUnknown,
		"remote subscription is ready but the local event bus failed to start: %s", cause).
		WithCause(cause).
		WithHint(applyOkRecoveryHint(resolved, identity, remoteSubscriptionID, createdByThisAttempt))
}

// errApplyOkHelloFailed is errApplyOkStartBusFailed's HelloV2 counterpart. A
// transport/decode error during the HelloV2 handshake happens after Apply
// already succeeded and the bus already started, so it must be as actionable
// as a startBus failure and carry the same recovery hint.
func errApplyOkHelloFailed(resolved event.ResolvedEventKey, identity core.Identity, remoteSubscriptionID string, createdByThisAttempt bool, cause error) error {
	return errs.NewInternalError(errs.SubtypeUnknown,
		"remote subscription is ready but the event bus handshake failed: %s", cause).
		WithCause(cause).
		WithHint(applyOkRecoveryHint(resolved, identity, remoteSubscriptionID, createdByThisAttempt))
}

// applyOkRejectedError is errApplyOkStartBusFailed's bus-rejection
// counterpart. rejectionError (consume.go, shared with the legacy Run path)
// already returns a typed failed_precondition naming the single-consumer
// conflict; this appends the same remote_subscription_id/
// created_by_this_attempt/next_action recovery hint used by other post-Apply
// failures. The original hint is preserved so the local rejection reason and
// the remote recovery context are both visible.
func applyOkRejectedError(resolved event.ResolvedEventKey, identity core.Identity, remoteSubscriptionID string, createdByThisAttempt bool, rejErr error) error {
	var ve *errs.ValidationError
	if !errors.As(rejErr, &ve) {
		return rejErr
	}
	return ve.WithHint("%s; %s", ve.Hint, applyOkRecoveryHint(resolved, identity, remoteSubscriptionID, createdByThisAttempt))
}

// writeRefinedDryRunPreview reports the plan on stderr — stdout is reserved
// for business-event NDJSON even during a real run, so a
// --dry-run preview (never a business event) never touches it; a dry-run
// leaves stdout completely empty. Structured, no secrets: only the event
// key/target resource/planned action/existing remote id, nothing UAT- or
// token-shaped ever flows through this.
func writeRefinedDryRunPreview(errOut io.Writer, resolved event.ResolvedEventKey, plan subown.SubscriptionPlan) {
	fmt.Fprintf(errOut, "[event] dry-run: event_key=%s target_resource=%s planned_action=%s",
		resolved.MaterializedKey, resolved.TargetResource, plan.Action)
	if plan.Before != nil {
		fmt.Fprintf(errOut, " remote_subscription_id=%s", plan.Before.ID.String())
	}
	fmt.Fprintln(errOut)
	fmt.Fprintf(errOut, "[event] dry-run: next_action=run without --dry-run to apply this plan and start consuming %s\n", resolved.MaterializedKey)
}

// buildHelloV2 constructs the v2-populated Hello frame for a refined
// consumer's HelloV2 stage. Version stays frozen at
// "v1" — protocol.StatusResponse's own doc comment: the v2 signal is the
// Capabilities marker, never a Version bump. localSubscriptionID keeps its
// frozen meaning as the LOCAL per-param fingerprint (fingerprint.go) and is
// never repurposed to carry the remote id; remoteSubscriptionID is the
// separate v2 field for that. UserOpenID is only ever set for a user
// identity — a bot Hello never carries a stray open_id. targetResource is
// this consumer's own resolved target resource; the bus stores it on the
// registered Conn as this consumer's local listening intent, so a later
// updated_v1 lifecycle event can be compared against what was actually asked
// for rather than against Authority alone.
func buildHelloV2(resolved event.ResolvedEventKey, identity core.Identity, localSubscriptionID, profile, userOpenID, remoteSubscriptionID, consumerScopeID, targetResource string) *protocol.Hello {
	h := protocol.NewHello(os.Getpid(), resolved.MaterializedKey, []string{resolved.Definition.EventType}, "v1", localSubscriptionID)
	h.Identity = string(identity)
	h.Profile = profile
	if !identity.IsBot() {
		h.UserOpenID = userOpenID
	}
	h.RemoteSubscriptionID = remoteSubscriptionID
	h.ConsumerScopeID = consumerScopeID
	h.TargetResource = targetResource
	h.Capabilities = []string{protocol.CapabilityHelloV2}
	return h
}

// computeConsumerScopeID composes ConsumerScopeID: a
// stable hash of base EventKey + canonical refined key (MaterializedKey) +
// authority type ("user"/"app") + app_id + user_open_id. Deterministic —
// the same tuple always yields the same id, which is the point: it groups
// fan-out for "the same consumer scope" across process restarts. NUL-joined
// (rather than e.g. "|") so no component's own content can forge a
// delimiter collision.
func computeConsumerScopeID(baseKey, materializedKey, authorityType, appID, userOpenID string) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{baseKey, materializedKey, authorityType, appID, userOpenID}, "\x00")))
	return hex.EncodeToString(sum[:])
}

// authorityTypeFor maps a resolved CLI identity to the remote Subscription's
// own Authority.Type vocabulary ("app" for bot, "user" for user — the same
// type-based match the subscription Observer uses) — a DIFFERENT vocabulary from
// Hello.Identity's "bot"/"user" strings, matching the remote OAPI's own
// terminology since ConsumerScopeID is meant to line up with it.
func authorityTypeFor(identity core.Identity) string {
	if identity.IsBot() {
		return "app"
	}
	return "user"
}
