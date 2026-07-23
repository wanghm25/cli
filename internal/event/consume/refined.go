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

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/protocol"
	"github.com/larksuite/cli/internal/event/transport"
)

// subscriptionApplyAPI is ApplyRemoteSubscriptionPlan's seam: List (reused by
// PlanRemoteSubscription via event.ReconcileExisting; SubscriptionLister),
// Create (event.SubscriptionCreateAPI's write half), and Reactivate (the
// suspended-plan write path). It embeds event.SubscriptionCreateAPI — the
// ready-made seam Task 15a exported for exactly this purpose (see
// internal/event/reconcile.go's own doc comment) — rather than redeclaring
// List+Create here. *event.SubscriptionClient satisfies this structurally
// (Create/Get/List/Patch/Renew/Reactivate/Delete); no explicit "implements"
// needed. Deliberately does NOT expose Delete: refined cleanup is nil (spec
// §4.2 — a failure after Apply never auto-deletes the remote subscription),
// so the type this whole chain is built against cannot even call Delete.
type subscriptionApplyAPI interface {
	event.SubscriptionCreateAPI
	Reactivate(ctx context.Context, req *larkeventv1.ReactivateSubscriptionReq) (*larkeventv1.ReactivateSubscriptionResp, error)
	// EncryptKeyProber (task E5): PlanRemoteSubscription needs it as the
	// WithEncryptKeyProber for the encryption conflict matrix (spec §4.7) when
	// IncludeResourceData is true, and the pre-ready prewarm calls GetEncryptKey
	// to confirm the subscription's key is retrievable before the consumer goes
	// ready. *event.SubscriptionClient satisfies it (GetEncryptKey, task E1).
	event.EncryptKeyProber
}

// RefinedOptions holds RunRefined's own parameters — deliberately separate
// from Options (which Run/consumeLoop/writeReadyMarker still use unchanged)
// so the legacy path is never perturbed by a refined-only field (design
// note: "do NOT add refined fields to Options/Run").
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
	// no remote write (security-relevant invariant, spec §4.2).
	DryRun bool

	// IncludeResourceData (task E5, spec §4.7): true creates an ENCRYPTED
	// remote Subscription — Plan reconciles against the encryption conflict
	// matrix (WithEncryptKeyProber), Apply generates a fresh CSPRNG encrypt_key
	// and submits it atomically with the Create, and a pre-ready prewarm
	// confirms the key is retrievable before the consumer emits ready. false
	// (the default) is the pre-E5 plaintext path, byte-for-byte unchanged.
	IncludeResourceData bool

	// Identity is the already-resolved --as identity (design spec §2.8 —
	// resolved by the caller, e.g. cmd/event/consume.go's resolveIdentity;
	// RunRefined never guesses or re-resolves it).
	Identity core.Identity

	// SubClient is the already-constructed, identity-bound remote
	// subscription seam PlanRemoteSubscription/ApplyRemoteSubscriptionPlan
	// call (List for Plan; Create/Reactivate for Apply).
	SubClient subscriptionApplyAPI
}

// refinedDeps are RunRefined's injectable seams for the 5 ordered stages
// (design note's refinedDeps shape) so refined_test.go can assert strict
// order — probe < plan < apply < startBus < hello — and the dry-run
// short-circuit with a call-recorder, never touching a real network or bus.
// Production wiring is prodRefinedDeps below; tests substitute their own
// closures directly.
type refinedDeps struct {
	probe    func(ctx context.Context) error
	plan     func(ctx context.Context) (event.ReconcilePlan, error)
	apply    func(ctx context.Context, plan event.ReconcilePlan) (remoteSubscriptionID string, createdByThisAttempt bool, err error)
	startBus func(ctx context.Context) (net.Conn, error)
	hello    func(ctx context.Context, conn net.Conn, remoteSubscriptionID string) (*protocol.HelloAck, *bufio.Reader, error)
	// prewarm (task E5) runs AFTER a successful HelloV2 registration and BEFORE
	// the ready marker (spec §4.7): for an encrypted subscription it confirms
	// the bus will be able to obtain the encrypt_key (by fetching it with the
	// same authority the bus uses), so the consumer never goes ready when its
	// events could not be decrypted. nil (or a no-op) for a plaintext
	// subscription. A non-nil error means "do NOT ready" — runRefinedChain
	// rolls back the local registration (the deferred conn.Close unregisters).
	prewarm func(ctx context.Context, remoteSubscriptionID string) error
}

// RunRefined drives the refined-subscription `event consume` startup chain
// (design spec §4.1/§4.2/§4.9): ProbeBusEligibility (read-only) ->
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
		plan: func(ctx context.Context) (event.ReconcilePlan, error) {
			// Task E5: an encrypted request (IncludeResourceData=true) must
			// reconcile against spec §4.7's encryption conflict matrix — supply
			// the EncryptKeyProber so an active include_resource_data=true match
			// is disambiguated (reuse vs conflict) exactly as `event
			// subscription create` does. The plaintext path passes no option and
			// is byte-for-byte unchanged.
			var reconcileOpts []event.ReconcileOption
			if opts.IncludeResourceData {
				reconcileOpts = append(reconcileOpts, event.WithEncryptKeyProber(opts.SubClient))
			}
			plan, err := event.ReconcileExisting(ctx, opts.SubClient, eventType, targetResource, opts.Identity, opts.IncludeResourceData, reconcileOpts...)
			if err != nil {
				return event.ReconcilePlan{}, err
			}
			return *plan, nil
		},
		apply: func(ctx context.Context, plan event.ReconcilePlan) (string, bool, error) {
			return applyRemoteSubscriptionPlan(ctx, opts.SubClient, eventType, targetResource, plan, opts.IncludeResourceData)
		},
		prewarm: func(ctx context.Context, remoteSubscriptionID string) error {
			if !opts.IncludeResourceData {
				return nil // plaintext subscription: nothing to decrypt, nothing to prewarm
			}
			return prewarmEncryptKey(ctx, opts.SubClient, remoteSubscriptionID)
		},
		startBus: func(ctx context.Context) (net.Conn, error) {
			return EnsureBus(ctx, tr, appID, profileName, domain, opts.RemoteAPIClient, opts.ErrOut)
		},
		hello: func(_ context.Context, conn net.Conn, remoteSubscriptionID string) (*protocol.HelloAck, *bufio.Reader, error) {
			localSubscriptionID := ComputeSubscriptionID(resolved.Definition, opts.Params)

			// Resolve THIS process's own current profile/user_open_id the
			// exact same way the bus-side identity gate re-derives "current"
			// (internal/event/bus/identity.go resolveCurrentIdentity:
			// core.LoadMultiAppConfig -> CurrentAppConfig("") -> Users[0]) --
			// so the owner HelloV2 establishes at register time is, by
			// construction, exactly "current" the instant the bus re-checks
			// it, with no self-inflicted stale_identity gate.
			profile, userOpenID, err := resolveCurrentProfileIdentity()
			if err != nil {
				return nil, nil, fmt.Errorf("resolve current profile identity for hello: %w", err)
			}

			// scopeUserOpenID mirrors buildHelloV2's own bot-drops-UserOpenID
			// rule (review Fix 2, Minor #3): a bot consumer's ConsumerScopeID
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

			hello := buildHelloV2(resolved, opts.Identity, localSubscriptionID, profile, userOpenID, remoteSubscriptionID, consumerScopeID)
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

	// A conflicting existing subscription is the one plan outcome Apply
	// cannot safely resolve on its own (create/reuse/suspended all proceed
	// below) — a real run fails closed instead of guessing.
	if plan.Action == event.PlanActionConflict {
		return refinedConflictError(resolved, opts.Identity, plan)
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
		// succeeded, so this must carry the same recovery Hint (review Fix
		// 3), not a generic unactionable message.
		return errApplyOkHelloFailed(resolved, opts.Identity, remoteSubscriptionID, createdByThisAttempt, err)
	}
	if rejErr := rejectionError(ack, resolved.MaterializedKey); rejErr != nil {
		return applyOkRejectedError(resolved, opts.Identity, remoteSubscriptionID, createdByThisAttempt, rejErr)
	}

	// ---- 6.5 Prewarm the encrypt_key BEFORE ready (task E5, spec §4.7) ----
	// For an encrypted subscription, confirm the key is retrievable (so the bus
	// can decrypt) before this consumer ever reports ready. A failure here must
	// NOT ready the consumer: return a structured error. Rollback of the local
	// registration is the deferred conn.Close() above — closing the IPC
	// connection makes the bus unregister this consumer, so no half-registered
	// consumer is left behind.
	if deps.prewarm != nil {
		if err := deps.prewarm(ctx, remoteSubscriptionID); err != nil {
			return refinedPrewarmError(resolved, opts.Identity, remoteSubscriptionID, err)
		}
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
	// the ready line itself (design note stage 7 / spec §4.5's stdout
	// contract — the ready marker format is frozen).
	if remoteSubscriptionID != "" {
		fmt.Fprintf(errOut, "[event] refined consumer bound to remote_subscription_id=%s (created_by_this_attempt=%t)\n", remoteSubscriptionID, createdByThisAttempt)
	}

	// Ready marker uses the FILLED template instance as event_key (design
	// note stage 7) — consumeOpts.EventKey is already resolved.MaterializedKey.
	writeReadyMarker(errOut, consumeOpts)

	startTime := time.Now()
	var lastForKey bool
	var emitted atomic.Int64
	// No PreConsume/cleanup for refined consumers (Apply already handled the
	// remote-provisioning role PreConsume plays for legacy keys), so unlike
	// Run there is no cleanup to run panic-safely here — refined cleanup is
	// nil by design (spec §4.2).
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

// applyRemoteSubscriptionPlan is the refined startup chain's ONLY remote
// write (design spec §4.2 stage 4): Create for a fresh plan, Reactivate for
// a suspended match (auto-resumed — `event consume`'s job is to get the
// caller listening, unlike `event subscription create`'s own stricter
// suspended handling), a no-op reuse of the existing id for an
// already-active compatible match. Never called for PlanActionConflict —
// runRefinedChain returns a typed error before Apply in that case.
func applyRemoteSubscriptionPlan(ctx context.Context, svc subscriptionApplyAPI, eventType, targetResource string, plan event.ReconcilePlan, includeResourceData bool) (remoteSubscriptionID string, createdByThisAttempt bool, err error) {
	switch plan.Action {
	case event.PlanActionReuse:
		return strVal(plan.Existing.SubscriptionId), false, nil

	case event.PlanActionSuspended:
		id := strVal(plan.Existing.SubscriptionId)
		resp, err := svc.Reactivate(ctx, larkeventv1.NewReactivateSubscriptionReqBuilder().SubscriptionId(id).Build())
		if err != nil {
			return "", false, err
		}
		if resp != nil && resp.Data != nil && resp.Data.Subscription != nil && resp.Data.Subscription.SubscriptionId != nil {
			return strVal(resp.Data.Subscription.SubscriptionId), false, nil
		}
		return id, false, nil

	case event.PlanActionCreate:
		// Task E5 (spec §4.7): an encrypted request generates a fresh
		// per-subscription encrypt_key via the OS CSPRNG and submits it
		// ATOMICALLY in the same Create body as include_resource_data=true —
		// fail-closed: a key-gen failure aborts the create, never falls back to
		// a plaintext subscription. The key is used only to build this one body
		// and is never logged, persisted, returned, or sent to the bus over IPC
		// (the bus fetches its own copy via GetEncryptKey). The plaintext path
		// (includeResourceData=false) generates no key — byte-for-byte the
		// pre-E5 behavior.
		var encryptKey string
		if includeResourceData {
			encryptKey, err = newEncryptKeyFunc()
			if err != nil {
				return "", false, err
			}
		}
		body := buildRefinedCreateBody(eventType, targetResource, includeResourceData, encryptKey)
		resp, err := svc.Create(ctx, larkeventv1.NewCreateSubscriptionReqBuilder().Body(body).Build())
		if err != nil {
			return "", false, err
		}
		if resp == nil || resp.Data == nil || resp.Data.Subscription == nil {
			return "", false, errs.NewInternalError(errs.SubtypeInvalidResponse,
				"subscription create reported success but returned no subscription data")
		}
		return strVal(resp.Data.Subscription.SubscriptionId), true, nil

	default:
		return "", false, errs.NewInternalError(errs.SubtypeUnknown,
			"apply_remote_subscription_plan: unexpected reconcile plan action %q", plan.Action)
	}
}

// newEncryptKeyFunc is indirected (mirroring cmd/event/subscription/create.go's
// own var of the same name) so a test can spy that it is called EXACTLY once
// for an encrypted create and NEVER for a plaintext create / reuse / suspended
// / dry-run. Production code must never reassign it outside tests.
var newEncryptKeyFunc = event.NewEncryptKey

// buildRefinedCreateBody constructs the Create body, always setting
// include_resource_data and (when non-empty) the encrypt_key on the SAME
// CreatePayloadOptions within the SAME body value — spec §4.7's atomicity is
// structural here, exactly like cmd/event/subscription/create.go's
// buildCreateSubscriptionBody. Split out so a test can assert the atomicity
// directly against a plain, inspectable body value.
func buildRefinedCreateBody(eventType, targetResource string, includeResourceData bool, encryptKey string) *larkeventv1.CreateSubscriptionReqBody {
	payloadOptions := larkeventv1.NewCreatePayloadOptionsBuilder().IncludeResourceData(includeResourceData)
	if encryptKey != "" {
		payloadOptions = payloadOptions.Encrypt(larkeventv1.NewPayloadOptionsEncryptBuilder().EncryptKey(encryptKey).Build())
	}
	return larkeventv1.NewCreateSubscriptionReqBodyBuilder().
		EventType(eventType).
		TargetResource(targetResource).
		PayloadOptions(payloadOptions.Build()).
		Build()
}

// errPrewarmNoKey is prewarmEncryptKey's sentinel for "GetEncryptKey succeeded
// but returned no usable key" — a genuine (not transient) unavailability.
var errPrewarmNoKey = errors.New("subscription encrypt_key is not retrievable") //nolint:forbidigo // sentinel, typed at call site

// prewarmEncryptKey confirms the subscription's encrypt_key is retrievable with
// this identity/scope (task E5, spec §4.7's "输出 ready 之前预热"). It fetches
// via the SAME GetEncryptKey the bus-side provider uses, with the SAME
// authority (opts.SubClient is bound to the resolved --as identity, whose
// current profile HelloV2 already established as the owner) — so a success here
// guarantees the bus's own first-event fetch will succeed too. It deliberately
// never returns or logs the key value: it only checks non-empty, then discards
// resp (spec §4.7 敏感信息红线).
func prewarmEncryptKey(ctx context.Context, prober event.EncryptKeyProber, remoteSubscriptionID string) error {
	req := larkeventv1.NewGetEncryptKeySubscriptionReqBuilder().SubscriptionId(remoteSubscriptionID).Build()
	resp, err := prober.GetEncryptKey(ctx, req)
	if err != nil {
		return err // already typed (WrapDoAPIError/classifyFailure); classified transient vs genuine by the caller
	}
	if resp == nil || resp.Data == nil || strVal(resp.Data.EncryptKey) == "" {
		return errPrewarmNoKey
	}
	return nil
}

// refinedPrewarmError turns a prewarm failure into the structured error the
// consumer exits with instead of going ready (task E5). A transient failure
// (context cancel/deadline, or a typed network error) is returned UNCHANGED —
// it is retryable and must never be relabeled as a decrypt conflict (the
// E2-review lesson). A genuine failure becomes a failed_precondition classified
// decrypt_key_unavailable, guiding the operator to fix scope/identity (or
// delete+recreate) — the consumer was NOT started.
func refinedPrewarmError(resolved event.ResolvedEventKey, identity core.Identity, remoteSubscriptionID string, cause error) error {
	if isTransientPrewarmErr(cause) {
		return cause
	}
	return errs.NewValidationError(errs.SubtypeFailedPrecondition,
		"cannot start consuming %s: its subscription encrypt_key is not retrievable (decrypt_key_unavailable), so the event bus could not decrypt its events",
		resolved.MaterializedKey).
		WithParam("--include-resource-data").
		WithHint("the consumer was NOT started (no half-registered consumer remains). Ensure identity %s holds scope `event:encrypt_key:read` and is the owner of remote_subscription_id=%s (switch --as/--profile if this identity is not the owner), then retry `lark-cli event consume %s --as %s`; or delete and recreate the subscription after human confirmation",
			identity, remoteSubscriptionID, resolved.MaterializedKey, identity).
		WithCause(cause)
}

// isTransientPrewarmErr reports whether a prewarm GetEncryptKey failure was a
// retryable transient (context cancel/deadline, or a typed network error)
// rather than a confirmed unavailability.
func isTransientPrewarmErr(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne *errs.NetworkError
	return errors.As(err, &ne)
}

// refinedConflictError mirrors cmd/event/subscription/create.go's own
// conflictError for PlanActionConflict — the one ReconcilePlan outcome
// runRefinedChain always turns into a typed error on a real run (create,
// reuse, and suspended all proceed to Apply instead).
func refinedConflictError(resolved event.ResolvedEventKey, identity core.Identity, plan event.ReconcilePlan) error {
	id := strVal(plan.Existing.SubscriptionId)
	return errs.NewValidationError(errs.SubtypeFailedPrecondition,
		"an active remote subscription already exists for %s with a conflicting configuration (remote_subscription_id=%s)",
		resolved.MaterializedKey, id).
		WithParam("event_key").
		WithParams(plan.ConflictFields...).
		WithHint("run `lark-cli event subscription get %s --as %s --json` to inspect remote_subscription_id=%s, then either accept its existing configuration or delete it before consuming", id, identity, id)
}

// applyOkRecoveryHint builds the recovery guidance shared by EVERY
// post-Apply failure (review Fix 3): once Apply has succeeded, a remote
// write may have just happened, and refined cleanup is nil (spec §4.2) — no
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

// errApplyOkStartBusFailed implements the design note's explicit
// apply-succeeded-but-bus-failed contract: typed InternalError, Hint carries
// remote_subscription_id + created_by_this_attempt + next_action. This never
// deletes the remote subscription it (maybe) just created — refined cleanup
// is nil (spec §4.2): a transient local bus failure must not undo a
// successful remote write. The safe recovery is to retry consume, which
// will Plan-reuse the same remote subscription rather than create a
// duplicate.
func errApplyOkStartBusFailed(resolved event.ResolvedEventKey, identity core.Identity, remoteSubscriptionID string, createdByThisAttempt bool, cause error) error {
	return errs.NewInternalError(errs.SubtypeUnknown,
		"remote subscription is ready but the local event bus failed to start: %s", cause).
		WithCause(cause).
		WithHint(applyOkRecoveryHint(resolved, identity, remoteSubscriptionID, createdByThisAttempt))
}

// errApplyOkHelloFailed is errApplyOkStartBusFailed's HelloV2 counterpart
// (review Fix 3, Minor #2): a transport/decode error during the HelloV2
// handshake, AFTER Apply already succeeded and the bus already started, must
// be exactly as actionable — same typed InternalError shape, same recovery
// Hint — as a startBus failure. Before this fix, this path returned a
// generic InternalError with no Hint at all, even though it leaves the exact
// same remote-subscription-behind situation.
func errApplyOkHelloFailed(resolved event.ResolvedEventKey, identity core.Identity, remoteSubscriptionID string, createdByThisAttempt bool, cause error) error {
	return errs.NewInternalError(errs.SubtypeUnknown,
		"remote subscription is ready but the event bus handshake failed: %s", cause).
		WithCause(cause).
		WithHint(applyOkRecoveryHint(resolved, identity, remoteSubscriptionID, createdByThisAttempt))
}

// applyOkRejectedError is errApplyOkStartBusFailed's bus-rejection
// counterpart (review Fix 3, Minor #2): rejectionError (consume.go, shared
// with the legacy Run path) already returns a typed failed_precondition
// naming the single-consumer conflict — that guidance stays useful and is
// deliberately left in Message untouched. What it lacks, and what this adds,
// is the SAME remote_subscription_id/created_by_this_attempt/next_action
// recovery Hint every other post-Apply failure in this chain carries — Apply
// already succeeded here too, so it is exactly as important to know the
// remote subscription was left as-is. Appended (never replacing) the
// original Hint so neither piece of guidance is lost. rejectionError only
// ever returns nil or a *errs.ValidationError (see its own doc comment); the
// errors.As failing is not a reachable case today, but falls back to
// returning rejErr unchanged rather than panicking if that ever changes.
func applyOkRejectedError(resolved event.ResolvedEventKey, identity core.Identity, remoteSubscriptionID string, createdByThisAttempt bool, rejErr error) error {
	var ve *errs.ValidationError
	if !errors.As(rejErr, &ve) {
		return rejErr
	}
	return ve.WithHint("%s; %s", ve.Hint, applyOkRecoveryHint(resolved, identity, remoteSubscriptionID, createdByThisAttempt))
}

// writeRefinedDryRunPreview reports the plan on stderr — stdout is reserved
// for business-event NDJSON even during a real run (design spec §4.5), so a
// --dry-run preview (never a business event) never touches it; a dry-run
// leaves stdout completely empty. Structured, no secrets: only the event
// key/target resource/planned action/existing remote id, nothing UAT- or
// token-shaped ever flows through this.
func writeRefinedDryRunPreview(errOut io.Writer, resolved event.ResolvedEventKey, plan event.ReconcilePlan) {
	fmt.Fprintf(errOut, "[event] dry-run: event_key=%s target_resource=%s planned_action=%s",
		resolved.MaterializedKey, resolved.TargetResource, plan.Action)
	if plan.Existing != nil {
		fmt.Fprintf(errOut, " remote_subscription_id=%s", strVal(plan.Existing.SubscriptionId))
	}
	fmt.Fprintln(errOut)
	fmt.Fprintf(errOut, "[event] dry-run: next_action=run without --dry-run to apply this plan and start consuming %s\n", resolved.MaterializedKey)
}

// buildHelloV2 constructs the v2-populated Hello frame for a refined
// consumer's HelloV2 stage (design spec §4.2/§4.3). Version stays frozen at
// "v1" — protocol.StatusResponse's own doc comment: the v2 signal is the
// Capabilities marker, never a Version bump. localSubscriptionID keeps its
// frozen meaning as the LOCAL per-param fingerprint (fingerprint.go) and is
// never repurposed to carry the remote id; remoteSubscriptionID is the
// separate v2 field for that. UserOpenID is only ever set for a user
// identity — a bot Hello never carries a stray open_id.
func buildHelloV2(resolved event.ResolvedEventKey, identity core.Identity, localSubscriptionID, profile, userOpenID, remoteSubscriptionID, consumerScopeID string) *protocol.Hello {
	h := protocol.NewHello(os.Getpid(), resolved.MaterializedKey, []string{resolved.Definition.EventType}, "v1", localSubscriptionID)
	h.Identity = string(identity)
	h.Profile = profile
	if !identity.IsBot() {
		h.UserOpenID = userOpenID
	}
	h.RemoteSubscriptionID = remoteSubscriptionID
	h.ConsumerScopeID = consumerScopeID
	h.Capabilities = []string{protocol.CapabilityHelloV2}
	return h
}

// resolveCurrentProfileIdentity resolves THIS process's own current
// profile/user_open_id via the exact same mechanism the bus-side identity
// gate uses for "current" (internal/event/bus/identity.go
// resolveCurrentIdentity: core.LoadMultiAppConfig -> CurrentAppConfig("") ->
// Users[0]) — uncached, reads config.json fresh, never a value cached from
// earlier in this process's own startup.
func resolveCurrentProfileIdentity() (profile, userOpenID string, err error) {
	multi, err := core.LoadMultiAppConfig()
	if err != nil {
		return "", "", err
	}
	app := multi.CurrentAppConfig("")
	if app == nil {
		return "", "", fmt.Errorf("refined consume: no current app config")
	}
	profile = app.ProfileName()
	if len(app.Users) > 0 {
		userOpenID = app.Users[0].UserOpenId
	}
	return profile, userOpenID, nil
}

// computeConsumerScopeID composes ConsumerScopeID (design spec §4.3:302): a
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
// own Authority.Type vocabulary ("app" for bot, "user" for user — see
// event.AuthorityMatchesIdentity) — a DIFFERENT vocabulary from
// Hello.Identity's "bot"/"user" strings, matching the remote OAPI's own
// terminology since ConsumerScopeID is meant to line up with it.
func authorityTypeFor(identity core.Identity) string {
	if identity.IsBot() {
		return "app"
	}
	return "user"
}

// strVal is a nil-safe *string dereference — this package's own tiny copy
// (internal/event/reconcile.go and cmd/event/subscription/subscription.go
// each already keep an independent one; internal/event/consume adding its
// own follows the same established repo pattern rather than reaching across
// a package boundary for a one-liner).
func strVal(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
