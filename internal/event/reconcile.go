// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"context"
	"fmt"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/core"
)

// Reconcile plan actions (the remote-state state table). Exported so
// every caller that reconciles a materialized refined EventKey against
// remote Subscription state shares one vocabulary instead of each defining
// its own copy: `event subscription create`
// (cmd/event/subscription/create.go) originated this logic; the refined
// `event consume` startup chain's PlanRemoteSubscription stage is the
// second caller.
const (
	PlanActionCreate    = "create"
	PlanActionReuse     = "reuse"
	PlanActionConflict  = "conflict"
	PlanActionSuspended = "suspended"
)

// SubscriptionLister is the narrow read-only seam ReconcileExisting depends
// on: enough to classify existing remote Subscription state (event_type +
// target_resource + authority) without ever writing, so it is safe to call
// from a --dry-run / plan-only preflight (the
// PlanRemoteSubscription stage). *SubscriptionClient satisfies this structurally
// — no explicit "implements" declaration needed, Go interfaces are
// structural.
type SubscriptionLister interface {
	List(ctx context.Context, req *larkeventv1.ListSubscriptionReq) (*larkeventv1.ListSubscriptionResp, error)
}

// SubscriptionCreateAPI extends SubscriptionLister with the write path a
// caller needs once it has decided, from a ReconcilePlan, to actually
// create a new remote Subscription (the PlanActionCreate row of the
// state table). *SubscriptionClient satisfies this structurally.
type SubscriptionCreateAPI interface {
	SubscriptionLister
	Create(ctx context.Context, req *larkeventv1.CreateSubscriptionReq) (*larkeventv1.CreateSubscriptionResp, error)
}

// EncryptKeyProber is the narrow seam ReconcileExisting's encryption
// conflict-matrix needs
// once it reaches an active, include_resource_data=true remote match AND
// the local request also wants include_resource_data=true (i.e. wants it
// ENCRYPTED — see ReconcileExisting's own doc comment on why "true" always
// means "encrypted" for this CLI's fail-closed policy). Calling
// GetEncryptKey(subscription_id) is the ONLY available signal to tell a
// genuinely encrypted, key-retrievable-by-this-identity match apart from a
// plaintext resource_data match or one whose key this identity cannot
// retrieve: ordinary Subscription List/Get responses never return
// encrypt_key at all. *SubscriptionClient satisfies this
// structurally (it gained GetEncryptKey earlier); so does
// cmd/event/subscription's own createSubscriptionAPI test seam (identical
// method signature).
type EncryptKeyProber interface {
	GetEncryptKey(ctx context.Context, req *larkeventv1.GetEncryptKeySubscriptionReq) (*larkeventv1.GetEncryptKeySubscriptionResp, error)
}

// ReconcileOption customizes ReconcileExisting without changing its positional
// signature. WithEncryptKeyProber and WithDeferredEncryptKeyConfirmation only
// affect the requestedIncludeResourceData=true path (an active,
// include_resource_data=true match). WithRequestedFilter adds the filter reuse
// dimension and applies on every path; an unset requested filter is compared as
// empty, so a caller that supplies no filter option keeps the unchanged
// both-empty reuse behavior.
type ReconcileOption func(*reconcileConfig)

// reconcileConfig carries the optional dependencies ReconcileOption values can
// set. encryptProber/deferEncryptKeyConfirmation drive the encryption conflict
// matrix (exactly one applies to an active, include_resource_data=true match;
// which one is chosen by the caller's nature — a management action that must
// classify now, vs a consumer that has an authoritative bus-side key gate to
// fall back on). requestedFilter is the caller's requested server-side filter,
// compared against an active match's filter as an additional reuse dimension;
// unset means "no filter requested" (compared as an empty filter).
type reconcileConfig struct {
	encryptProber               EncryptKeyProber
	deferEncryptKeyConfirmation bool
	requestedFilter             *Filter
}

// WithEncryptKeyProber supplies the EncryptKeyProber ReconcileExisting uses
// to resolve the encryption conflict matrix — see EncryptKeyProber's
// own doc comment for exactly when it is invoked. It suits a management action
// (`event subscription create`) that must classify reuse-vs-conflict at Plan
// time because it has no later key gate to defer to. Callers whose local
// request has requestedIncludeResourceData=false never need this option:
// the branch it configures is unreachable for them.
func WithEncryptKeyProber(prober EncryptKeyProber) ReconcileOption {
	return func(c *reconcileConfig) { c.encryptProber = prober }
}

// WithRequestedFilter supplies the server-side event filter the caller is
// requesting, so ReconcileExisting can compare it against an active match's
// remote filter as an additional reuse dimension. Reuse of an active match
// requires the requested filter to Equal the remote one; any difference —
// including empty-requested against a filtered remote, or vice versa — is a
// conflict, mirroring the fail-closed include_resource_data reuse rule (a human
// resolves it via update or a new subscription, never a silent wrong reuse).
// Callers that request no filter may omit this option: an unset requested
// filter is compared as an empty filter, so both-empty stays a compatible
// reuse.
func WithRequestedFilter(f *Filter) ReconcileOption {
	return func(c *reconcileConfig) { c.requestedFilter = f }
}

// WithDeferredEncryptKeyConfirmation tells ReconcileExisting to treat an
// active, include_resource_data=true match as a reuse WITHOUT probing its
// encrypt_key. It suits `event consume`, whose bus fetches and confirms the key
// once at Hello time (the authoritative, fail-closed key gate: an unretrievable
// key rejects the Hello with decrypt_key_unavailable). The consume front-end
// therefore never calls GetEncryptKey itself — key availability is confirmed
// solely by the bus. Mutually exclusive with WithEncryptKeyProber; supplying
// neither on a requestedIncludeResourceData=true match is a fail-closed
// internal error.
func WithDeferredEncryptKeyConfirmation() ReconcileOption {
	return func(c *reconcileConfig) { c.deferEncryptKeyConfirmation = true }
}

// ReconcilePlan is the outcome of reconciling a create request against
// remote state (the state table), computed identically
// whether the caller is only previewing (--dry-run, or the
// PlanRemoteSubscription stage) or is about to write — only what happens AFTER
// the plan differs. Action is one of PlanActionCreate (no blocking match;
// proceed to Create), PlanActionReuse (an active, payload_options-compatible
// match exists; return it idempotently), PlanActionConflict (an active but
// differently-configured match exists), or PlanActionSuspended (a suspended
// match exists; do not overwrite it).
//
// A matched "expired" or "deleted"/invisible remote entry, or no match at
// all, all resolve to PlanActionCreate: expired and deleted subscriptions
// are inert (the platform explicitly withholds Renew/auto-Reactivate from
// expired, but nothing blocks a fresh Create the way an active or suspended
// entry does), so they are treated the same as "not found" rather than as a
// separate blocking state.
type ReconcilePlan struct {
	Action         string
	Existing       *larkeventv1.SubscriptionDetail
	ConflictFields []errs.InvalidParam

	// PaginationCapped is true only when Action==PlanActionCreate AND the
	// underlying List scan hit MaxSubscriptionListPages before finding an
	// authority match or exhausting has_more. It is never set alongside
	// Reuse/Conflict/Suspended -- those all require a match, and a match
	// always stops the scan early rather than via the cap. Reaching the cap
	// is NOT proof no matching remote Subscription exists, only that none was
	// found within the pages actually read -- callers must log this rather
	// than treat a capped scan as a confirmed negative.
	PaginationCapped bool
}

// ReconcileExisting is the read-only half: it lists remote
// Subscriptions matching event_type + target_resource, narrows to the one
// (if any) whose authority matches the effective identity, and classifies
// it per the state table. It never writes — safe to call from --dry-run or
// a plan-only preflight (the PlanRemoteSubscription stage).
//
// The List scan pages via WalkSubscriptionPages (bounded by
// MaxSubscriptionListPages, ctx-aware) rather than reading only the first
// page: an authority match on a later page is found exactly the same as one
// on the first, so a caller never misjudges Create against a match it simply
// didn't page far enough to see. See ReconcilePlan.PaginationCapped for what
// happens when the cap is reached with no match yet found.
//
// Authority matching is by type only ("user" vs "app"), not by open_id: the
// caller is expected to always issue List using the effective identity's
// own token (WithUserAccessToken for user, the app's own tenant token for
// bot — see SubscriptionClient's identityOptions), so the server itself
// already scopes a List response to that caller's own authority; a second,
// redundant open_id comparison would need an extra call (e.g. resolving "my
// own open_id") this does not otherwise need.
//
// opts carries the extension points. It is the encryption-conflict-matrix
// entry: a caller whose requestedIncludeResourceData is true — meaning it
// wants an ENCRYPTED subscription, since this CLI's own fail-closed policy
// never offers a "plaintext resource_data" request — must pass exactly one of
// WithEncryptKeyProber (classify an active include_resource_data=true match
// now, for a management action like create) or
// WithDeferredEncryptKeyConfirmation (reuse without probing and let the bus
// Hello confirm the key, for consume). A caller with
// requestedIncludeResourceData == false needs neither: the plaintext call site
// passes them and is completely unaffected by that extension.
//
// opts also carries WithRequestedFilter (the requested server-side filter). It
// applies on every path — an active match is only reused when the requested
// filter equals the remote one — and defaults, when omitted, to an empty
// filter that keeps the both-empty reuse behavior unchanged.
func ReconcileExisting(ctx context.Context, svc SubscriptionLister, eventType, targetResource string, identity core.Identity, requestedIncludeResourceData bool, opts ...ReconcileOption) (*ReconcilePlan, error) {
	cfg := reconcileConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	buildReq := func(pageToken string) *larkeventv1.ListSubscriptionReq {
		b := larkeventv1.NewListSubscriptionReqBuilder().
			EventType(eventType).
			TargetResource(targetResource)
		if pageToken != "" {
			b = b.PageToken(pageToken)
		}
		return b.Build()
	}

	var match *larkeventv1.SubscriptionDetail
	capped, err := WalkSubscriptionPages(ctx, svc, buildReq, func(item *larkeventv1.SubscriptionDetail) bool {
		if AuthorityMatchesIdentity(item.Authority, identity) {
			match = item
			return false // stop -- found our authority match
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	if match == nil {
		// No authority match within the pages actually read. If the page cap
		// was hit first, PaginationCapped tells the caller to log that
		// rather than treat it as confirmed truth; the safe default action
		// is still Create either way (an unconfirmed "not found" behaves the
		// same as a genuine one: the server's own unique-key enforcement is
		// the backstop against a real duplicate).
		return &ReconcilePlan{Action: PlanActionCreate, PaginationCapped: capped}, nil
	}

	switch strVal(match.State) {
	case "active":
		var existingIncluded bool
		if match.PayloadOptions != nil {
			existingIncluded = boolVal(match.PayloadOptions.IncludeResourceData)
		}
		if existingIncluded != requestedIncludeResourceData {
			return &ReconcilePlan{
				Action:   PlanActionConflict,
				Existing: match,
				ConflictFields: []errs.InvalidParam{{
					Name:   "include_resource_data",
					Reason: fmt.Sprintf("existing subscription has include_resource_data=%t, this request has include_resource_data=%t", existingIncluded, requestedIncludeResourceData),
				}},
			}, nil
		}
		// Filter is a second reuse dimension. An active match may only be reused
		// when the requested filter equals the remote one; any difference
		// (including empty-vs-filtered either way) is a conflict a human
		// resolves. Checked here, ahead of the plaintext and encrypted reuse
		// paths below, so a filter mismatch blocks reuse regardless of encryption.
		if !Equal(cfg.requestedFilter, FilterFromSDK(match.Filter)) {
			return filterConflictPlan(match), nil
		}
		if !requestedIncludeResourceData {
			// Both sides agree on plain "no resource data" -- the
			// unencrypted reuse row; no encryption dimension applies.
			return &ReconcilePlan{Action: PlanActionReuse, Existing: match}, nil
		}
		// Both sides say include_resource_data=true (i.e. both want it
		// ENCRYPTED). An existing match reporting include_resource_data=true
		// is ambiguous on its own -- it may or may not have been created
		// with an encrypt_key, and ordinary List/Get never reveals that --
		// so how it is resolved depends on the caller's option:
		switch {
		case cfg.encryptProber != nil:
			// Management action (create): classify reuse-vs-conflict NOW by
			// probing the key; it has no later key gate to defer to. See
			// probeEncryptedActiveMatch.
			return probeEncryptedActiveMatch(ctx, cfg.encryptProber, cfg.requestedFilter, match)
		case cfg.deferEncryptKeyConfirmation:
			// Consumer (consume): reuse WITHOUT probing. The bus fetches and
			// confirms the key once at Hello time — the authoritative,
			// fail-closed key gate (an unretrievable key rejects the Hello with
			// decrypt_key_unavailable, not a front-end conflict) — so the
			// consume front-end never calls GetEncryptKey.
			return &ReconcilePlan{Action: PlanActionReuse, Existing: match}, nil
		default:
			// Defensive fail-closed: a requestedIncludeResourceData=true caller
			// must pick one of the two options above. Silently reusing here
			// without either would defeat the conflict matrix, so a misconfig is
			// an internal error rather than "assume compatible".
			return nil, errs.NewInternalError(errs.SubtypeUnknown,
				"reconcile: requestedIncludeResourceData=true requires WithEncryptKeyProber (classify now) or WithDeferredEncryptKeyConfirmation (defer to the bus) to resolve an existing include_resource_data=true match; neither was supplied")
		}
	case "suspended":
		// Filter is a reuse dimension on the suspended path too. A suspended
		// match is reactivated and reused by the caller; reactivating one whose
		// remote filter differs from the requested filter would bind the caller
		// to the wrong event stream, so a filter mismatch is a conflict a human
		// resolves (via update or a new subscription), never a silent
		// reactivate-reuse — the same fail-closed rule the active path enforces.
		if !Equal(cfg.requestedFilter, FilterFromSDK(match.Filter)) {
			return filterConflictPlan(match), nil
		}
		return &ReconcilePlan{Action: PlanActionSuspended, Existing: match}, nil
	default:
		// "expired", "deleted", "", or any other/unknown state: inert:
		// treat like not-found.
		return &ReconcilePlan{Action: PlanActionCreate}, nil
	}
}

// probeEncryptedActiveMatch implements the conflict-matrix rows for
// an active, include_resource_data=true remote match once the local request
// also wants include_resource_data=true (encrypted). GetEncryptKey is the
// only available signal:
//   - a usable (non-empty) encrypt_key comes back -> the match is genuinely
//     encrypted and THIS identity can retrieve its key -> reuse it; no new
//     Subscription or key is
//     ever created for an already-encrypted, reusable match.
//   - no key, an empty key, a nil Data, or ANY error (business or
//     transport) -> "plaintext resource_data" and "encrypted but key
//     unavailable to this identity/scope" are indistinguishable from this
//     response alone, and both are unsafe to auto-proceed -> conflict,
//     human decision. This deliberately does not try to
//     distinguish a transport failure from a confirmed empty key:
//     retrying `create` re-probes
//     from scratch, and a human resolving a genuine conflict needs the same
//     guidance (verify the subscription, check `event:encrypt_key:read`, or
//     delete+recreate) regardless of which case it was.
func probeEncryptedActiveMatch(ctx context.Context, prober EncryptKeyProber, requestedFilter *Filter, match *larkeventv1.SubscriptionDetail) (*ReconcilePlan, error) {
	// The filter reuse dimension applies to an encrypted match too: a filter
	// mismatch blocks reuse before the key is even relevant. ReconcileExisting's
	// active-match path already enforces this before calling here; repeating it
	// keeps this encrypted-reuse decision correct on its own terms.
	if !Equal(requestedFilter, FilterFromSDK(match.Filter)) {
		return filterConflictPlan(match), nil
	}
	id := strVal(match.SubscriptionId)
	req := larkeventv1.NewGetEncryptKeySubscriptionReqBuilder().SubscriptionId(id).Build()
	resp, err := prober.GetEncryptKey(ctx, req)
	if err == nil && resp != nil && resp.Data != nil && strVal(resp.Data.EncryptKey) != "" {
		return &ReconcilePlan{Action: PlanActionReuse, Existing: match}, nil
	}
	return &ReconcilePlan{
		Action:   PlanActionConflict,
		Existing: match,
		ConflictFields: []errs.InvalidParam{{
			Name:   "include_resource_data",
			Reason: "existing subscription has include_resource_data=true but its encrypt_key could not be confirmed retrievable with this identity (remote may be plaintext resource_data, or the key is unavailable — indistinguishable from here, and both unsafe to auto-reuse); delete and recreate after human confirmation, or verify the subscription and the `event:encrypt_key:read` scope",
		}},
	}, nil
}

// FilterConflictField is the ConflictFields name used for a server-side filter
// mismatch. Callers surfacing a conflict use ConflictOnFilter to detect this
// dimension so they can steer recovery to the non-destructive update path (a
// filter is changeable in place) rather than delete-and-recreate.
const FilterConflictField = "filter"

// filterConflictPlan builds the PlanActionConflict returned when a remote
// match's filter differs from the requested filter. The reason names
// only the dimension, never the filter contents or values on either side, so
// no filter payload can leak into an error string.
func filterConflictPlan(match *larkeventv1.SubscriptionDetail) *ReconcilePlan {
	return &ReconcilePlan{
		Action:   PlanActionConflict,
		Existing: match,
		ConflictFields: []errs.InvalidParam{{
			Name:   FilterConflictField,
			Reason: "the requested event filter does not match the existing subscription's filter",
		}},
	}
}

// ConflictOnFilter reports whether a conflict's fields include the server-side
// filter dimension. A filter difference is resolvable in place with
// `event subscription update`, so a caller can steer such a conflict to that
// non-destructive path instead of delete-and-recreate.
func ConflictOnFilter(fields []errs.InvalidParam) bool {
	for _, f := range fields {
		if f.Name == FilterConflictField {
			return true
		}
	}
	return false
}

// AuthorityMatchesIdentity reports whether a, an already-observed remote
// Authority, plausibly belongs to the given effective identity's own
// authority domain — see ReconcileExisting's doc comment for why type-only
// matching is sufficient here.
func AuthorityMatchesIdentity(a *larkeventv1.Authority, identity core.Identity) bool {
	if a == nil || a.Type == nil {
		return false
	}
	if identity.IsBot() {
		return *a.Type == "app"
	}
	return *a.Type == "user"
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
