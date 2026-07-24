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

// ReconcileOption customizes ReconcileExisting without changing its
// signature for existing callers: internal/event/consume/refined.go's
// PlanRemoteSubscription stage keeps
// calling ReconcileExisting exactly as it does today, passing zero options,
// and remains byte-for-byte unaffected — every option this type carries only
// changes behavior on the requestedIncludeResourceData=true path, which that
// call site never reaches (it always passes false).
type ReconcileOption func(*reconcileConfig)

// reconcileConfig carries the optional dependencies ReconcileOption values can
// set for the encryption conflict matrix. Exactly one of these applies to an
// active, include_resource_data=true match; which one is chosen by the caller's
// nature (a management action that must classify now, vs a consumer that has an
// authoritative bus-side key gate to fall back on).
type reconcileConfig struct {
	encryptProber               EncryptKeyProber
	deferEncryptKeyConfirmation bool
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
}

// ReconcileExisting is the read-only half: it lists remote
// Subscriptions matching event_type + target_resource, narrows to the one
// (if any) whose authority matches the effective identity, and classifies
// it per the state table. It never writes — safe to call from --dry-run or
// a plan-only preflight (the PlanRemoteSubscription stage).
//
// Authority matching is by type only ("user" vs "app"), not by open_id: the
// caller is expected to always issue List using the effective identity's
// own token (WithUserAccessToken for user, the app's own tenant token for
// bot — see SubscriptionClient's identityOptions), so the server itself
// already scopes a List response to that caller's own authority; a second,
// redundant open_id comparison would need an extra call (e.g. resolving "my
// own open_id") this does not otherwise need.
//
// opts is the encryption-conflict-matrix extension point: a caller whose
// requestedIncludeResourceData is true — meaning it
// wants an ENCRYPTED subscription, since this CLI's own fail-closed policy
// never offers a "plaintext resource_data" request — must pass exactly one of
// WithEncryptKeyProber (classify an active include_resource_data=true match
// now, for a management action like create) or
// WithDeferredEncryptKeyConfirmation (reuse without probing and let the bus
// Hello confirm the key, for consume). A caller with
// requestedIncludeResourceData == false needs no option at all: the plaintext
// call site passes none and is completely unaffected by this extension.
func ReconcileExisting(ctx context.Context, svc SubscriptionLister, eventType, targetResource string, identity core.Identity, requestedIncludeResourceData bool, opts ...ReconcileOption) (*ReconcilePlan, error) {
	cfg := reconcileConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	req := larkeventv1.NewListSubscriptionReqBuilder().
		EventType(eventType).
		TargetResource(targetResource).
		Build()
	resp, err := svc.List(ctx, req)
	if err != nil {
		return nil, err
	}

	var match *larkeventv1.SubscriptionDetail
	if resp != nil && resp.Data != nil {
		for _, item := range resp.Data.Items {
			if item != nil && AuthorityMatchesIdentity(item.Authority, identity) {
				match = item
				break
			}
		}
	}
	if match == nil {
		return &ReconcilePlan{Action: PlanActionCreate}, nil
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
			return probeEncryptedActiveMatch(ctx, cfg.encryptProber, match)
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
func probeEncryptedActiveMatch(ctx context.Context, prober EncryptKeyProber, match *larkeventv1.SubscriptionDetail) (*ReconcilePlan, error) {
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
