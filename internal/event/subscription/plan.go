// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"context"
	"fmt"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/model"
)

// SubscriptionPlan is the Planner's classification of an Observation under a
// Policy — computed identically whether the caller is previewing (--dry-run) or
// about to write. Before is the existing remote match the plan is about (nil for
// a fresh Create). ConflictFields carries the fail-closed conflict dimensions
// for an ActionBlock caused by a configuration conflict (include_resource_data
// or filter), or the "state" dimension when a match is in an unrecognized
// remote state; it is empty for an ActionBlock caused by a suspended match the
// Policy refuses to overwrite, which is how a caller tells a config/state block
// from a suspended block. Reason is a short, secret-free machine token for
// logging (never any filter contents).
type SubscriptionPlan struct {
	Policy         Policy
	Action         Action
	Before         *model.RemoteSubscription
	Reason         string
	ConflictFields []errs.InvalidParam
}

// FilterConflictField is the ConflictFields Name used for a server-side filter
// mismatch. Callers use ConflictOnFilter to detect this dimension so they can
// steer recovery to the non-destructive `update` path (a filter is changeable in
// place) rather than delete-and-recreate.
const FilterConflictField = "filter"

// includeResourceDataField is the ConflictFields Name for an include_resource_data
// (encryption) mismatch.
const includeResourceDataField = "include_resource_data"

// StateConflictField is the ConflictFields Name used when a matched subscription
// is in a remote state this CLI does not recognize. Callers use ConflictOnState
// to detect this dimension and render an "unrecognized state" block (inspect the
// subscription and decide) rather than a configuration-conflict message.
const StateConflictField = "state"

// encryptKeyProber is the read-only seam the Planner needs to resolve the
// encryption conflict matrix for an active, include_resource_data=true match:
// GetEncryptKey is the ONLY signal that tells a genuinely encrypted,
// key-retrievable-by-this-identity match apart from a plaintext resource_data
// match or one whose key this identity cannot retrieve. *lark.Gateway satisfies
// it (its GetEncryptKey returns the key, or InvalidResponse for an empty one).
type encryptKeyProber interface {
	GetEncryptKey(ctx context.Context, remoteSubscriptionID string) (string, error)
}

// Planner classifies an Observation against a Request and Policy. It is
// read-only except for the encrypt-key probe (a GetEncryptKey call the
// ManagementCreate policy uses to classify an active encrypted match).
type Planner struct {
	prober encryptKeyProber
}

// NewPlanner builds a Planner. prober is used only on the ManagementCreate
// encrypted-active path; a Planner whose Policy never probes may hold a nil
// prober.
func NewPlanner(prober encryptKeyProber) Planner { return Planner{prober: prober} }

// Plan classifies obs under policy for req, preserving every conflict dimension
// the old reconcile enforced:
//   - Completeness=Indeterminate  => ActionIndeterminate (never Create).
//   - no authority match          => ActionCreate.
//   - active match: include_resource_data mismatch OR filter mismatch => Block
//     (with ConflictFields); else plain reuse; else (both encrypted) probe (for
//     a probing Policy) reuse-vs-Block, or reuse deferring to the bus.
//   - suspended match: the same include/filter conflict checks first (a
//     mismatch Blocks, never a silent reactivate-reuse); else the Policy's
//     suspendedAction (Block or Reactivate).
//   - expired/deleted state => ActionCreate (terminal/inert; treated as absent).
//   - any other/unrecognized state => ActionBlock (fail-closed: an unknown state
//     cannot be safely classified, so it is never silently Created or reused).
func (p Planner) Plan(ctx context.Context, obs Observation, policy Policy, req Request) (SubscriptionPlan, error) {
	if obs.Completeness == Indeterminate {
		return SubscriptionPlan{Policy: policy, Action: ActionIndeterminate, Reason: "list_incomplete"}, nil
	}

	match := obs.authorityMatch()
	if match == nil {
		return SubscriptionPlan{Policy: policy, Action: ActionCreate}, nil
	}

	switch match.State {
	case "active":
		if cf := includeConflict(match, req.IncludeResourceData); cf != nil {
			return blockPlan(policy, match, "include_resource_data_mismatch", cf), nil
		}
		// Filter is a second reuse dimension, checked ahead of the plaintext and
		// encrypted reuse paths so a filter mismatch blocks reuse regardless of
		// encryption.
		if !event.Equal(req.Filter, match.Filter) {
			return blockPlan(policy, match, "filter_mismatch", filterConflictFields()), nil
		}
		if !req.IncludeResourceData {
			// Both sides agree on plain "no resource data" -- unencrypted reuse.
			return reusePlan(policy, match), nil
		}
		// Both sides want include_resource_data=true (both want it ENCRYPTED). An
		// existing include_resource_data=true match is ambiguous on its own; how it
		// resolves depends on the Policy.
		if policy.probeEncryptedActive {
			return p.probeEncryptedActiveMatch(ctx, policy, match)
		}
		// Deferred (consume): reuse WITHOUT probing; the bus Hello is the
		// authoritative, fail-closed key gate.
		return reusePlan(policy, match), nil

	case "suspended":
		// A suspended match is reactivated and reused, so it must agree with the
		// request on the same reuse dimensions the active path checks: reactivating
		// one whose include_resource_data or filter differs would bind the caller
		// to the wrong encryption/event stream. Each mismatch Blocks (a human
		// resolves it), the same fail-closed rule the active path enforces.
		if cf := includeConflict(match, req.IncludeResourceData); cf != nil {
			return blockPlan(policy, match, "include_resource_data_mismatch", cf), nil
		}
		if !event.Equal(req.Filter, match.Filter) {
			return blockPlan(policy, match, "filter_mismatch", filterConflictFields()), nil
		}
		return SubscriptionPlan{Policy: policy, Action: policy.suspendedAction, Before: match, Reason: "suspended"}, nil

	case "expired", "deleted":
		// A terminal remote state is inert -- the subscription no longer
		// delivers and can be neither reused nor reactivated, so it is treated
		// like not-found: create a fresh one.
		return SubscriptionPlan{Policy: policy, Action: ActionCreate}, nil

	default:
		// Any other state -- the empty string, or any token this CLI does not
		// recognize -- cannot be safely classified. Fail closed: never silently
		// Create (which could duplicate a still-live subscription) and never
		// reuse/reactivate a state whose meaning is unknown. Block so a human
		// inspects the matched subscription and decides.
		return blockPlan(policy, match, "unrecognized_remote_state", stateConflictFields(match.State)), nil
	}
}

// probeEncryptedActiveMatch implements the conflict-matrix rows for an active,
// include_resource_data=true match once the request also wants it encrypted.
// GetEncryptKey is the only available signal: a usable (non-empty) key means the
// match is genuinely encrypted and this identity can retrieve it -> reuse.
//
// A FAILED probe no longer collapses into a blanket "delete-and-recreate" Block:
// that told a user with a transient transport blip to destroy their
// subscription and lost GetEncryptKey's own typed classification + cause. The
// gateway already classifies the failure (transport / auth / permission /
// invalid-response); the Planner PROPAGATES that typed error (Plan returns it as
// an error, which the caller surfaces) with a recovery that FITS the class —
// retry for transport, fix-scope for permission, re-auth for auth, and only the
// definitive empty-key (invalid_response) is genuinely "unconfirmed, verify or
// recreate after human confirmation". See encryptKeyProbeError.
func (p Planner) probeEncryptedActiveMatch(ctx context.Context, policy Policy, match *model.RemoteSubscription) (SubscriptionPlan, error) {
	if p.prober == nil {
		// Fail-closed: a probing Policy must be constructed with a prober.
		// Silently reusing here would defeat the conflict matrix.
		return SubscriptionPlan{}, errs.NewInternalError(errs.SubtypeUnknown,
			"subscription planner: policy %q classifies an active include_resource_data=true match by probing its encrypt_key, but no prober is configured", policy.name)
	}
	key, err := p.prober.GetEncryptKey(ctx, match.ID.String())
	if err != nil {
		return SubscriptionPlan{}, encryptKeyProbeError(err, match)
	}
	if key == "" {
		// The production gateway maps an empty key to InvalidResponse (so this is
		// unreachable there), but a direct prober could still return (\"\", nil);
		// treat it as the same definitive "no usable key" case rather than
		// silently reusing a possibly-plaintext subscription.
		return SubscriptionPlan{}, encryptKeyProbeError(
			errs.NewInternalError(errs.SubtypeInvalidResponse,
				"subscription get_encrypt_key for %s returned no encrypt_key", match.ID.String()),
			match)
	}
	return reusePlan(policy, match), nil
}

// encryptKeyProbeError maps a failed encrypt-key probe to a typed error that
// PRESERVES GetEncryptKey's own classification (transport / auth / permission /
// invalid-response) and its cause (errors.Is/Unwrap reaches the original), and
// carries a recovery that fits the class. A transient transport failure must
// tell the caller to RETRY, never to delete the subscription; a permission
// failure points at the missing scope; an auth failure points at re-auth; only a
// definitive empty key (invalid_response) is genuinely unconfirmed, and even
// then recreate is guarded behind human confirmation, never automatic. The
// Planner returns this as an error (not a Block), so the caller surfaces the
// real failure instead of a fabricated conflict.
func encryptKeyProbeError(err error, match *model.RemoteSubscription) error {
	id := match.ID.String()
	switch {
	case errs.IsNetwork(err):
		return errs.NewNetworkError(probeSubtype(err, errs.SubtypeNetworkTransport),
			"could not confirm the encrypt_key for remote subscription %s: the get_encrypt_key probe could not reach the server", id).
			WithRetryable().
			WithHint("this is a transient transport failure — retry `event subscription create`; do not delete or recreate the subscription").
			WithCause(err)
	case errs.IsPermission(err):
		return errs.NewPermissionError(probeSubtype(err, errs.SubtypeMissingScope),
			"could not confirm the encrypt_key for remote subscription %s: this identity is not permitted to read it", id).
			WithHint("grant this identity the `event:encrypt_key:read` scope — the existing subscription is fine, do not delete it — then retry `event subscription create`").
			WithCause(err)
	case errs.IsAuthentication(err):
		return errs.NewAuthenticationError(probeSubtype(err, errs.SubtypeTokenInvalid),
			"could not confirm the encrypt_key for remote subscription %s: authentication failed", id).
			WithHint("re-authenticate with `lark-cli auth login`, then retry `event subscription create`; do not delete the subscription").
			WithCause(err)
	default:
		// A definitive empty key (invalid_response) or any other classification:
		// the key is genuinely unconfirmed — the ONE case where verify-or-recreate
		// is honest guidance. Still NEVER an automatic delete: the remote could
		// simply be plaintext resource_data, which only a human can confirm.
		return errs.NewInternalError(probeSubtype(err, errs.SubtypeInvalidResponse),
			"could not confirm the encrypt_key for the active include_resource_data=true subscription %s with this identity", id).
			WithHint("verify remote_subscription_id=%s with `event subscription get %s` and this identity's `event:encrypt_key:read` scope; the remote may be plaintext resource_data or the key may be unavailable — both unsafe to auto-reuse — so delete and recreate only after human confirmation", id, id).
			WithCause(err)
	}
}

// probeSubtype returns err's own Subtype (preserving the gateway's
// classification) when it carries one, else fallback.
func probeSubtype(err error, fallback errs.Subtype) errs.Subtype {
	if p, ok := errs.ProblemOf(err); ok && p.Subtype != "" {
		return p.Subtype
	}
	return fallback
}

// reusePlan builds an ActionReuse plan for a compatible active match.
func reusePlan(policy Policy, match *model.RemoteSubscription) SubscriptionPlan {
	return SubscriptionPlan{Policy: policy, Action: ActionReuse, Before: match, Reason: "compatible"}
}

// blockPlan builds an ActionBlock plan carrying the conflict dimensions.
func blockPlan(policy Policy, match *model.RemoteSubscription, reason string, fields []errs.InvalidParam) SubscriptionPlan {
	return SubscriptionPlan{Policy: policy, Action: ActionBlock, Before: match, Reason: reason, ConflictFields: fields}
}

// includeConflict returns the include_resource_data ConflictFields when an
// existing match's include_resource_data differs from the request, or nil when
// they agree. The reason names only the boolean flags, never any payload.
func includeConflict(match *model.RemoteSubscription, requested bool) []errs.InvalidParam {
	existing := match.PayloadOptionsPresent && match.IncludeResourceData != nil && *match.IncludeResourceData
	if existing == requested {
		return nil
	}
	return []errs.InvalidParam{{
		Name:   includeResourceDataField,
		Reason: fmt.Sprintf("existing subscription has include_resource_data=%t, this request has include_resource_data=%t", existing, requested),
	}}
}

// filterConflictFields builds the filter-mismatch ConflictFields. The reason
// names only the dimension, never the filter contents or values on either side,
// so no filter payload can leak into an error string.
func filterConflictFields() []errs.InvalidParam {
	return []errs.InvalidParam{{
		Name:   FilterConflictField,
		Reason: "the requested event filter does not match the existing subscription's filter",
	}}
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

// stateConflictFields builds the ConflictFields for a match in an unrecognized
// remote state. The reason names the observed state token (a short status word,
// never a filter or secret) so a human can see what was found and decide.
func stateConflictFields(state string) []errs.InvalidParam {
	return []errs.InvalidParam{{
		Name:   StateConflictField,
		Reason: fmt.Sprintf("the existing subscription is in an unrecognized remote state %q; it cannot be safely reused, reactivated, or treated as absent, so it is not silently overwritten or duplicated", state),
	}}
}

// ConflictOnState reports whether a block's fields include the unrecognized-state
// dimension. Such a block is neither a configuration conflict nor a suspended
// match: the remote subscription's state could not be classified, so a human
// inspects it and decides.
func ConflictOnState(fields []errs.InvalidParam) bool {
	for _, f := range fields {
		if f.Name == StateConflictField {
			return true
		}
	}
	return false
}
