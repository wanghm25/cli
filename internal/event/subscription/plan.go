// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"context"
	"fmt"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/event"
	lark "github.com/larksuite/cli/internal/event/platform/lark"
)

// SubscriptionPlan is the Planner's classification of an Observation under a
// Policy — computed identically whether the caller is previewing (--dry-run) or
// about to write. Before is the existing remote match the plan is about (nil for
// a fresh Create). ConflictFields carries the fail-closed conflict dimensions
// for an ActionBlock caused by a configuration conflict (include_resource_data
// or filter); it is empty for an ActionBlock caused by a suspended match the
// Policy refuses to overwrite, which is how a caller tells a config conflict
// from a suspended block. Reason is a short, secret-free machine token for
// logging (never any filter contents).
type SubscriptionPlan struct {
	Policy         Policy
	Action         Action
	Before         *lark.RemoteSubscription
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
//   - expired/deleted/unknown state => ActionCreate (inert; treated as absent).
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

	default:
		// "expired", "deleted", "", or any other/unknown state: inert -- treat
		// like not-found.
		return SubscriptionPlan{Policy: policy, Action: ActionCreate}, nil
	}
}

// probeEncryptedActiveMatch implements the conflict-matrix rows for an active,
// include_resource_data=true match once the request also wants it encrypted.
// GetEncryptKey is the only available signal: a usable (non-empty) key means the
// match is genuinely encrypted and this identity can retrieve it -> reuse; no
// key, an empty key, or ANY error (the gateway maps an empty key to
// InvalidResponse, and every business/transport failure surfaces as an error)
// -> Block, because "plaintext resource_data" and "encrypted but key
// unavailable to this identity" are indistinguishable from here and both unsafe
// to auto-reuse. It deliberately does not distinguish a transport failure from a
// confirmed empty key: retrying re-probes from scratch, and a human needs the
// same guidance either way.
func (p Planner) probeEncryptedActiveMatch(ctx context.Context, policy Policy, match *lark.RemoteSubscription) (SubscriptionPlan, error) {
	if p.prober == nil {
		// Fail-closed: a probing Policy must be constructed with a prober.
		// Silently reusing here would defeat the conflict matrix.
		return SubscriptionPlan{}, errs.NewInternalError(errs.SubtypeUnknown,
			"subscription planner: policy %q classifies an active include_resource_data=true match by probing its encrypt_key, but no prober is configured", policy.name)
	}
	key, err := p.prober.GetEncryptKey(ctx, match.ID.String())
	if err == nil && key != "" {
		return reusePlan(policy, match), nil
	}
	return blockPlan(policy, match, "encrypt_key_unconfirmed", []errs.InvalidParam{{
		Name:   includeResourceDataField,
		Reason: "existing subscription has include_resource_data=true but its encrypt_key could not be confirmed retrievable with this identity (remote may be plaintext resource_data, or the key is unavailable — indistinguishable from here, and both unsafe to auto-reuse); delete and recreate after human confirmation, or verify the subscription and the `event:encrypt_key:read` scope",
	}}), nil
}

// reusePlan builds an ActionReuse plan for a compatible active match.
func reusePlan(policy Policy, match *lark.RemoteSubscription) SubscriptionPlan {
	return SubscriptionPlan{Policy: policy, Action: ActionReuse, Before: match, Reason: "compatible"}
}

// blockPlan builds an ActionBlock plan carrying the conflict dimensions.
func blockPlan(policy Policy, match *lark.RemoteSubscription, reason string, fields []errs.InvalidParam) SubscriptionPlan {
	return SubscriptionPlan{Policy: policy, Action: ActionBlock, Before: match, Reason: reason, ConflictFields: fields}
}

// includeConflict returns the include_resource_data ConflictFields when an
// existing match's include_resource_data differs from the request, or nil when
// they agree. The reason names only the boolean flags, never any payload.
func includeConflict(match *lark.RemoteSubscription, requested bool) []errs.InvalidParam {
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
