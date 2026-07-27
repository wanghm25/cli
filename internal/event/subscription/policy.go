// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

// Action is the decision the Planner reaches for a subscription request against
// observed remote state. It is the shared vocabulary the Controller applies and
// callers render.
//
//   - Create: no blocking match; create a new remote Subscription.
//   - Reuse: an active, compatible match exists; return it idempotently.
//   - Reactivate: a compatible suspended match exists and the Policy resumes it.
//   - Update: reserved for the ExplicitUpdate policy (see update.go, which
//     currently patches directly through the gateway).
//   - Block: a match exists that must not be silently written over — either a
//     configuration conflict (ConflictFields is populated) or a suspended match
//     under a Policy that does not auto-resume (ConflictFields empty). The
//     caller renders its own typed error.
//   - Indeterminate: the remote scan was inconclusive (page cap reached without
//     a definitive answer); NEVER treated as Create.
type Action string

const (
	ActionCreate        Action = "create"
	ActionReuse         Action = "reuse"
	ActionReactivate    Action = "reactivate"
	ActionUpdate        Action = "update"
	ActionBlock         Action = "block"
	ActionIndeterminate Action = "indeterminate"
)

// Policy parameterizes the Planner's caller-specific decisions as data, so the
// same Planner serves both `event subscription create` and the refined
// `event consume` startup without either caller re-deciding them in a switch.
// The two decisions that differ are:
//
//   - probeEncryptedActive: for an active match whose include_resource_data is
//     already true when the request also wants encryption, whether to classify
//     reuse-vs-conflict NOW by probing GetEncryptKey (a management action with
//     no later key gate) or to reuse and defer key confirmation to the bus Hello
//     (a consumer whose bus is the authoritative fail-closed key gate). This is
//     the fix for "dry-run != real": one Planner, Policy-parameterized, instead
//     of a WithEncryptKeyProber-vs-WithDeferredEncryptKeyConfirmation split
//     living in two callers.
//   - suspendedAction: what a compatible suspended match becomes — ActionBlock
//     (do not overwrite; guide the human to reactivate) or ActionReactivate
//     (auto-resume and reuse).
//
// reconcileAfterCreateFailure controls the Controller's one bounded
// reconcile-after-a-failed-Create pass (a management create's "someone raced us"
// recovery); a consumer bootstrap does not re-list after a failed create.
//
// Policy is a value type with unexported fields, so callers use the exported
// instances below rather than constructing arbitrary behavior combinations.
type Policy struct {
	name                        string
	probeEncryptedActive        bool
	suspendedAction             Action
	reconcileAfterCreateFailure bool
}

var (
	// ManagementCreate is `event subscription create`'s policy: it classifies an
	// encrypted active match now (probe), refuses to overwrite a suspended match
	// (Block), and reconciles once more after a failed Create.
	ManagementCreate = Policy{
		name:                        "management_create",
		probeEncryptedActive:        true,
		suspendedAction:             ActionBlock,
		reconcileAfterCreateFailure: true,
	}

	// ConsumeBootstrap is the refined `event consume` startup policy: it defers
	// an encrypted match's key confirmation to the bus (no front-end probe),
	// auto-resumes a compatible suspended match (Reactivate), and does not
	// re-list after a failed Create.
	ConsumeBootstrap = Policy{
		name:                        "consume_bootstrap",
		probeEncryptedActive:        false,
		suspendedAction:             ActionReactivate,
		reconcileAfterCreateFailure: false,
	}

	// ExplicitUpdate is reserved for a future caller that routes an in-place
	// change through the Controller. `event subscription update` currently
	// patches directly via the gateway (it changes only a reversible filter, so
	// it needs neither the reconcile matrix nor the encrypt-key probe); this
	// value exists so the vocabulary is complete and open for that extension.
	ExplicitUpdate = Policy{
		name:                        "explicit_update",
		probeEncryptedActive:        false,
		suspendedAction:             ActionBlock,
		reconcileAfterCreateFailure: false,
	}
)

// Name returns the policy's stable identifier (for logging/diagnostics).
func (p Policy) Name() string { return p.name }
