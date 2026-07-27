// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package lifecycle

import (
	"github.com/larksuite/cli/internal/event/source"
)

// LifecycleEvent re-exports source.LifecycleEvent under this package's own
// name (a type ALIAS, not a new type) so the rest of this package — and its
// tests — can write LifecycleEvent unqualified. The struct itself must live
// in package source (internal/event/source/feishu.go), not here:
// FeishuSource.OnLifecycleEvent's parameter type has to be nameable from
// within package source without a reverse import, so source must never import
// this package back. Do not "fix" this by moving the struct here — that would
// create an import cycle.
type LifecycleEvent = source.LifecycleEvent

// Event type strings, derived from source/feishu.go's exported constants
// (the single source of truth — see that package's own doc comment) so the
// two packages can never drift apart. Kept as local (mostly unexported)
// names purely so the rest of this package can write them unqualified.
const (
	lifecycleEventTypeActivated          = source.LifecycleEventTypeActivated
	lifecycleEventTypeUpdated            = source.LifecycleEventTypeUpdated
	lifecycleEventTypeSuspended          = source.LifecycleEventTypeSuspended
	lifecycleEventTypeExpirationReminder = source.LifecycleEventTypeExpirationReminder
	lifecycleEventTypeExpired            = source.LifecycleEventTypeExpired
	// LifecycleEventTypeDeleted is exported because the deleted_v1 path (the
	// encrypt-key release) is verified from the host package's tests.
	LifecycleEventTypeDeleted = source.LifecycleEventTypeDeleted
)

// suspensionCodeAuthorityRevoked is the ONLY confirmed stable suspension.code
// value. suspension.code is otherwise an open string CLI never builds a closed
// enum for (source/feishu.go's LifecycleEvent.SuspensionCode doc) — any OTHER
// value (including a future, currently-unknown one) takes the "default branch"
// (EffectReconcileGet / runReconcileGet) rather than guessing that Reactivate
// is the right recovery action.
const suspensionCodeAuthorityRevoked = "authority_revoked"

// --- degraded/next_action classification tokens ---
// Reused (never per-branch bespoke strings) so a status display can key off a
// small, stable vocabulary. reasonRemoteSubscriptionConflict's exact string is
// the canonical remote_subscription_conflict token; the others are our own
// short, consistent classifications, chosen to mirror the existing
// bind_failed:*/current_identity_unresolved/lifecycle_executor_full style
// already established elsewhere. Exported where the host package's tests assert
// on them; kept unexported where only this package uses them.
const (
	ReasonRemoteSubscriptionConflict     = "remote_subscription_conflict"
	ReasonRemoteSubscriptionSuspended    = "remote_subscription_suspended"
	ReasonRemoteSubscriptionExpired      = "remote_subscription_expired"
	ReasonRemoteSubscriptionExpiringSoon = "remote_subscription_expiring_soon"
	ReasonRemoteSubscriptionDeleted      = "remote_subscription_deleted"
	reasonRemoteStateUnreconciled        = "remote_state_unreconciled"
)

// next_action tokens. The recovery command is uniformly "reactivate" —
// NextActionReactivate is used verbatim, literally spelled "reactivate", never
// "reactive"/"resume".
const (
	NextActionReactivate = "reactivate"
	NextActionRenew      = "renew"
	NextActionRebuild    = "rebuild" // expired/deleted: guide to rebuild, never auto
	NextActionRebind     = "rebind"  // a local BindUser is needed/failed
	NextActionGet        = "get"     // inspect current remote state before deciding
)

// updateCompatibility classification (updated_v1 row).
const (
	updateCompatible   = "compatible"
	updateIncompatible = "incompatible"
	updateUnclear      = "unclear"
)
