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

// Event type strings, mirrored from source/feishu.go's identically-named,
// identically-valued but UNEXPORTED constants (lifecycleEventTypeActivated
// et al.) — this package cannot reference those directly since they're
// private to package source, and re-exporting them there is out of scope for
// this change. These 6 strings are SDK facts, stable across this whole
// feature — if they ever change, both copies must be updated together.
const (
	lifecycleEventTypeActivated          = "event.subscription.activated_v1"
	lifecycleEventTypeUpdated            = "event.subscription.updated_v1"
	lifecycleEventTypeSuspended          = "event.subscription.suspended_v1"
	lifecycleEventTypeExpirationReminder = "event.subscription.expiration_reminder_v1"
	lifecycleEventTypeExpired            = "event.subscription.expired_v1"
	// LifecycleEventTypeDeleted is exported because the deleted_v1 path (the
	// encrypt-key release) is verified from the host package's tests.
	LifecycleEventTypeDeleted = "event.subscription.deleted_v1"
)

// suspensionCodeAuthorityRevoked is the ONLY confirmed stable suspension.code
// value. suspension.code is otherwise an open string CLI never builds a closed
// enum for (source/feishu.go's LifecycleEvent.SuspensionCode doc) — any OTHER
// value (including a future, currently-unknown one) takes the "default branch"
// (reconcileWithGet) rather than guessing that Reactivate is the right
// recovery action.
const suspensionCodeAuthorityRevoked = "authority_revoked"

// reasonCurrentIdentityUnresolved is the SetDegraded reason used when
// resolveCurrent itself fails — distinct from stale_identity (which means
// current WAS resolved but didn't match this consumer's owner). It mirrors the
// host's identity-gate classification of the same name so a status display sees
// one consistent token regardless of which gate recorded it.
const reasonCurrentIdentityUnresolved = "current_identity_unresolved"

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

// ownerMatchesCurrent is the owner comparison: owner_app_id +
// owner_user_open_id, exactly. Never compares tokens. Mirrors the host's own
// identically-named comparison against its equivalent current-identity value.
func ownerMatchesCurrent(ownerAppID, ownerUserOpenID string, cur CurrentIdentity) bool {
	return ownerAppID == cur.AppID && ownerUserOpenID == cur.UserOpenID
}
