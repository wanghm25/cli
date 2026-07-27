// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

// Package lifecycle is the subscription lifecycle control plane: the bounded
// in-memory executor, its per-event actions (summary recording plus the real
// Reactivate/Renew/Get/BindUser action with the owner==current security gate),
// and the deleted-tombstone.
//
// It defines interfaces for everything it needs from its host bus (Conn,
// Registry, IdentityGate, SubscriptionClient) rather than importing the bus
// package, so the dependency only ever points bus -> lifecycle. The bus's
// concrete types satisfy these interfaces structurally.
package lifecycle

import (
	"context"

	"github.com/larksuite/cli/internal/event"
	lark "github.com/larksuite/cli/internal/event/platform/lark"
)

// Conn is one consumer connection, as the lifecycle actions need to observe
// and mutate it. Every method here is already present on the host's concrete
// connection type; the interface only narrows it to the lifecycle-relevant
// surface so this package never imports the bus package.
type Conn interface {
	// SetLifecycleSummary records one lifecycle event's summary (event type,
	// event id, and remote state — state applied only when non-empty).
	SetLifecycleSummary(eventType, eventID, state string)
	// SetSuspensionReason records suspension.code verbatim ("" clears it).
	SetSuspensionReason(reason string)
	// SetLastAction / SetLastActionError record the last remote action
	// attempted and its classified outcome ("" on success).
	SetLastAction(action string)
	SetLastActionError(reason string)
	// SetNextAction records the recommended recovery step ("" = none).
	SetNextAction(action string)
	// SetDegraded records a per-consumer degraded classification.
	SetDegraded(reason string)
	// DegradedReason returns the current degraded classification ("" = healthy).
	DegradedReason() string
	// ClearActionDegraded clears both the degraded reason and the next action
	// together (used when an action's outcome means "fully healthy again").
	ClearActionDegraded()
	// SetStaleIdentity marks owner != current.
	SetStaleIdentity()
	// OwnerAppID / OwnerUserOpenID are the owner identity fixed at
	// registration; OwnerUserOpenID()=="" marks a bot or legacy consumer.
	OwnerAppID() string
	OwnerUserOpenID() string
	// TargetResource / IncludeResourceDataIntent / FilterIntent are this
	// consumer's own local listening intent, compared against an updated_v1
	// event's after snapshot.
	TargetResource() string
	IncludeResourceDataIntent() bool
	FilterIntent() *event.Filter
}

// Registry looks up the consumers bound to a remote Subscription. The host
// converts its own concrete connection slice to []Conn exactly once here, so no
// handler downstream repeats the conversion.
type Registry interface {
	ConnsByRemoteSubscriptionID(remoteSubID string) []Conn
}

// CurrentIdentity is the CLI's active identity resolved at action time — the
// (appID, userOpenID) pair an eligible user consumer's owner must match. UAT is
// never part of it.
type CurrentIdentity struct {
	AppID      string
	UserOpenID string
}

// IdentityGate is the owner/current identity gate + BindUser wiring the real
// action relies on. resolveCurrent is read fresh on every resolution (never a
// bus-startup-cached value); resolveUAT mints a UAT for exactly the resolved
// current identity; bindConsumer performs the per-consumer bind sequence.
type IdentityGate interface {
	ResolveCurrent() (CurrentIdentity, error)
	ResolveUAT(ctx context.Context, appID, userOpenID string) (string, error)
	BindConsumer(ctx context.Context, c Conn) error
}

// SubscriptionClient is the narrow subset of the platform/lark
// SubscriptionGateway the real action needs: a single Get (state-source-of-truth
// reconcile) and a single Reactivate/Renew (at most one such remote call per
// event, never a retry). Declared here — narrower than the full gateway surface,
// and domain-typed (no larkeventv1.* crosses it) — purely as a test seam: the
// production *lark.Gateway satisfies it structurally, so production code passes
// it straight through while tests substitute a fake with no *lark.Client or
// network call involved. Reactivate/Renew return the refreshed RemoteSubscription
// to match the gateway's shape; the action discards it (only the error matters).
type SubscriptionClient interface {
	Get(ctx context.Context, remoteSubscriptionID string) (*lark.RemoteSubscription, error)
	Reactivate(ctx context.Context, remoteSubscriptionID string) (*lark.RemoteSubscription, error)
	Renew(ctx context.Context, remoteSubscriptionID string) (*lark.RemoteSubscription, error)
}
