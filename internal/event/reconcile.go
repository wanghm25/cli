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

// Reconcile plan actions (design spec §3.3/§4.2's state table). Exported so
// every caller that reconciles a materialized refined EventKey against
// remote Subscription state shares one vocabulary instead of each defining
// its own copy: `event subscription create`
// (cmd/event/subscription/create.go) originated this logic; the refined
// `event consume` startup chain's PlanRemoteSubscription stage is the
// second caller (Task 15b).
const (
	PlanActionCreate    = "create"
	PlanActionReuse     = "reuse"
	PlanActionConflict  = "conflict"
	PlanActionSuspended = "suspended"
)

// SubscriptionLister is the narrow read-only seam ReconcileExisting depends
// on: enough to classify existing remote Subscription state (event_type +
// target_resource + authority) without ever writing, so it is safe to call
// from a --dry-run / plan-only preflight (Task 15b's
// PlanRemoteSubscription). *SubscriptionClient satisfies this structurally
// — no explicit "implements" declaration needed, Go interfaces are
// structural.
type SubscriptionLister interface {
	List(ctx context.Context, req *larkeventv1.ListSubscriptionReq) (*larkeventv1.ListSubscriptionResp, error)
}

// SubscriptionCreateAPI extends SubscriptionLister with the write path a
// caller needs once it has decided, from a ReconcilePlan, to actually
// create a new remote Subscription (the PlanActionCreate row of the
// §3.3/§4.2 state table). *SubscriptionClient satisfies this structurally.
type SubscriptionCreateAPI interface {
	SubscriptionLister
	Create(ctx context.Context, req *larkeventv1.CreateSubscriptionReq) (*larkeventv1.CreateSubscriptionResp, error)
}

// ReconcilePlan is the outcome of reconciling a create request against
// remote state (spec §3.3/§4.2's state table), computed identically
// whether the caller is only previewing (--dry-run, or Task 15b's
// PlanRemoteSubscription) or is about to write — only what happens AFTER
// the plan differs. Action is one of PlanActionCreate (no blocking match;
// proceed to Create), PlanActionReuse (an active, payload_options-compatible
// match exists; return it idempotently), PlanActionConflict (an active but
// differently-configured match exists), or PlanActionSuspended (a suspended
// match exists; do not overwrite it).
//
// A matched "expired" or "deleted"/invisible remote entry, or no match at
// all, all resolve to PlanActionCreate: expired and deleted subscriptions
// are inert (spec §3.3 explicitly withholds Renew/auto-Reactivate from
// expired, but nothing blocks a fresh Create the way an active or suspended
// entry does), so they are treated the same as "not found" rather than as a
// separate blocking state.
type ReconcilePlan struct {
	Action         string
	Existing       *larkeventv1.SubscriptionDetail
	ConflictFields []errs.InvalidParam
}

// ReconcileExisting is the read-only half of §3.3/§4.2: it lists remote
// Subscriptions matching event_type + target_resource, narrows to the one
// (if any) whose authority matches the effective identity, and classifies
// it per the state table. It never writes — safe to call from --dry-run or
// a plan-only preflight (Task 15b's PlanRemoteSubscription).
//
// Authority matching is by type only ("user" vs "app"), not by open_id: the
// caller is expected to always issue List using the effective identity's
// own token (WithUserAccessToken for user, the app's own tenant token for
// bot — see SubscriptionClient's identityOptions), so the server itself
// already scopes a List response to that caller's own authority; a second,
// redundant open_id comparison would need an extra call (e.g. resolving "my
// own open_id") this does not otherwise need.
func ReconcileExisting(ctx context.Context, svc SubscriptionLister, eventType, targetResource string, identity core.Identity, requestedIncludeResourceData bool) (*ReconcilePlan, error) {
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
		if existingIncluded == requestedIncludeResourceData {
			return &ReconcilePlan{Action: PlanActionReuse, Existing: match}, nil
		}
		return &ReconcilePlan{
			Action:   PlanActionConflict,
			Existing: match,
			ConflictFields: []errs.InvalidParam{{
				Name:   "include_resource_data",
				Reason: fmt.Sprintf("existing subscription has include_resource_data=%t, this request has include_resource_data=%t", existingIncluded, requestedIncludeResourceData),
			}},
		}, nil
	case "suspended":
		return &ReconcilePlan{Action: PlanActionSuspended, Existing: match}, nil
	default:
		// "expired", "deleted", "", or any other/unknown state: inert:
		// treat like not-found (spec §3.3).
		return &ReconcilePlan{Action: PlanActionCreate}, nil
	}
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
