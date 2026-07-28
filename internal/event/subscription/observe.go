// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"context"

	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/model"
)

// Request is the classify input: the refined key's remote coordinates
// (EventType + TargetResource), the effective Identity whose authority the
// observation narrows to, and the desired reuse dimensions
// (IncludeResourceData, Filter). An empty/nil Filter is compared as "no filter".
type Request struct {
	EventType           string
	TargetResource      string
	Identity            core.Identity
	IncludeResourceData bool
	Filter              *event.Filter
}

// Completeness reports whether an Observation is a definitive answer or an
// inconclusive one. Complete means the scan either found the authority match or
// genuinely exhausted every page without one; Indeterminate means the paginated
// List scan hit MaxSubscriptionListPages with more pages still possible, so
// "no match found" is NOT confirmed absence — the Planner must never turn it
// into a Create.
type Completeness int

const (
	Complete Completeness = iota
	Indeterminate
)

// Observation is the Observer's read-only output: the authority-narrowed
// match(es) plus whether the scan was conclusive. By the platform's own unique
// key (event_type + target_resource + authority) there is at most one match, so
// Matches holds zero or one element; it stays a slice to keep the shape open if
// the platform ever relaxes that.
type Observation struct {
	Completeness Completeness
	Matches      []model.RemoteSubscription
}

// authorityMatch returns the single authority-narrowed match, or nil.
func (o Observation) authorityMatch() *model.RemoteSubscription {
	if len(o.Matches) == 0 {
		return nil
	}
	m := o.Matches[0]
	return &m
}

// walker is the narrow read seam the Observer needs from the gateway: the one
// bounded, ctx-aware paginated List scan. The full Gateway port (and the
// platform/lark adapter that implements it) satisfies it.
type walker interface {
	WalkSubscriptions(ctx context.Context, params ListParams, visit func(model.RemoteSubscription) bool) (bool, error)
}

// Observer reads remote Subscription state through the gateway and narrows it to
// the caller's own authority. It never writes, so it is safe on a --dry-run /
// plan-only path.
type Observer struct {
	gateway walker
}

// NewObserver builds an Observer over the gateway's paginated List scan.
func NewObserver(gw walker) Observer { return Observer{gateway: gw} }

// Observe scans remote Subscriptions matching req's event_type + target_resource
// and stops at the first whose authority matches req.Identity — exactly the
// behavior the old ReconcileExisting had, so an authority match on a later page
// is found the same as one on the first, and the scan stops as soon as it is
// found rather than reading every page.
//
// Authority is matched by type only ("user" vs "app"): callers always List using
// the effective identity's own token, so the server already scopes the response
// to that authority. A capped scan that found no match yields
// Completeness=Indeterminate; anything else (a match found, or every page
// genuinely read) is Complete.
func (o Observer) Observe(ctx context.Context, req Request) (Observation, error) {
	var match *model.RemoteSubscription
	capped, err := o.gateway.WalkSubscriptions(ctx,
		ListParams{EventType: req.EventType, TargetResource: req.TargetResource},
		func(sub model.RemoteSubscription) bool {
			if authorityMatchesIdentity(sub.Authority, req.Identity) {
				m := sub
				match = &m
				return false // stop -- found our authority match
			}
			return true
		})
	if err != nil {
		return Observation{}, err
	}
	obs := Observation{Completeness: Complete}
	switch {
	case match != nil:
		obs.Matches = []model.RemoteSubscription{*match}
	case capped:
		// No authority match within the pages actually read, and the scan hit
		// the page cap with more possibly remaining: not confirmed absence.
		obs.Completeness = Indeterminate
	}
	return obs, nil
}

// authorityMatchesIdentity reports whether a projected remote authority plausibly
// belongs to identity's own authority domain. Type-only matching is sufficient
// for the reason given on Observe: the List response is already authority-scoped
// by the caller's token. A user identity matches a "user" authority, a bot a
// "app" authority; an unset type never matches.
func authorityMatchesIdentity(a model.RemoteAuthority, identity core.Identity) bool {
	if a.Type == "" {
		return false
	}
	if identity.IsBot() {
		return a.Type == "app"
	}
	return a.Type == "user"
}
