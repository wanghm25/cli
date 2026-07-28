// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package model

// RemoteSubscription is the domain projection of a remote Lark Subscription —
// what the domain sees instead of the SDK's larkeventv1.SubscriptionDetail. It
// carries the fields the read / simple-write / reconcile callers actually use,
// as a pure value object: every field is a value type or another model value
// object (RemoteSubscriptionID, RemoteAuthority, the SDK-free *Filter), so no
// larkeventv1.* spelling — and no platform-package type — leaks into
// domain-facing contracts.
//
// It lives here, in the SDK-free value layer, so subscription / app / status /
// lifecycle speak a domain type rather than a platform/lark type (review #6:
// platform types must not leak into the domain). The platform gateway
// (internal/event/platform/lark) projects the SDK SubscriptionDetail into this
// type via ProjectSubscription; the domain only ever consumes it.
type RemoteSubscription struct {
	// ID is the remote management-side subscription_id.
	ID RemoteSubscriptionID
	// EventType is the subscription's OAPI event_type.
	EventType string
	// TargetResource is the "<resource>?<selector>" the subscription targets;
	// empty for a legacy (non-refined) subscription.
	TargetResource string
	// Authority is the projected subscription authority (user/app + open_id).
	Authority RemoteAuthority

	// PayloadOptionsPresent reports whether the SDK carried a payload_options
	// object at all; IncludeResourceData is its include_resource_data field
	// (nil when payload_options was absent OR carried no include_resource_data).
	// The two are kept distinct so callers reproduce the exact
	// present-vs-absent semantics the raw SDK response expressed.
	PayloadOptionsPresent bool
	IncludeResourceData   *bool

	// Filter is the server-side event filter as the CLI Filter model. It is
	// never nil once projected — an unfiltered subscription projects to an empty
	// *Filter (Filter.IsEmpty() == true) — so callers compare/canonicalize it
	// directly.
	Filter *Filter

	// State is the remote subscription state (open vocabulary: "active",
	// "suspended", "expired", ...).
	State string
	// SuspensionReason is suspension.code, present only for a suspended state.
	SuspensionReason string
	// ExpireTime/CreateTime/UpdateTime are Unix seconds; nil when the SDK
	// omitted them.
	ExpireTime *int
	CreateTime *int
	UpdateTime *int
}
