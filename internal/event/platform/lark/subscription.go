// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package lark

import (
	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/model"
)

// RemoteSubscription is the domain projection of a remote Lark Subscription —
// what callers see instead of larkeventv1.SubscriptionDetail. It carries the
// fields the read/simple-write callers actually use, with no SDK type in its
// shape except the CLI-owned *event.Filter (whose projection deliberately stays
// in package event; see event.FilterFromSDK).
//
// It lives here, in the gateway package, rather than in internal/event/model:
// its Filter field is *event.Filter, and package event imports the SDK, so a
// model that carried this type would transitively depend on the SDK — which
// model is verified never to do (and, since event imports model, would form an
// import cycle). Keeping RemoteSubscription with its projector is also cleanly
// open-closed: the gateway owns both the SDK type and its domain projection.
type RemoteSubscription struct {
	// ID is the remote management-side subscription_id.
	ID model.RemoteSubscriptionID
	// EventType is the subscription's OAPI event_type.
	EventType string
	// TargetResource is the "<resource>?<selector>" the subscription targets;
	// empty for a legacy (non-refined) subscription.
	TargetResource string
	// Authority is the projected subscription authority (user/app + open_id).
	Authority model.RemoteAuthority

	// PayloadOptionsPresent reports whether the SDK carried a payload_options
	// object at all; IncludeResourceData is its include_resource_data field
	// (nil when payload_options was absent OR carried no include_resource_data).
	// The two are kept distinct so callers reproduce the exact
	// present-vs-absent semantics the raw SDK response expressed.
	PayloadOptionsPresent bool
	IncludeResourceData   *bool

	// Filter is the server-side event filter as the CLI Filter model. It is
	// never nil — an unfiltered subscription projects to an empty *event.Filter
	// (Filter.IsEmpty() == true) — so callers compare/canonicalize it directly.
	Filter *event.Filter

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

// CreateSpec is the domain request for creating a remote Subscription. The
// gateway projects it into a larkeventv1 Create request body.
type CreateSpec struct {
	EventType           string
	TargetResource      string
	IncludeResourceData bool
	// EncryptKey, when non-empty, is injected atomically alongside
	// IncludeResourceData for an encrypted subscription. Callers must never log
	// it.
	EncryptKey string
	// Filter is the requested server-side filter; a nil/empty filter omits the
	// field entirely (create's "no server-side filter", distinct from the
	// update-only {"filter":{}} clear form).
	Filter *event.Filter
}

// PatchSpec is the domain request for patching a remote Subscription. Patch
// carries only the filter (the sole field update changes); an empty/cleared
// filter is sent as the {"filter":{}} clear form rather than an omitted field.
type PatchSpec struct {
	Filter *event.Filter
}

// ListParams narrows a Subscription List to one CLI-relevant scope. Every field
// is optional; an empty value omits that filter from the request.
type ListParams struct {
	State          string
	EventType      string
	TargetResource string
	PageToken      string
	PageSize       int
}

// SubscriptionPage is one page of a Subscription List: the projected items plus
// the raw pagination signals a caller (e.g. `event subscription list`) surfaces
// for user-driven paging.
type SubscriptionPage struct {
	Items         []RemoteSubscription
	HasMore       bool
	NextPageToken string
}

// ProjectSubscription faithfully projects an SDK SubscriptionDetail into the
// domain RemoteSubscription. A nil detail yields the zero value. It performs NO
// required-field validation (an empty id projects to an empty ID) — that is the
// gateway methods' job (requireSubscription), so this projection can be reused
// for List items and for the reconcile/create details create.go still holds
// while it lives on the legacy subscription_client (retired in PR2b).
func ProjectSubscription(d *larkeventv1.SubscriptionDetail) RemoteSubscription {
	if d == nil {
		return RemoteSubscription{}
	}
	sub := RemoteSubscription{
		ID:             model.RemoteSubscriptionID(strVal(d.SubscriptionId)),
		EventType:      strVal(d.EventType),
		TargetResource: strVal(d.TargetResource),
		Authority:      projectAuthority(d.Authority),
		Filter:         event.FilterFromSDK(d.Filter),
		State:          strVal(d.State),
		ExpireTime:     d.ExpireTime,
		CreateTime:     d.CreateTime,
		UpdateTime:     d.UpdateTime,
	}
	if d.PayloadOptions != nil {
		sub.PayloadOptionsPresent = true
		sub.IncludeResourceData = d.PayloadOptions.IncludeResourceData
	}
	if d.Suspension != nil {
		sub.SuspensionReason = strVal(d.Suspension.Code)
	}
	return sub
}

// projectAuthority projects an SDK Authority into the domain value object. A nil
// authority yields the zero RemoteAuthority (its String() renders "").
func projectAuthority(a *larkeventv1.Authority) model.RemoteAuthority {
	if a == nil {
		return model.RemoteAuthority{}
	}
	return model.RemoteAuthority{
		Type:   strVal(a.Type),
		OpenID: strVal(a.OpenId),
		AppID:  strVal(a.AppId),
	}
}

func strVal(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
