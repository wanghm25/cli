// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package lark

import (
	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/internal/event/model"
)

// The domain projection of a remote Lark Subscription is model.RemoteSubscription
// (a pure, SDK-free value object). This gateway owns the projection FROM the SDK
// SubscriptionDetail into that domain type — see ProjectSubscription — but the
// type itself lives in the domain so subscription / app / status / lifecycle
// never speak a platform-package type.

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
	Filter *model.Filter
}

// PatchSpec is the domain request for patching a remote Subscription. Patch
// carries only the filter (the sole field update changes); an empty/cleared
// filter is sent as the {"filter":{}} clear form rather than an omitted field.
type PatchSpec struct {
	Filter *model.Filter
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
	Items         []model.RemoteSubscription
	HasMore       bool
	NextPageToken string
}

// ProjectSubscription faithfully projects an SDK SubscriptionDetail into the
// domain model.RemoteSubscription. A nil detail yields the zero value. It
// performs NO required-field validation (an empty id projects to an empty ID) —
// that is the gateway methods' job (requireSubscription), so this projection can
// be reused for List items and for the reconcile/create details create.go still
// holds while it lives on the legacy subscription_client (retired in PR2b).
func ProjectSubscription(d *larkeventv1.SubscriptionDetail) model.RemoteSubscription {
	if d == nil {
		return model.RemoteSubscription{}
	}
	sub := model.RemoteSubscription{
		ID:             model.RemoteSubscriptionID(strVal(d.SubscriptionId)),
		EventType:      strVal(d.EventType),
		TargetResource: strVal(d.TargetResource),
		Authority:      projectAuthority(d.Authority),
		Filter:         FilterFromSDK(d.Filter),
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
