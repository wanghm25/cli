// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package lark

import (
	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/internal/event/model"
)

// The domain projection of a remote Lark Subscription is model.RemoteSubscription
// (a pure, SDK-free value object). This adapter owns the projection FROM the SDK
// SubscriptionDetail into that domain type — see ProjectSubscription — but the
// type itself, the Gateway port, and its request specs
// (subscription.CreateSpec/PatchSpec/ListParams/SubscriptionPage) all live in
// the domain, so subscription / app / status / lifecycle never speak a
// platform-package type and the dependency runs inward, adapter -> domain.

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
