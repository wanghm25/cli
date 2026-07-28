// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"context"

	"github.com/larksuite/cli/internal/event/model"
)

// Gateway is the domain's outbound port to the remote-Subscription management
// plane: the bounded List scan and the point Get read, the three writes the
// Controller performs (Create/Reactivate/Renew), and the encrypt-key probe. The
// domain owns this interface and expresses every parameter in domain types
// (CreateSpec, ListParams, model.RemoteSubscription) — no SDK type ever crosses
// it. The platform/lark adapter implements it inward: its *Gateway translates
// these domain specs into SDK requests and projects responses back to domain
// types, so no domain package ever depends on the adapter.
//
// Get and Renew back the Controller's lifecycle-recovery entry points
// (Controller.Get/Reactivate/Renew): the subscription lifecycle control plane
// drives its state-source-of-truth Get and its Reactivate/Renew writes through
// the Controller, so every remote write funnels through that one owner instead
// of reaching the adapter directly.
type Gateway interface {
	WalkSubscriptions(ctx context.Context, params ListParams, visit func(model.RemoteSubscription) bool) (bool, error)
	Get(ctx context.Context, remoteSubscriptionID string) (*model.RemoteSubscription, error)
	Create(ctx context.Context, spec CreateSpec) (*model.RemoteSubscription, error)
	Reactivate(ctx context.Context, remoteSubscriptionID string) (*model.RemoteSubscription, error)
	Renew(ctx context.Context, remoteSubscriptionID string) (*model.RemoteSubscription, error)
	GetEncryptKey(ctx context.Context, remoteSubscriptionID string) (string, error)
}

// CreateSpec is the domain request for creating a remote Subscription. The
// adapter projects it into an SDK Create request body.
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
