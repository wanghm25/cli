// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package lark

import (
	"context"

	larksdk "github.com/larksuite/oapi-sdk-go/v3"
	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/model"
)

// SubscriptionGateway is the domain-facing surface of the remote Subscription
// management plane. Every method takes/returns domain types only — no
// larkeventv1.* value ever crosses this boundary — so callers never build a
// request, unwrap a response, or classify an error themselves.
type SubscriptionGateway interface {
	// List returns one page of Subscriptions matching params, with the raw
	// pagination signals for user-driven paging.
	List(ctx context.Context, params ListParams) (*SubscriptionPage, error)
	// WalkSubscriptions pages through Subscriptions matching params (bounded by
	// event.MaxSubscriptionListPages), invoking visit for each projected item
	// until visit returns false or the pages are exhausted. capped is true when
	// the page cap was reached with visit still returning true — NOT a
	// confirmed "no more results", only "none more within the pages read".
	WalkSubscriptions(ctx context.Context, params ListParams, visit func(model.RemoteSubscription) bool) (capped bool, err error)
	// Get returns one Subscription by remote_subscription_id.
	Get(ctx context.Context, remoteSubscriptionID string) (*model.RemoteSubscription, error)
	// Create creates a Subscription from spec.
	Create(ctx context.Context, spec CreateSpec) (*model.RemoteSubscription, error)
	// Patch applies spec (currently the filter) to a Subscription.
	Patch(ctx context.Context, remoteSubscriptionID string, spec PatchSpec) (*model.RemoteSubscription, error)
	// Renew extends a Subscription's TTL and returns its refreshed state.
	Renew(ctx context.Context, remoteSubscriptionID string) (*model.RemoteSubscription, error)
	// Reactivate resumes delivery on a suspended Subscription and returns its
	// refreshed state.
	Reactivate(ctx context.Context, remoteSubscriptionID string) (*model.RemoteSubscription, error)
	// Delete removes a Subscription. It carries no subscription payload, so it
	// returns only an error.
	Delete(ctx context.Context, remoteSubscriptionID string) error
	// GetEncryptKey returns a Subscription's encrypt_key. Callers must never log
	// the returned key.
	GetEncryptKey(ctx context.Context, remoteSubscriptionID string) (string, error)
}

// subscriptionClient is the identity-bound, already-classifying SDK client the
// gateway calls. *SubscriptionClient (client.go, this package) satisfies it
// structurally: it binds one resolved identity's access-token option for its
// whole lifetime and turns transport/business failures into typed errs.* errors,
// so the gateway itself only builds requests, unwraps + validates responses, and
// projects to domain types. Declared as an interface purely as the gateway's
// test seam — a fake substitutes for it with no *larksdk.Client or network call.
type subscriptionClient interface {
	Create(ctx context.Context, req *larkeventv1.CreateSubscriptionReq) (*larkeventv1.CreateSubscriptionResp, error)
	Get(ctx context.Context, req *larkeventv1.GetSubscriptionReq) (*larkeventv1.GetSubscriptionResp, error)
	GetEncryptKey(ctx context.Context, req *larkeventv1.GetEncryptKeySubscriptionReq) (*larkeventv1.GetEncryptKeySubscriptionResp, error)
	List(ctx context.Context, req *larkeventv1.ListSubscriptionReq) (*larkeventv1.ListSubscriptionResp, error)
	Patch(ctx context.Context, req *larkeventv1.PatchSubscriptionReq) (*larkeventv1.PatchSubscriptionResp, error)
	Renew(ctx context.Context, req *larkeventv1.RenewSubscriptionReq) (*larkeventv1.RenewSubscriptionResp, error)
	Reactivate(ctx context.Context, req *larkeventv1.ReactivateSubscriptionReq) (*larkeventv1.ReactivateSubscriptionResp, error)
	Delete(ctx context.Context, req *larkeventv1.DeleteSubscriptionReq) (*larkeventv1.DeleteSubscriptionResp, error)
}

// Gateway is the concrete SubscriptionGateway, bound to one resolved identity
// via the wrapped subscriptionClient.
type Gateway struct {
	client subscriptionClient
}

// compile-time assertions: the concrete gateway satisfies its interface, and
// the identity-bound client satisfies the wrapped seam.
var (
	_ SubscriptionGateway = (*Gateway)(nil)
	_ subscriptionClient  = (*SubscriptionClient)(nil)
)

// NewSubscriptionGateway builds a Gateway bound to sdk (already configured with
// the resolved account's appID/secret) and to the given already-resolved
// identity. as must be core.AsUser or core.AsBot; uat is the user access token
// sent on every call for core.AsUser (ignored for core.AsBot). It delegates the
// identity binding + fail-closed construction to NewSubscriptionClient
// (client.go, this package).
func NewSubscriptionGateway(sdk *larksdk.Client, as core.Identity, uat string) (*Gateway, error) {
	client, err := NewSubscriptionClient(sdk, as, uat)
	if err != nil {
		return nil, err
	}
	return &Gateway{client: client}, nil
}

// newGateway wraps an already-built subscriptionClient. It is the seam the
// gateway's own tests use to inject a fake; NewSubscriptionGateway is the
// production entry point.
func newGateway(client subscriptionClient) *Gateway {
	return &Gateway{client: client}
}

// List issues one List page.
func (g *Gateway) List(ctx context.Context, params ListParams) (*SubscriptionPage, error) {
	resp, err := g.client.List(ctx, buildListReq(params))
	if err != nil {
		return nil, err
	}
	page := &SubscriptionPage{Items: []model.RemoteSubscription{}}
	if resp == nil || resp.Data == nil {
		return page, nil
	}
	for _, item := range resp.Data.Items {
		page.Items = append(page.Items, ProjectSubscription(item))
	}
	if resp.Data.HasMore != nil {
		page.HasMore = *resp.Data.HasMore
	}
	if resp.Data.PageToken != nil {
		page.NextPageToken = *resp.Data.PageToken
	}
	return page, nil
}

// WalkSubscriptions pages via event.WalkSubscriptionPages so it shares the one
// bounded, ctx-aware pager (and its exact cap/early-stop/capped semantics) every
// other List-scan caller in the event subsystem uses, projecting each SDK item
// to the domain type before handing it to visit.
func (g *Gateway) WalkSubscriptions(ctx context.Context, params ListParams, visit func(model.RemoteSubscription) bool) (bool, error) {
	buildReq := func(pageToken string) *larkeventv1.ListSubscriptionReq {
		p := params
		p.PageToken = pageToken
		return buildListReq(p)
	}
	return event.WalkSubscriptionPages(ctx, g.client, buildReq, func(item *larkeventv1.SubscriptionDetail) bool {
		return visit(ProjectSubscription(item))
	})
}

// Get issues a Get and validates its payload.
func (g *Gateway) Get(ctx context.Context, remoteSubscriptionID string) (*model.RemoteSubscription, error) {
	resp, err := g.client.Get(ctx, larkeventv1.NewGetSubscriptionReqBuilder().SubscriptionId(remoteSubscriptionID).Build())
	if err != nil {
		return nil, err
	}
	return g.requireSubscription(subscriptionOf(resp), "get")
}

// Create issues a Create from spec and validates its payload.
func (g *Gateway) Create(ctx context.Context, spec CreateSpec) (*model.RemoteSubscription, error) {
	req := larkeventv1.NewCreateSubscriptionReqBuilder().Body(buildCreateBody(spec)).Build()
	resp, err := g.client.Create(ctx, req)
	if err != nil {
		return nil, err
	}
	return g.requireSubscription(createSubscriptionOf(resp), "create")
}

// Patch issues a Patch and validates its payload.
func (g *Gateway) Patch(ctx context.Context, remoteSubscriptionID string, spec PatchSpec) (*model.RemoteSubscription, error) {
	req := larkeventv1.NewPatchSubscriptionReqBuilder().
		SubscriptionId(remoteSubscriptionID).
		Body(buildPatchBody(spec)).
		Build()
	resp, err := g.client.Patch(ctx, req)
	if err != nil {
		return nil, err
	}
	return g.requireSubscription(patchSubscriptionOf(resp), "update")
}

// buildPatchBody projects a PatchSpec into the Patch request body, which carries
// only the filter (the sole field a patch changes). FilterToSDK projects the
// desired filter; an empty/cleared filter becomes the {"filter":{}} clear form
// (a non-nil empty SDK filter) rather than an omitted field, so a clear is
// distinguishable on the wire from "leave the filter unchanged". Split out so
// the projection is directly assertable against a plain, fully-inspectable
// *larkeventv1.PatchSubscriptionReqBody (the built *PatchSubscriptionReq stores
// its body in an internal field the SDK transport reads, not readably back).
func buildPatchBody(spec PatchSpec) *larkeventv1.PatchSubscriptionReqBody {
	return larkeventv1.NewPatchSubscriptionReqBodyBuilder().
		Filter(FilterToSDK(spec.Filter)).
		Build()
}

// Renew issues a Renew and validates its payload.
func (g *Gateway) Renew(ctx context.Context, remoteSubscriptionID string) (*model.RemoteSubscription, error) {
	resp, err := g.client.Renew(ctx, larkeventv1.NewRenewSubscriptionReqBuilder().SubscriptionId(remoteSubscriptionID).Build())
	if err != nil {
		return nil, err
	}
	return g.requireSubscription(renewSubscriptionOf(resp), "renew")
}

// Reactivate issues a Reactivate and validates its payload.
func (g *Gateway) Reactivate(ctx context.Context, remoteSubscriptionID string) (*model.RemoteSubscription, error) {
	resp, err := g.client.Reactivate(ctx, larkeventv1.NewReactivateSubscriptionReqBuilder().SubscriptionId(remoteSubscriptionID).Build())
	if err != nil {
		return nil, err
	}
	return g.requireSubscription(reactivateSubscriptionOf(resp), "reactivate")
}

// Delete issues a Delete. DeleteSubscriptionResp carries no subscription
// payload, so there is nothing to unwrap on success.
func (g *Gateway) Delete(ctx context.Context, remoteSubscriptionID string) error {
	_, err := g.client.Delete(ctx, larkeventv1.NewDeleteSubscriptionReqBuilder().SubscriptionId(remoteSubscriptionID).Build())
	return err
}

// GetEncryptKey issues a GetEncryptKey and returns the encrypt_key. A success
// response carrying no key is an InvalidResponse rather than a silent empty
// string.
func (g *Gateway) GetEncryptKey(ctx context.Context, remoteSubscriptionID string) (string, error) {
	resp, err := g.client.GetEncryptKey(ctx, larkeventv1.NewGetEncryptKeySubscriptionReqBuilder().SubscriptionId(remoteSubscriptionID).Build())
	if err != nil {
		return "", err
	}
	if resp == nil || resp.Data == nil || resp.Data.EncryptKey == nil || *resp.Data.EncryptKey == "" {
		return "", errs.NewInternalError(errs.SubtypeInvalidResponse,
			"subscription get_encrypt_key reported success for %s but returned no encrypt_key", remoteSubscriptionID)
	}
	return *resp.Data.EncryptKey, nil
}

// requireSubscription is the single required-field validation site: a
// syntactically successful response that carries no subscription, or one whose
// remote_subscription_id is empty, is a wire anomaly, not real data — it becomes
// a typed InvalidResponse rather than a silently empty projection.
func (g *Gateway) requireSubscription(detail *larkeventv1.SubscriptionDetail, op string) (*model.RemoteSubscription, error) {
	if detail == nil {
		return nil, errs.NewInternalError(errs.SubtypeInvalidResponse,
			"subscription %s reported success but returned no subscription data", op)
	}
	sub := ProjectSubscription(detail)
	if sub.ID.IsZero() {
		return nil, errs.NewInternalError(errs.SubtypeInvalidResponse,
			"subscription %s reported success but returned an empty remote_subscription_id", op)
	}
	return &sub, nil
}

// buildListReq builds the List request from params, adding only the filters the
// caller actually set.
func buildListReq(params ListParams) *larkeventv1.ListSubscriptionReq {
	b := larkeventv1.NewListSubscriptionReqBuilder()
	if params.State != "" {
		b = b.State(params.State)
	}
	if params.EventType != "" {
		b = b.EventType(params.EventType)
	}
	if params.TargetResource != "" {
		b = b.TargetResource(params.TargetResource)
	}
	if params.PageToken != "" {
		b = b.PageToken(params.PageToken)
	}
	if params.PageSize > 0 {
		b = b.PageSize(params.PageSize)
	}
	return b.Build()
}

// buildCreateBody projects a CreateSpec into the Create request body, setting
// include_resource_data and (when present) the encrypt_key on the SAME
// payload_options so the two are always submitted atomically, and sending the
// filter only when one was requested.
func buildCreateBody(spec CreateSpec) *larkeventv1.CreateSubscriptionReqBody {
	payloadOptions := larkeventv1.NewCreatePayloadOptionsBuilder().IncludeResourceData(spec.IncludeResourceData)
	if spec.EncryptKey != "" {
		payloadOptions = payloadOptions.Encrypt(larkeventv1.NewPayloadOptionsEncryptBuilder().EncryptKey(spec.EncryptKey).Build())
	}
	b := larkeventv1.NewCreateSubscriptionReqBodyBuilder().
		EventType(spec.EventType).
		TargetResource(spec.TargetResource).
		PayloadOptions(payloadOptions.Build())
	if !spec.Filter.IsEmpty() {
		b = b.Filter(FilterToSDK(spec.Filter))
	}
	return b.Build()
}

// The *SubscriptionOf helpers extract the (possibly nil) SubscriptionDetail from
// each response type's Data, tolerating a nil resp / nil Data so
// requireSubscription can produce one uniform InvalidResponse.
func subscriptionOf(resp *larkeventv1.GetSubscriptionResp) *larkeventv1.SubscriptionDetail {
	if resp == nil || resp.Data == nil {
		return nil
	}
	return resp.Data.Subscription
}

func createSubscriptionOf(resp *larkeventv1.CreateSubscriptionResp) *larkeventv1.SubscriptionDetail {
	if resp == nil || resp.Data == nil {
		return nil
	}
	return resp.Data.Subscription
}

func patchSubscriptionOf(resp *larkeventv1.PatchSubscriptionResp) *larkeventv1.SubscriptionDetail {
	if resp == nil || resp.Data == nil {
		return nil
	}
	return resp.Data.Subscription
}

func renewSubscriptionOf(resp *larkeventv1.RenewSubscriptionResp) *larkeventv1.SubscriptionDetail {
	if resp == nil || resp.Data == nil {
		return nil
	}
	return resp.Data.Subscription
}

func reactivateSubscriptionOf(resp *larkeventv1.ReactivateSubscriptionResp) *larkeventv1.SubscriptionDetail {
	if resp == nil || resp.Data == nil {
		return nil
	}
	return resp.Data.Subscription
}
