// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"context"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"

	// larkeventv1 alias is REQUIRED: this package's declared name is
	// larkevent, which collides with the core oapi-sdk-go/v3/event package
	// (also larkevent) — spec §0.4.
	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/client"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/errclass"
)

// subscriptionService is the subset of the candidate SDK's typed
// client.Event.V1.Subscription surface that Phase-B `event subscription`
// commands need (Create/Get/List/Patch/Renew/Reactivate/Delete — GetEncryptKey
// and ListByIterator are out of scope for this phase, see spec §0.2/§9).
//
// It exists purely as a test seam: the concrete *lark.Client's
// Event.V1.Subscription value satisfies this interface structurally (Go
// interfaces are duck-typed), so production code passes it straight through,
// while tests substitute a fake and capture exactly which
// larkcore.RequestOptionFunc values SubscriptionClient calls it with —
// without ever making a real network call.
type subscriptionService interface {
	Create(ctx context.Context, req *larkeventv1.CreateSubscriptionReq, options ...larkcore.RequestOptionFunc) (*larkeventv1.CreateSubscriptionResp, error)
	Get(ctx context.Context, req *larkeventv1.GetSubscriptionReq, options ...larkcore.RequestOptionFunc) (*larkeventv1.GetSubscriptionResp, error)
	List(ctx context.Context, req *larkeventv1.ListSubscriptionReq, options ...larkcore.RequestOptionFunc) (*larkeventv1.ListSubscriptionResp, error)
	Patch(ctx context.Context, req *larkeventv1.PatchSubscriptionReq, options ...larkcore.RequestOptionFunc) (*larkeventv1.PatchSubscriptionResp, error)
	Renew(ctx context.Context, req *larkeventv1.RenewSubscriptionReq, options ...larkcore.RequestOptionFunc) (*larkeventv1.RenewSubscriptionResp, error)
	Reactivate(ctx context.Context, req *larkeventv1.ReactivateSubscriptionReq, options ...larkcore.RequestOptionFunc) (*larkeventv1.ReactivateSubscriptionResp, error)
	Delete(ctx context.Context, req *larkeventv1.DeleteSubscriptionReq, options ...larkcore.RequestOptionFunc) (*larkeventv1.DeleteSubscriptionResp, error)
}

// SubscriptionClient is a thin CLI <-> lark.Client adapter over the candidate
// SDK's typed Subscription service (client.Event.V1.Subscription — this is
// the first typed-SDK-service-client usage in this CLI; everywhere else goes
// through APIClient.DoSDKRequest's raw transport path). It binds one
// already-resolved CLI identity to the correct per-call access-token option
// for its whole lifetime, so Phase-B `event subscription` command code calls
// Create/Get/List/Patch/Renew/Reactivate/Delete without ever handling
// larkcore.RequestOptionFunc or identity branching itself.
//
// Per spec §0.4/§3.2: a user identity carries a larkcore.WithUserAccessToken
// option on every call; an app/bot identity carries none at all — the SDK
// mints and caches its own tenant access token from the appID/secret already
// configured on the *lark.Client. This mirrors how the fork's own e2e harness
// (scene/eventsub + internal/e2e/clients.go) drives the same service.
//
// This client never falls back from one identity to the other: construction
// fails closed (typed error) instead of guessing, matching the "no silent
// identity switch" rule that governs the rest of this feature (spec §2.8).
type SubscriptionClient struct {
	svc      subscriptionService
	identity core.Identity
	opts     []larkcore.RequestOptionFunc
}

// NewSubscriptionClient builds a SubscriptionClient bound to sdk (typically
// obtained via the CLI factory's f.LarkClient(), already configured with the
// resolved account's appID/secret) and to the given already-resolved
// identity.
//
// as must be core.AsUser or core.AsBot — callers resolve --as via
// f.ResolveAs (or equivalent) before reaching this adapter; construction
// rejects any other value rather than guessing an identity.
//
// uat is the user access token to send on every call when as == core.AsUser
// (obtained by the caller, e.g. via credential.CredentialProvider.ResolveToken
// with credential.NewTokenSpec(core.AsUser, appID)); it must be non-empty in
// that case. It is ignored for core.AsBot: the SDK mints its own tenant
// access token, so no per-call token option is added. Callers must not log
// uat.
func NewSubscriptionClient(sdk *lark.Client, as core.Identity, uat string) (*SubscriptionClient, error) {
	if sdk == nil {
		return nil, errs.NewInternalError(errs.SubtypeSDKError, "subscription client: sdk is nil")
	}
	return newSubscriptionClient(sdk.Event.V1.Subscription, as, uat)
}

// newSubscriptionClient is the subscriptionService-level constructor tests
// use to substitute a fake and assert option selection without a real
// network call. NewSubscriptionClient is the production entry point.
func newSubscriptionClient(svc subscriptionService, as core.Identity, uat string) (*SubscriptionClient, error) {
	opts, err := identityOptions(as, uat)
	if err != nil {
		return nil, err
	}
	return &SubscriptionClient{svc: svc, identity: as, opts: opts}, nil
}

// identityOptions returns the per-call larkcore.RequestOptionFunc list for an
// already-resolved identity: exactly one WithUserAccessToken(uat) for
// core.AsUser, none for core.AsBot (spec §0.4). Any other identity value
// (notably core.AsAuto — "--as auto" must already have been resolved to a
// concrete identity before reaching this adapter) is rejected rather than
// defaulted.
func identityOptions(as core.Identity, uat string) ([]larkcore.RequestOptionFunc, error) {
	switch as {
	case core.AsUser:
		if uat == "" {
			return nil, errs.NewAuthenticationError(errs.SubtypeTokenMissing,
				"no user access token available to call the subscription API as user").
				WithHint("run: lark-cli auth login to re-authorize")
		}
		return []larkcore.RequestOptionFunc{larkcore.WithUserAccessToken(uat)}, nil
	case core.AsBot:
		// No per-call token option: the SDK mints/caches its own tenant
		// access token from the appID/secret already configured on the
		// *lark.Client (spec §0.4). uat is intentionally ignored here so a
		// stray/leftover value never leaks onto a bot-identity call.
		return nil, nil
	default:
		return nil, errs.NewInternalError(errs.SubtypeUnknown,
			"subscription client requires an already-resolved identity (user or bot), got %q", as)
	}
}

// Create wraps client.Event.V1.Subscription.Create with the bound identity option.
func (c *SubscriptionClient) Create(ctx context.Context, req *larkeventv1.CreateSubscriptionReq) (*larkeventv1.CreateSubscriptionResp, error) {
	resp, err := c.svc.Create(ctx, req, c.opts...)
	if err != nil {
		return nil, client.WrapDoAPIError(err)
	}
	if !resp.Success() {
		return resp, c.classifyFailure(resp.ApiResp)
	}
	return resp, nil
}

// Get wraps client.Event.V1.Subscription.Get with the bound identity option.
func (c *SubscriptionClient) Get(ctx context.Context, req *larkeventv1.GetSubscriptionReq) (*larkeventv1.GetSubscriptionResp, error) {
	resp, err := c.svc.Get(ctx, req, c.opts...)
	if err != nil {
		return nil, client.WrapDoAPIError(err)
	}
	if !resp.Success() {
		return resp, c.classifyFailure(resp.ApiResp)
	}
	return resp, nil
}

// List wraps client.Event.V1.Subscription.List with the bound identity option.
func (c *SubscriptionClient) List(ctx context.Context, req *larkeventv1.ListSubscriptionReq) (*larkeventv1.ListSubscriptionResp, error) {
	resp, err := c.svc.List(ctx, req, c.opts...)
	if err != nil {
		return nil, client.WrapDoAPIError(err)
	}
	if !resp.Success() {
		return resp, c.classifyFailure(resp.ApiResp)
	}
	return resp, nil
}

// Patch wraps client.Event.V1.Subscription.Patch with the bound identity
// option. Patch (not Update — the SDK method is spelled Patch) is used for
// `event subscription update`.
func (c *SubscriptionClient) Patch(ctx context.Context, req *larkeventv1.PatchSubscriptionReq) (*larkeventv1.PatchSubscriptionResp, error) {
	resp, err := c.svc.Patch(ctx, req, c.opts...)
	if err != nil {
		return nil, client.WrapDoAPIError(err)
	}
	if !resp.Success() {
		return resp, c.classifyFailure(resp.ApiResp)
	}
	return resp, nil
}

// Renew wraps client.Event.V1.Subscription.Renew with the bound identity option.
func (c *SubscriptionClient) Renew(ctx context.Context, req *larkeventv1.RenewSubscriptionReq) (*larkeventv1.RenewSubscriptionResp, error) {
	resp, err := c.svc.Renew(ctx, req, c.opts...)
	if err != nil {
		return nil, client.WrapDoAPIError(err)
	}
	if !resp.Success() {
		return resp, c.classifyFailure(resp.ApiResp)
	}
	return resp, nil
}

// Reactivate wraps client.Event.V1.Subscription.Reactivate with the bound identity option.
func (c *SubscriptionClient) Reactivate(ctx context.Context, req *larkeventv1.ReactivateSubscriptionReq) (*larkeventv1.ReactivateSubscriptionResp, error) {
	resp, err := c.svc.Reactivate(ctx, req, c.opts...)
	if err != nil {
		return nil, client.WrapDoAPIError(err)
	}
	if !resp.Success() {
		return resp, c.classifyFailure(resp.ApiResp)
	}
	return resp, nil
}

// Delete wraps client.Event.V1.Subscription.Delete with the bound identity option.
func (c *SubscriptionClient) Delete(ctx context.Context, req *larkeventv1.DeleteSubscriptionReq) (*larkeventv1.DeleteSubscriptionResp, error) {
	resp, err := c.svc.Delete(ctx, req, c.opts...)
	if err != nil {
		return nil, client.WrapDoAPIError(err)
	}
	if !resp.Success() {
		return resp, c.classifyFailure(resp.ApiResp)
	}
	return resp, nil
}

// classifyFailure maps a syntactically successful but business-failed
// Subscription response (resp.Code != 0, i.e. !resp.Success()) to a typed
// errs.* error via the same errclass.BuildAPIError classification every
// other Lark API call in this CLI uses (see errs/ERROR_CONTRACT.md) —
// Subscription never gets bespoke error codes or a hand-built envelope.
//
// raw is the response's embedded *larkcore.ApiResp. Its RawBody is
// guaranteed to already be valid JSON at this point: the SDK only produces a
// non-nil resp (letting us reach here) after apiResp.JSONUnmarshalBody
// successfully decoded RawBody into the typed response struct that carries
// this same ApiResp. Decoding it again as a generic map (rather than
// reconstructing one from the already-typed Code/Msg/Err fields) preserves
// every server-supplied detail — troubleshooter URL, permission_violations,
// log_id — exactly as errclass.BuildAPIError expects, with no hand-built
// substitute shape.
func (c *SubscriptionClient) classifyFailure(raw *larkcore.ApiResp) error {
	body, err := client.ParseJSONResponse(raw)
	if err != nil {
		return client.WrapJSONResponseParseError(err, raw.RawBody)
	}
	bodyMap, ok := body.(map[string]interface{})
	if !ok {
		return errs.NewInternalError(errs.SubtypeInvalidResponse, "subscription API returned a non-object JSON response")
	}
	cc := errclass.ClassifyContext{Identity: string(c.identity)}
	if apiErr := errclass.BuildAPIError(bodyMap, cc); apiErr != nil {
		return apiErr
	}
	// Defensive: BuildAPIError returns nil only for code==0, which
	// !resp.Success() (Code != 0) already rules out. Never silently report
	// success from this branch.
	return errs.NewInternalError(errs.SubtypeUnknown, "subscription API reported a non-success response without a classifiable error code")
}
