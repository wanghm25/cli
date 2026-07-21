// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"context"
	"errors"
	"testing"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/core"
)

// fakeSubscriptionService is a network-free stand-in for the candidate SDK's
// client.Event.V1.Subscription. It implements subscriptionService so tests
// can construct a SubscriptionClient around it and assert exactly which
// larkcore.RequestOptionFunc values were passed on each call, without a real
// network call ever happening.
type fakeSubscriptionService struct {
	gotOpts []larkcore.RequestOptionFunc // captured from the most recent call

	createResp *larkeventv1.CreateSubscriptionResp
	createErr  error

	getResp *larkeventv1.GetSubscriptionResp
	getErr  error

	listResp *larkeventv1.ListSubscriptionResp
	listErr  error

	patchResp *larkeventv1.PatchSubscriptionResp
	patchErr  error

	renewResp *larkeventv1.RenewSubscriptionResp
	renewErr  error

	reactivateResp *larkeventv1.ReactivateSubscriptionResp
	reactivateErr  error

	deleteResp *larkeventv1.DeleteSubscriptionResp
	deleteErr  error
}

func (f *fakeSubscriptionService) Create(_ context.Context, _ *larkeventv1.CreateSubscriptionReq, options ...larkcore.RequestOptionFunc) (*larkeventv1.CreateSubscriptionResp, error) {
	f.gotOpts = options
	return f.createResp, f.createErr
}

func (f *fakeSubscriptionService) Get(_ context.Context, _ *larkeventv1.GetSubscriptionReq, options ...larkcore.RequestOptionFunc) (*larkeventv1.GetSubscriptionResp, error) {
	f.gotOpts = options
	return f.getResp, f.getErr
}

func (f *fakeSubscriptionService) List(_ context.Context, _ *larkeventv1.ListSubscriptionReq, options ...larkcore.RequestOptionFunc) (*larkeventv1.ListSubscriptionResp, error) {
	f.gotOpts = options
	return f.listResp, f.listErr
}

func (f *fakeSubscriptionService) Patch(_ context.Context, _ *larkeventv1.PatchSubscriptionReq, options ...larkcore.RequestOptionFunc) (*larkeventv1.PatchSubscriptionResp, error) {
	f.gotOpts = options
	return f.patchResp, f.patchErr
}

func (f *fakeSubscriptionService) Renew(_ context.Context, _ *larkeventv1.RenewSubscriptionReq, options ...larkcore.RequestOptionFunc) (*larkeventv1.RenewSubscriptionResp, error) {
	f.gotOpts = options
	return f.renewResp, f.renewErr
}

func (f *fakeSubscriptionService) Reactivate(_ context.Context, _ *larkeventv1.ReactivateSubscriptionReq, options ...larkcore.RequestOptionFunc) (*larkeventv1.ReactivateSubscriptionResp, error) {
	f.gotOpts = options
	return f.reactivateResp, f.reactivateErr
}

func (f *fakeSubscriptionService) Delete(_ context.Context, _ *larkeventv1.DeleteSubscriptionReq, options ...larkcore.RequestOptionFunc) (*larkeventv1.DeleteSubscriptionResp, error) {
	f.gotOpts = options
	return f.deleteResp, f.deleteErr
}

// appliedOptions replays opts against a zero-value larkcore.RequestOption so
// tests can assert on the resulting token fields directly instead of
// comparing func values (which Go cannot compare for equality).
func appliedOptions(opts []larkcore.RequestOptionFunc) larkcore.RequestOption {
	var ro larkcore.RequestOption
	for _, o := range opts {
		o(&ro)
	}
	return ro
}

func TestNewSubscriptionClient_UserIdentity_AppendsUserAccessTokenOnly(t *testing.T) {
	svc := &fakeSubscriptionService{getResp: &larkeventv1.GetSubscriptionResp{
		ApiResp: &larkcore.ApiResp{RawBody: []byte(`{"code":0}`)},
	}}
	sc, err := newSubscriptionClient(svc, core.AsUser, "u-secret-token")
	if err != nil {
		t.Fatalf("newSubscriptionClient: unexpected error: %v", err)
	}

	if _, err := sc.Get(context.Background(), larkeventv1.NewGetSubscriptionReqBuilder().SubscriptionId("sub_1").Build()); err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}

	if len(svc.gotOpts) != 1 {
		t.Fatalf("gotOpts = %d options, want exactly 1 (WithUserAccessToken)", len(svc.gotOpts))
	}
	applied := appliedOptions(svc.gotOpts)
	if applied.UserAccessToken != "u-secret-token" {
		t.Errorf("UserAccessToken = %q, want %q", applied.UserAccessToken, "u-secret-token")
	}
	if applied.TenantAccessToken != "" {
		t.Errorf("TenantAccessToken = %q, want empty: a user-identity call must not also carry a tenant token", applied.TenantAccessToken)
	}
}

func TestNewSubscriptionClient_BotIdentity_AppendsNoTokenOption(t *testing.T) {
	svc := &fakeSubscriptionService{getResp: &larkeventv1.GetSubscriptionResp{
		ApiResp: &larkcore.ApiResp{RawBody: []byte(`{"code":0}`)},
	}}
	sc, err := newSubscriptionClient(svc, core.AsBot, "")
	if err != nil {
		t.Fatalf("newSubscriptionClient: unexpected error: %v", err)
	}

	if _, err := sc.Get(context.Background(), larkeventv1.NewGetSubscriptionReqBuilder().SubscriptionId("sub_1").Build()); err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}

	if len(svc.gotOpts) != 0 {
		t.Fatalf("gotOpts = %d options, want exactly 0: bot/app identity must not carry a per-call token option (the SDK auto-mints its own tenant token)", len(svc.gotOpts))
	}
}

func TestNewSubscriptionClient_BotIdentity_IgnoresStrayUAT(t *testing.T) {
	// Even if a caller mistakenly threads a leftover UAT through for a bot
	// call, the bot branch must still send zero token options: identity
	// selection is driven entirely by `as`, never by whether uat is set.
	svc := &fakeSubscriptionService{getResp: &larkeventv1.GetSubscriptionResp{
		ApiResp: &larkcore.ApiResp{RawBody: []byte(`{"code":0}`)},
	}}
	sc, err := newSubscriptionClient(svc, core.AsBot, "leftover-uat-should-be-ignored")
	if err != nil {
		t.Fatalf("newSubscriptionClient: unexpected error: %v", err)
	}

	if _, err := sc.Get(context.Background(), larkeventv1.NewGetSubscriptionReqBuilder().SubscriptionId("sub_1").Build()); err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}

	if len(svc.gotOpts) != 0 {
		t.Fatalf("gotOpts = %d options, want 0: a stray uat must never leak onto a bot-identity call", len(svc.gotOpts))
	}
}

func TestNewSubscriptionClient_UserIdentity_MissingUAT_FailsClosed(t *testing.T) {
	_, err := newSubscriptionClient(&fakeSubscriptionService{}, core.AsUser, "")
	if err == nil {
		t.Fatal("expected an error constructing a user-identity client with no UAT, got nil")
	}
	var authErr *errs.AuthenticationError
	if !errors.As(err, &authErr) {
		t.Fatalf("expected *errs.AuthenticationError, got %T: %v", err, err)
	}
	if authErr.Subtype != errs.SubtypeTokenMissing {
		t.Errorf("Subtype = %s, want %s", authErr.Subtype, errs.SubtypeTokenMissing)
	}
}

func TestNewSubscriptionClient_UnresolvedIdentity_FailsClosedRatherThanGuessing(t *testing.T) {
	// core.AsAuto must never reach this adapter — callers resolve --as via
	// f.ResolveAs first. Construction must reject it instead of silently
	// treating it as bot (or any other default).
	_, err := newSubscriptionClient(&fakeSubscriptionService{}, core.AsAuto, "")
	if err == nil {
		t.Fatal("expected an error constructing a client with an unresolved (auto) identity, got nil")
	}
	if _, ok := errs.ProblemOf(err); !ok {
		t.Fatalf("expected a typed errs.* error, got %T: %v", err, err)
	}
}

func TestSubscriptionClient_Create_TransportErrorBecomesTypedError(t *testing.T) {
	svc := &fakeSubscriptionService{createErr: errors.New("boom: connection reset")}
	sc, err := newSubscriptionClient(svc, core.AsBot, "")
	if err != nil {
		t.Fatalf("newSubscriptionClient: unexpected error: %v", err)
	}

	_, err = sc.Create(context.Background(), larkeventv1.NewCreateSubscriptionReqBuilder().Build())
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if _, ok := errs.ProblemOf(err); !ok {
		t.Fatalf("expected a typed errs.* error (network/internal), got %T: %v", err, err)
	}
}

func TestSubscriptionClient_Create_BusinessFailureClassifiedViaErrclass(t *testing.T) {
	resp := &larkeventv1.CreateSubscriptionResp{
		ApiResp: &larkcore.ApiResp{RawBody: []byte(`{"code":600901,"msg":"synthetic failure for classification test"}`)},
	}
	resp.Code = 600901
	resp.Msg = "synthetic failure for classification test"
	svc := &fakeSubscriptionService{createResp: resp}
	sc, err := newSubscriptionClient(svc, core.AsBot, "")
	if err != nil {
		t.Fatalf("newSubscriptionClient: unexpected error: %v", err)
	}

	_, err = sc.Create(context.Background(), larkeventv1.NewCreateSubscriptionReqBuilder().Build())
	if err == nil {
		t.Fatal("expected an error for a non-zero response code, got nil")
	}
	problem, ok := errs.ProblemOf(err)
	if !ok {
		t.Fatalf("expected a typed errs.* error (via errclass.BuildAPIError), got %T: %v", err, err)
	}
	if problem.Code != 600901 {
		t.Errorf("Code = %d, want 600901", problem.Code)
	}
	if problem.Message != "synthetic failure for classification test" {
		t.Errorf("Message = %q, want %q", problem.Message, "synthetic failure for classification test")
	}
}

func TestSubscriptionClient_Create_SuccessReturnsNoError(t *testing.T) {
	resp := &larkeventv1.CreateSubscriptionResp{
		ApiResp: &larkcore.ApiResp{RawBody: []byte(`{"code":0,"msg":"success"}`)},
	}
	svc := &fakeSubscriptionService{createResp: resp}
	sc, err := newSubscriptionClient(svc, core.AsBot, "")
	if err != nil {
		t.Fatalf("newSubscriptionClient: unexpected error: %v", err)
	}

	got, err := sc.Create(context.Background(), larkeventv1.NewCreateSubscriptionReqBuilder().Build())
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}
	if got != resp {
		t.Error("Create returned a different response pointer than the fake produced")
	}
}

func TestNewSubscriptionClient_WiresRealLarkClient(t *testing.T) {
	// Smoke test: NewSubscriptionClient must correctly navigate
	// sdk.Event.V1.Subscription on a real *lark.Client without making any
	// network call (construction alone never dials out).
	sdk := lark.NewClient("cli_fake_app_id", "fake_app_secret")
	sc, err := NewSubscriptionClient(sdk, core.AsBot, "")
	if err != nil {
		t.Fatalf("NewSubscriptionClient: unexpected error: %v", err)
	}
	if sc == nil {
		t.Fatal("NewSubscriptionClient returned a nil client with a nil error")
	}
}

func TestNewSubscriptionClient_NilSDKFailsClosed(t *testing.T) {
	_, err := NewSubscriptionClient(nil, core.AsBot, "")
	if err == nil {
		t.Fatal("expected an error for a nil *lark.Client, got nil")
	}
	if _, ok := errs.ProblemOf(err); !ok {
		t.Fatalf("expected a typed errs.* error, got %T: %v", err, err)
	}
}
