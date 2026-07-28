// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package lark

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/model"
)

// fakeClient is a network-free stand-in for the identity-bound, already-
// classifying *event.SubscriptionClient (the gateway's subscriptionClient
// seam). Each method returns whatever its hook was set to; a nil hook returns a
// benign empty success. Because it replaces the classifying client, a simulated
// business/transport failure is expressed as a non-nil err, never a
// non-success resp.
type fakeClient struct {
	getFunc        func(*larkeventv1.GetSubscriptionReq) (*larkeventv1.GetSubscriptionResp, error)
	createFunc     func(*larkeventv1.CreateSubscriptionReq) (*larkeventv1.CreateSubscriptionResp, error)
	patchFunc      func(*larkeventv1.PatchSubscriptionReq) (*larkeventv1.PatchSubscriptionResp, error)
	renewFunc      func(*larkeventv1.RenewSubscriptionReq) (*larkeventv1.RenewSubscriptionResp, error)
	reactivateFunc func(*larkeventv1.ReactivateSubscriptionReq) (*larkeventv1.ReactivateSubscriptionResp, error)
	deleteFunc     func(*larkeventv1.DeleteSubscriptionReq) (*larkeventv1.DeleteSubscriptionResp, error)
	encryptFunc    func(*larkeventv1.GetEncryptKeySubscriptionReq) (*larkeventv1.GetEncryptKeySubscriptionResp, error)
	listFunc       func(int, *larkeventv1.ListSubscriptionReq) (*larkeventv1.ListSubscriptionResp, error)
	listCalls      int
}

func (f *fakeClient) Get(_ context.Context, req *larkeventv1.GetSubscriptionReq) (*larkeventv1.GetSubscriptionResp, error) {
	if f.getFunc == nil {
		return &larkeventv1.GetSubscriptionResp{}, nil
	}
	return f.getFunc(req)
}

func (f *fakeClient) Create(_ context.Context, req *larkeventv1.CreateSubscriptionReq) (*larkeventv1.CreateSubscriptionResp, error) {
	if f.createFunc == nil {
		return &larkeventv1.CreateSubscriptionResp{}, nil
	}
	return f.createFunc(req)
}

func (f *fakeClient) Patch(_ context.Context, req *larkeventv1.PatchSubscriptionReq) (*larkeventv1.PatchSubscriptionResp, error) {
	if f.patchFunc == nil {
		return &larkeventv1.PatchSubscriptionResp{}, nil
	}
	return f.patchFunc(req)
}

func (f *fakeClient) Renew(_ context.Context, req *larkeventv1.RenewSubscriptionReq) (*larkeventv1.RenewSubscriptionResp, error) {
	if f.renewFunc == nil {
		return &larkeventv1.RenewSubscriptionResp{}, nil
	}
	return f.renewFunc(req)
}

func (f *fakeClient) Reactivate(_ context.Context, req *larkeventv1.ReactivateSubscriptionReq) (*larkeventv1.ReactivateSubscriptionResp, error) {
	if f.reactivateFunc == nil {
		return &larkeventv1.ReactivateSubscriptionResp{}, nil
	}
	return f.reactivateFunc(req)
}

func (f *fakeClient) Delete(_ context.Context, req *larkeventv1.DeleteSubscriptionReq) (*larkeventv1.DeleteSubscriptionResp, error) {
	if f.deleteFunc == nil {
		return &larkeventv1.DeleteSubscriptionResp{}, nil
	}
	return f.deleteFunc(req)
}

func (f *fakeClient) GetEncryptKey(_ context.Context, req *larkeventv1.GetEncryptKeySubscriptionReq) (*larkeventv1.GetEncryptKeySubscriptionResp, error) {
	if f.encryptFunc == nil {
		return &larkeventv1.GetEncryptKeySubscriptionResp{}, nil
	}
	return f.encryptFunc(req)
}

func (f *fakeClient) List(_ context.Context, req *larkeventv1.ListSubscriptionReq) (*larkeventv1.ListSubscriptionResp, error) {
	call := f.listCalls
	f.listCalls++
	if f.listFunc == nil {
		return &larkeventv1.ListSubscriptionResp{}, nil
	}
	return f.listFunc(call, req)
}

func detail(id string) *larkeventv1.SubscriptionDetail {
	return &larkeventv1.SubscriptionDetail{SubscriptionId: strPtr(id), State: strPtr("active")}
}

func assertInvalidResponse(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected a typed InvalidResponse error, got nil")
	}
	p, ok := errs.ProblemOf(err)
	if !ok {
		t.Fatalf("expected a typed errs.* error, got %T: %v", err, err)
	}
	if p.Subtype != errs.SubtypeInvalidResponse {
		t.Errorf("Subtype = %q, want %q", p.Subtype, errs.SubtypeInvalidResponse)
	}
}

// --- must-fix: required-field validation ---

func TestGateway_Get_NilData_ReturnsInvalidResponse(t *testing.T) {
	g := newGateway(&fakeClient{getFunc: func(*larkeventv1.GetSubscriptionReq) (*larkeventv1.GetSubscriptionResp, error) {
		return &larkeventv1.GetSubscriptionResp{}, nil // success, but no Data/Subscription
	}})
	_, err := g.Get(context.Background(), "sub_x")
	assertInvalidResponse(t, err)
}

func TestGateway_Get_EmptyID_ReturnsInvalidResponse(t *testing.T) {
	g := newGateway(&fakeClient{getFunc: func(*larkeventv1.GetSubscriptionReq) (*larkeventv1.GetSubscriptionResp, error) {
		return &larkeventv1.GetSubscriptionResp{Data: &larkeventv1.GetSubscriptionRespData{
			Subscription: &larkeventv1.SubscriptionDetail{State: strPtr("active")}, // no subscription_id
		}}, nil
	}})
	_, err := g.Get(context.Background(), "sub_x")
	assertInvalidResponse(t, err)
}

func TestGateway_Create_NilData_ReturnsInvalidResponse(t *testing.T) {
	g := newGateway(&fakeClient{createFunc: func(*larkeventv1.CreateSubscriptionReq) (*larkeventv1.CreateSubscriptionResp, error) {
		return &larkeventv1.CreateSubscriptionResp{}, nil
	}})
	_, err := g.Create(context.Background(), CreateSpec{EventType: "im.message.created_v1"})
	assertInvalidResponse(t, err)
}

func TestGateway_Create_EmptyID_ReturnsInvalidResponse(t *testing.T) {
	g := newGateway(&fakeClient{createFunc: func(*larkeventv1.CreateSubscriptionReq) (*larkeventv1.CreateSubscriptionResp, error) {
		return &larkeventv1.CreateSubscriptionResp{Data: &larkeventv1.CreateSubscriptionRespData{
			Subscription: &larkeventv1.SubscriptionDetail{State: strPtr("active")},
		}}, nil
	}})
	_, err := g.Create(context.Background(), CreateSpec{EventType: "im.message.created_v1"})
	assertInvalidResponse(t, err)
}

func TestGateway_Renew_NilData_ReturnsInvalidResponse(t *testing.T) {
	g := newGateway(&fakeClient{renewFunc: func(*larkeventv1.RenewSubscriptionReq) (*larkeventv1.RenewSubscriptionResp, error) {
		return &larkeventv1.RenewSubscriptionResp{}, nil
	}})
	_, err := g.Renew(context.Background(), "sub_x")
	assertInvalidResponse(t, err)
}

func TestGateway_Reactivate_NilData_ReturnsInvalidResponse(t *testing.T) {
	g := newGateway(&fakeClient{reactivateFunc: func(*larkeventv1.ReactivateSubscriptionReq) (*larkeventv1.ReactivateSubscriptionResp, error) {
		return &larkeventv1.ReactivateSubscriptionResp{}, nil
	}})
	_, err := g.Reactivate(context.Background(), "sub_x")
	assertInvalidResponse(t, err)
}

func TestGateway_Patch_NilData_ReturnsInvalidResponse(t *testing.T) {
	g := newGateway(&fakeClient{patchFunc: func(*larkeventv1.PatchSubscriptionReq) (*larkeventv1.PatchSubscriptionResp, error) {
		return &larkeventv1.PatchSubscriptionResp{}, nil
	}})
	_, err := g.Patch(context.Background(), "sub_x", PatchSpec{Filter: &event.Filter{}})
	assertInvalidResponse(t, err)
}

// --- happy paths + error passthrough ---

func TestGateway_Get_Success_Projects(t *testing.T) {
	g := newGateway(&fakeClient{getFunc: func(*larkeventv1.GetSubscriptionReq) (*larkeventv1.GetSubscriptionResp, error) {
		return &larkeventv1.GetSubscriptionResp{Data: &larkeventv1.GetSubscriptionRespData{Subscription: detail("sub_1")}}, nil
	}})
	sub, err := g.Get(context.Background(), "sub_1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sub.ID.String() != "sub_1" || sub.State != "active" {
		t.Errorf("got %+v, want {sub_1 active}", sub)
	}
}

func TestGateway_Get_ClientError_PropagatesUnchanged(t *testing.T) {
	sentinel := errors.New("boom: connection reset")
	g := newGateway(&fakeClient{getFunc: func(*larkeventv1.GetSubscriptionReq) (*larkeventv1.GetSubscriptionResp, error) {
		return nil, sentinel
	}})
	_, err := g.Get(context.Background(), "sub_1")
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want it passed through unchanged (%v)", err, sentinel)
	}
}

func TestGateway_Delete_PassesThrough(t *testing.T) {
	called := 0
	g := newGateway(&fakeClient{deleteFunc: func(*larkeventv1.DeleteSubscriptionReq) (*larkeventv1.DeleteSubscriptionResp, error) {
		called++
		return &larkeventv1.DeleteSubscriptionResp{}, nil
	}})
	if err := g.Delete(context.Background(), "sub_1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if called != 1 {
		t.Errorf("Delete called %d times, want 1", called)
	}
}

func TestGateway_GetEncryptKey_Success(t *testing.T) {
	g := newGateway(&fakeClient{encryptFunc: func(*larkeventv1.GetEncryptKeySubscriptionReq) (*larkeventv1.GetEncryptKeySubscriptionResp, error) {
		return &larkeventv1.GetEncryptKeySubscriptionResp{Data: &larkeventv1.GetEncryptKeySubscriptionRespData{EncryptKey: strPtr("the-key")}}, nil
	}})
	key, err := g.GetEncryptKey(context.Background(), "sub_1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key != "the-key" {
		t.Errorf("key = %q, want the-key", key)
	}
}

func TestGateway_GetEncryptKey_NoKey_ReturnsInvalidResponse(t *testing.T) {
	g := newGateway(&fakeClient{encryptFunc: func(*larkeventv1.GetEncryptKeySubscriptionReq) (*larkeventv1.GetEncryptKeySubscriptionResp, error) {
		return &larkeventv1.GetEncryptKeySubscriptionResp{Data: &larkeventv1.GetEncryptKeySubscriptionRespData{EncryptKey: strPtr("")}}, nil
	}})
	_, err := g.GetEncryptKey(context.Background(), "sub_1")
	assertInvalidResponse(t, err)
}

// --- List (single page) ---

func TestGateway_List_ProjectsItemsAndPagination(t *testing.T) {
	g := newGateway(&fakeClient{listFunc: func(_ int, _ *larkeventv1.ListSubscriptionReq) (*larkeventv1.ListSubscriptionResp, error) {
		return &larkeventv1.ListSubscriptionResp{Data: &larkeventv1.ListSubscriptionRespData{
			Items:     []*larkeventv1.SubscriptionDetail{detail("sub_1"), detail("sub_2")},
			HasMore:   boolPtr(true),
			PageToken: strPtr("tok_next"),
		}}, nil
	}})
	page, err := g.List(context.Background(), ListParams{State: "active"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(page.Items) != 2 || page.Items[0].ID.String() != "sub_1" || page.Items[1].ID.String() != "sub_2" {
		t.Fatalf("items = %+v, want [sub_1 sub_2]", page.Items)
	}
	if !page.HasMore || page.NextPageToken != "tok_next" {
		t.Errorf("pagination = (%v, %q), want (true, tok_next)", page.HasMore, page.NextPageToken)
	}
}

func TestGateway_List_EmptyData_ReturnsEmptyPage(t *testing.T) {
	g := newGateway(&fakeClient{listFunc: func(_ int, _ *larkeventv1.ListSubscriptionReq) (*larkeventv1.ListSubscriptionResp, error) {
		return &larkeventv1.ListSubscriptionResp{}, nil
	}})
	page, err := g.List(context.Background(), ListParams{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if page.Items == nil || len(page.Items) != 0 || page.HasMore {
		t.Errorf("page = %+v, want empty non-nil items and HasMore=false", page)
	}
}

// --- WalkSubscriptions (bounded, projecting, early-stop, capped) ---

func TestGateway_WalkSubscriptions_StopsEarlyWhenVisitReturnsFalse(t *testing.T) {
	g := newGateway(&fakeClient{listFunc: func(call int, _ *larkeventv1.ListSubscriptionReq) (*larkeventv1.ListSubscriptionResp, error) {
		return &larkeventv1.ListSubscriptionResp{Data: &larkeventv1.ListSubscriptionRespData{
			Items: []*larkeventv1.SubscriptionDetail{detail(fmt.Sprintf("sub_%d", call))}, HasMore: boolPtr(true), PageToken: strPtr("t"),
		}}, nil
	}})
	var seen []string
	capped, err := g.WalkSubscriptions(context.Background(), ListParams{}, func(s model.RemoteSubscription) bool {
		seen = append(seen, s.ID.String())
		return false // stop after the first
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capped {
		t.Error("capped = true, want false when visit stopped early")
	}
	if len(seen) != 1 {
		t.Errorf("visited %v, want exactly one before stopping", seen)
	}
}

// --- buildPatchBody: filter projection onto the Patch request body ---

func TestBuildPatchBody_SetFilter_ProjectsFilter(t *testing.T) {
	f, err := event.ParseAndValidateFilter(
		`{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"message_type","op":"eq","value":"text"}}]}}`,
		event.FilterMetaFor("im.message.created_v1"))
	if err != nil {
		t.Fatalf("build filter: %v", err)
	}
	body := buildPatchBody(PatchSpec{Filter: f})
	if body.Filter == nil {
		t.Fatal("body.Filter is nil, want the projected filter")
	}
	got, err := json.Marshal(body.Filter)
	if err != nil {
		t.Fatalf("marshal body.Filter: %v", err)
	}
	want, err := f.Canonicalize()
	if err != nil {
		t.Fatalf("canonicalize: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("body.Filter JSON = %s, want %s", got, want)
	}
}

// TestBuildPatchBody_ClearFilter_IsClearForm locks that clearing sends the
// documented {"filter":{}} clear form (a non-nil empty filter), distinct from
// omitting the field (which the server reads as "leave the filter unchanged").
func TestBuildPatchBody_ClearFilter_IsClearForm(t *testing.T) {
	for name, f := range map[string]*event.Filter{"nil": nil, "empty": {}} {
		t.Run(name, func(t *testing.T) {
			body := buildPatchBody(PatchSpec{Filter: f})
			if body.Filter == nil {
				t.Fatal(`clear form must send a non-nil empty filter ({"filter":{}}), not omit the field`)
			}
			if body.Filter.CompositeCondition != nil {
				t.Errorf("clear form must have a nil CompositeCondition, got %+v", body.Filter.CompositeCondition)
			}
			got, err := json.Marshal(body.Filter)
			if err != nil {
				t.Fatalf("marshal body.Filter: %v", err)
			}
			if string(got) != "{}" {
				t.Errorf("clear-form filter JSON = %s, want {}", got)
			}
		})
	}
}

// TestBuildCreateBody_WithKeyAndFilter_SetsAtomically locks that an encrypted
// create sets include_resource_data and the encrypt_key on the SAME
// payload_options, and projects a requested filter; and that no filter/key
// omits those fields entirely.
func TestBuildCreateBody_Projection(t *testing.T) {
	f, err := event.ParseAndValidateFilter(
		`{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"message_type","op":"eq","value":"text"}}]}}`,
		event.FilterMetaFor("im.message.created_v1"))
	if err != nil {
		t.Fatalf("build filter: %v", err)
	}
	body := buildCreateBody(CreateSpec{EventType: "im.message.created_v1", TargetResource: "im.message?chat_id=oc_aaa", IncludeResourceData: true, EncryptKey: "the-key", Filter: f})
	if body.PayloadOptions == nil || body.PayloadOptions.IncludeResourceData == nil || !*body.PayloadOptions.IncludeResourceData {
		t.Fatalf("include_resource_data not set: %+v", body.PayloadOptions)
	}
	if body.PayloadOptions.Encrypt == nil || body.PayloadOptions.Encrypt.EncryptKey == nil || *body.PayloadOptions.Encrypt.EncryptKey != "the-key" {
		t.Fatalf("encrypt_key not set atomically: %+v", body.PayloadOptions.Encrypt)
	}
	if body.Filter == nil {
		t.Error("body.Filter is nil, want the projected filter")
	}

	plain := buildCreateBody(CreateSpec{EventType: "im.message.created_v1", TargetResource: "im.message?chat_id=oc_aaa"})
	if plain.PayloadOptions.Encrypt != nil {
		t.Errorf("Encrypt = %+v, want nil when no key supplied", plain.PayloadOptions.Encrypt)
	}
	if plain.Filter != nil {
		t.Errorf("body.Filter = %+v, want nil (omitted) when no filter requested", plain.Filter)
	}
}

func TestGateway_WalkSubscriptions_HitsCap(t *testing.T) {
	g := newGateway(&fakeClient{listFunc: func(call int, _ *larkeventv1.ListSubscriptionReq) (*larkeventv1.ListSubscriptionResp, error) {
		return &larkeventv1.ListSubscriptionResp{Data: &larkeventv1.ListSubscriptionRespData{
			Items: []*larkeventv1.SubscriptionDetail{detail(fmt.Sprintf("sub_%d", call))}, HasMore: boolPtr(true), PageToken: strPtr(fmt.Sprintf("t%d", call)),
		}}, nil
	}})
	fake := g.client.(*fakeClient)
	capped, err := g.WalkSubscriptions(context.Background(), ListParams{}, func(model.RemoteSubscription) bool { return true })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !capped {
		t.Error("capped = false, want true (every page had has_more=true)")
	}
	if fake.listCalls != event.MaxSubscriptionListPages {
		t.Errorf("List called %d times, want %d (bounded by the cap)", fake.listCalls, event.MaxSubscriptionListPages)
	}
}
