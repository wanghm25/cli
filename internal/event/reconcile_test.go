// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"context"
	"errors"
	"testing"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/internal/core"
)

// ---- fakeLister: a network-free SubscriptionLister test seam ----

type fakeLister struct {
	resp *larkeventv1.ListSubscriptionResp
	err  error
}

func (f *fakeLister) List(_ context.Context, _ *larkeventv1.ListSubscriptionReq) (*larkeventv1.ListSubscriptionResp, error) {
	return f.resp, f.err
}

func listResp(items []*larkeventv1.SubscriptionDetail) *larkeventv1.ListSubscriptionResp {
	return &larkeventv1.ListSubscriptionResp{
		Data: &larkeventv1.ListSubscriptionRespData{Items: items},
	}
}

func strPtr(s string) *string { return &s }
func boolPtr(b bool) *bool    { return &b }

func activeSub(id string, includeResourceData bool, authorityType string) *larkeventv1.SubscriptionDetail {
	return &larkeventv1.SubscriptionDetail{
		SubscriptionId: strPtr(id),
		EventType:      strPtr("im.message.created_v1"),
		TargetResource: strPtr("im.message?chat_id=oc_aaa"),
		Authority:      &larkeventv1.Authority{Type: strPtr(authorityType), OpenId: strPtr("ou_aaa")},
		State:          strPtr("active"),
		PayloadOptions: &larkeventv1.PayloadOptions{IncludeResourceData: boolPtr(includeResourceData)},
	}
}

func suspendedSub(id, reason string) *larkeventv1.SubscriptionDetail {
	return &larkeventv1.SubscriptionDetail{
		SubscriptionId: strPtr(id),
		EventType:      strPtr("im.message.created_v1"),
		TargetResource: strPtr("im.message?chat_id=oc_aaa"),
		Authority:      &larkeventv1.Authority{Type: strPtr("user"), OpenId: strPtr("ou_aaa")},
		State:          strPtr("suspended"),
		Suspension:     &larkeventv1.Suspension{Code: strPtr(reason)},
	}
}

// ---- ReconcileExisting: state table (spec §3.3/§4.2) ----

func TestReconcileExisting_NotFound_ReturnsCreateAction(t *testing.T) {
	fake := &fakeLister{resp: listResp(nil)}

	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != PlanActionCreate {
		t.Errorf("Action = %q, want %q", plan.Action, PlanActionCreate)
	}
	if plan.Existing != nil {
		t.Errorf("Existing = %+v, want nil", plan.Existing)
	}
}

func TestReconcileExisting_ActiveCompatible_ReturnsReuseAction(t *testing.T) {
	fake := &fakeLister{resp: listResp([]*larkeventv1.SubscriptionDetail{
		activeSub("sub_1", false, "user"),
	})}

	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != PlanActionReuse {
		t.Errorf("Action = %q, want %q", plan.Action, PlanActionReuse)
	}
	if plan.Existing == nil || strVal(plan.Existing.SubscriptionId) != "sub_1" {
		t.Errorf("Existing = %+v, want sub_1", plan.Existing)
	}
}

func TestReconcileExisting_ActiveConflicting_ReturnsConflictActionWithFields(t *testing.T) {
	fake := &fakeLister{resp: listResp([]*larkeventv1.SubscriptionDetail{
		activeSub("sub_1", true, "user"), // existing has include_resource_data=true
	})}

	// requested include_resource_data=false mismatches the existing true.
	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != PlanActionConflict {
		t.Fatalf("Action = %q, want %q", plan.Action, PlanActionConflict)
	}
	if len(plan.ConflictFields) != 1 || plan.ConflictFields[0].Name != "include_resource_data" {
		t.Errorf("ConflictFields = %+v, want one entry naming include_resource_data", plan.ConflictFields)
	}
	if plan.ConflictFields[0].Reason == "" {
		t.Error("ConflictFields[0].Reason must not be empty")
	}
}

func TestReconcileExisting_Suspended_ReturnsSuspendedAction(t *testing.T) {
	fake := &fakeLister{resp: listResp([]*larkeventv1.SubscriptionDetail{
		suspendedSub("sub_1", "authority_revoked"),
	})}

	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != PlanActionSuspended {
		t.Errorf("Action = %q, want %q", plan.Action, PlanActionSuspended)
	}
	if plan.Existing == nil || strVal(plan.Existing.SubscriptionId) != "sub_1" {
		t.Errorf("Existing = %+v, want sub_1", plan.Existing)
	}
}

func TestReconcileExisting_ExpiredOrDeletedOrUnknown_TreatedAsNew(t *testing.T) {
	for _, state := range []string{"expired", "deleted", "", "some_future_state"} {
		t.Run(state, func(t *testing.T) {
			fake := &fakeLister{resp: listResp([]*larkeventv1.SubscriptionDetail{
				{
					SubscriptionId: strPtr("sub_old"),
					EventType:      strPtr("im.message.created_v1"),
					TargetResource: strPtr("im.message?chat_id=oc_aaa"),
					Authority:      &larkeventv1.Authority{Type: strPtr("user"), OpenId: strPtr("ou_aaa")},
					State:          strPtr(state),
				},
			})}

			plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if plan.Action != PlanActionCreate {
				t.Errorf("Action = %q, want %q (state %q treated as inert/not-found)", plan.Action, PlanActionCreate, state)
			}
		})
	}
}

func TestReconcileExisting_TransportError_PropagatesUnchanged(t *testing.T) {
	sentinel := errors.New("boom: connection reset")
	fake := &fakeLister{err: sentinel}

	_, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false)
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want it passed through unchanged (%v)", err, sentinel)
	}
}

// ---- authority type match (bot -> "app", user -> "user") ----

func TestReconcileExisting_AuthorityMismatch_IgnoresOtherIdentityItems(t *testing.T) {
	// An "app" authority item must not be treated as a match for a "user"
	// identity's reconcile — never reuse/conflict against another
	// identity's subscription.
	fake := &fakeLister{resp: listResp([]*larkeventv1.SubscriptionDetail{
		activeSub("sub_app", false, "app"),
	})}

	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != PlanActionCreate {
		t.Errorf("Action = %q, want %q (the only match belongs to a different authority)", plan.Action, PlanActionCreate)
	}
}

func TestReconcileExisting_BotIdentityMatchesAppAuthority_ReturnsReuseAction(t *testing.T) {
	fake := &fakeLister{resp: listResp([]*larkeventv1.SubscriptionDetail{
		activeSub("sub_app", false, "app"),
	})}

	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsBot, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != PlanActionReuse {
		t.Errorf("Action = %q, want %q", plan.Action, PlanActionReuse)
	}
	if plan.Existing == nil || strVal(plan.Existing.SubscriptionId) != "sub_app" {
		t.Errorf("Existing = %+v, want sub_app", plan.Existing)
	}
}

func TestAuthorityMatchesIdentity_UserIdentity_MatchesUserAuthority(t *testing.T) {
	if !AuthorityMatchesIdentity(&larkeventv1.Authority{Type: strPtr("user")}, core.AsUser) {
		t.Error("want true: user identity matches a user-typed authority")
	}
}

func TestAuthorityMatchesIdentity_BotIdentity_MatchesAppAuthority(t *testing.T) {
	if !AuthorityMatchesIdentity(&larkeventv1.Authority{Type: strPtr("app")}, core.AsBot) {
		t.Error("want true: bot identity matches an app-typed authority")
	}
}

func TestAuthorityMatchesIdentity_UserIdentity_DoesNotMatchAppAuthority(t *testing.T) {
	if AuthorityMatchesIdentity(&larkeventv1.Authority{Type: strPtr("app")}, core.AsUser) {
		t.Error("want false: user identity must not match an app-typed authority")
	}
}

func TestAuthorityMatchesIdentity_BotIdentity_DoesNotMatchUserAuthority(t *testing.T) {
	if AuthorityMatchesIdentity(&larkeventv1.Authority{Type: strPtr("user")}, core.AsBot) {
		t.Error("want false: bot identity must not match a user-typed authority")
	}
}

func TestAuthorityMatchesIdentity_NilAuthorityOrType_ReturnsFalse(t *testing.T) {
	if AuthorityMatchesIdentity(nil, core.AsUser) {
		t.Error("want false for a nil Authority")
	}
	if AuthorityMatchesIdentity(&larkeventv1.Authority{}, core.AsUser) {
		t.Error("want false for a nil Authority.Type")
	}
}

// ---- seam satisfaction: *SubscriptionClient must satisfy the exported
// interfaces structurally (compile-time assertion, not a runtime test) ----

var _ SubscriptionLister = (*SubscriptionClient)(nil)
var _ SubscriptionCreateAPI = (*SubscriptionClient)(nil)
