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
var _ EncryptKeyProber = (*SubscriptionClient)(nil)

// ---- ReconcileExisting: encryption conflict matrix (task-E-design-note.md
// task E2; design spec §4.7's conflict matrix) ----
//
// Every test in this block passes requestedIncludeResourceData=true: the
// CLI's own fail-closed policy (spec §4.7 "载荷与加密模型") means "true"
// always means "the caller wants an ENCRYPTED subscription" — there is no
// CLI-level "plaintext resource_data" request to compare against. The
// existing requestedIncludeResourceData=false tests above (already GREEN,
// entirely unmodified by this task) double as the "backward compatible, no
// WithEncryptKeyProber option supplied" proof: internal/event/consume/
// refined.go's own ReconcileExisting call site (out of E2's scope; must not
// be touched) always passes false and zero options, so it is completely
// unaffected by every addition below.

// fakeEncryptKeyProber is a network-free stand-in for the EncryptKeyProber
// seam ReconcileExisting's encryption conflict-matrix probe depends on.
type fakeEncryptKeyProber struct {
	resp  *larkeventv1.GetEncryptKeySubscriptionResp
	err   error
	calls int
}

func (f *fakeEncryptKeyProber) GetEncryptKey(_ context.Context, _ *larkeventv1.GetEncryptKeySubscriptionReq) (*larkeventv1.GetEncryptKeySubscriptionResp, error) {
	f.calls++
	return f.resp, f.err
}

func encryptKeyResp(key string) *larkeventv1.GetEncryptKeySubscriptionResp {
	return &larkeventv1.GetEncryptKeySubscriptionResp{
		Data: &larkeventv1.GetEncryptKeySubscriptionRespData{EncryptKey: strPtr(key)},
	}
}

// TestReconcileExisting_EncryptedBothTrue_UsableKey_ReturnsReuseAction locks
// spec §4.7's "加密资源详情 | 加密资源详情且密钥可取 | 复用" row: an active
// match whose include_resource_data is already true, when the local request
// ALSO wants true (i.e. encrypted), reuses it once GetEncryptKey confirms a
// usable (non-empty) key — never a new Create, never a new key.
func TestReconcileExisting_EncryptedBothTrue_UsableKey_ReturnsReuseAction(t *testing.T) {
	fake := &fakeLister{resp: listResp([]*larkeventv1.SubscriptionDetail{
		activeSub("sub_enc", true, "user"),
	})}
	prober := &fakeEncryptKeyProber{resp: encryptKeyResp("usable-key-value")}

	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, true, WithEncryptKeyProber(prober))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != PlanActionReuse {
		t.Errorf("Action = %q, want %q", plan.Action, PlanActionReuse)
	}
	if plan.Existing == nil || strVal(plan.Existing.SubscriptionId) != "sub_enc" {
		t.Errorf("Existing = %+v, want sub_enc", plan.Existing)
	}
	if prober.calls != 1 {
		t.Errorf("prober.calls = %d, want exactly 1", prober.calls)
	}
}

// TestReconcileExisting_EncryptedBothTrue_EmptyKey_ReturnsConflictAction
// locks spec §4.7's "加密资源详情但密钥不可取" row: a successful
// GetEncryptKey response carrying no usable key (empty/absent) must conflict
// — it is indistinguishable from a plaintext resource_data match, and both
// are unsafe to auto-reuse.
func TestReconcileExisting_EncryptedBothTrue_EmptyKey_ReturnsConflictAction(t *testing.T) {
	fake := &fakeLister{resp: listResp([]*larkeventv1.SubscriptionDetail{
		activeSub("sub_enc", true, "user"),
	})}
	prober := &fakeEncryptKeyProber{resp: encryptKeyResp("")}

	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, true, WithEncryptKeyProber(prober))
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

// TestReconcileExisting_EncryptedBothTrue_NilData_ReturnsConflictAction
// covers the "success response but nil Data" shape (distinct from an
// explicit empty-string key) — must classify the same as "no usable key".
func TestReconcileExisting_EncryptedBothTrue_NilData_ReturnsConflictAction(t *testing.T) {
	fake := &fakeLister{resp: listResp([]*larkeventv1.SubscriptionDetail{
		activeSub("sub_enc", true, "user"),
	})}
	prober := &fakeEncryptKeyProber{resp: &larkeventv1.GetEncryptKeySubscriptionResp{}}

	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, true, WithEncryptKeyProber(prober))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != PlanActionConflict {
		t.Errorf("Action = %q, want %q", plan.Action, PlanActionConflict)
	}
}

// TestReconcileExisting_EncryptedBothTrue_ProberErrors_ReturnsConflictAction
// locks the task's explicit "CONTROLLER INTERPRETATION": ANY GetEncryptKey
// failure (business or transport) — not just a confirmed empty key — must
// still classify as conflict/human-decision here, never propagate as a raw
// error and never silently reuse. Retrying `create` re-probes from scratch;
// a transient network blip and a real permission/state problem both need
// the same "go verify / delete+recreate" guidance at this layer.
func TestReconcileExisting_EncryptedBothTrue_ProberErrors_ReturnsConflictAction(t *testing.T) {
	fake := &fakeLister{resp: listResp([]*larkeventv1.SubscriptionDetail{
		activeSub("sub_enc", true, "user"),
	})}
	prober := &fakeEncryptKeyProber{err: errors.New("boom: permission denied")}

	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, true, WithEncryptKeyProber(prober))
	if err != nil {
		t.Fatalf("unexpected error (a probe failure must classify as conflict, not propagate): %v", err)
	}
	if plan.Action != PlanActionConflict {
		t.Errorf("Action = %q, want %q", plan.Action, PlanActionConflict)
	}
	if prober.calls != 1 {
		t.Errorf("prober.calls = %d, want exactly 1", prober.calls)
	}
}

// TestReconcileExisting_EncryptedRemoteFalse_ReturnsConflict_NeverProbes
// locks spec §4.7's "加密资源详情 | include_resource_data=false | 冲突,人工
// 决策" row: when the remote match itself has include_resource_data=false
// but the local request wants true (encrypted), this is a conflict from the
// existing include_resource_data mismatch alone — the encrypt-key probe
// must never even run (there is no encrypted state to confirm).
func TestReconcileExisting_EncryptedRemoteFalse_ReturnsConflict_NeverProbes(t *testing.T) {
	fake := &fakeLister{resp: listResp([]*larkeventv1.SubscriptionDetail{
		activeSub("sub_plain", false, "user"),
	})}
	prober := &fakeEncryptKeyProber{resp: encryptKeyResp("usable-key-value")}

	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, true, WithEncryptKeyProber(prober))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != PlanActionConflict {
		t.Errorf("Action = %q, want %q", plan.Action, PlanActionConflict)
	}
	if prober.calls != 0 {
		t.Errorf("prober.calls = %d, want 0: the encrypt-key probe must never run when the state mismatch is already decisive", prober.calls)
	}
}

// TestReconcileExisting_EncryptedBothTrue_NoProberSupplied_FailsClosed is a
// defensive-programming lock: requestedIncludeResourceData=true reaching an
// active, include_resource_data=true match with NO WithEncryptKeyProber
// option supplied must never silently reuse (that would defeat the whole
// conflict matrix) — it is a typed internal error instead. Every real
// caller that can reach requestedIncludeResourceData=true always supplies a
// prober (cmd/event/subscription/create.go); this only guards a
// theoretical future caller that forgets to.
func TestReconcileExisting_EncryptedBothTrue_NoProberSupplied_FailsClosed(t *testing.T) {
	fake := &fakeLister{resp: listResp([]*larkeventv1.SubscriptionDetail{
		activeSub("sub_enc", true, "user"),
	})}

	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, true)
	if err == nil {
		t.Fatalf("expected an error (fail-closed, no silent reuse), got plan: %+v", plan)
	}
	if plan != nil && plan.Action == PlanActionReuse {
		t.Errorf("must never silently reuse without a supplied EncryptKeyProber, got plan: %+v", plan)
	}
}
