// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

// pagedListResp builds one page of a ListSubscriptionResp, explicit about
// has_more/page_token (unlike listResp above, which always leaves both nil —
// i.e. an implicit "no more pages" single-page response).
func pagedListResp(items []*larkeventv1.SubscriptionDetail, hasMore bool, nextToken string) *larkeventv1.ListSubscriptionResp {
	d := &larkeventv1.ListSubscriptionRespData{Items: items, HasMore: boolPtr(hasMore)}
	if nextToken != "" {
		d.PageToken = strPtr(nextToken)
	}
	return &larkeventv1.ListSubscriptionResp{Data: d}
}

// pagedLister is a network-free SubscriptionLister test seam that serves a
// response per call via pageAt (0-indexed by call order) — unlike fakeLister
// above (one fixed response), this lets a test drive WalkSubscriptionPages/
// ReconcileExisting across multiple pages, including an unbounded has_more
// sequence for the page-cap test. cancel/cancelAfterCall optionally cancel a
// context.CancelFunc right after serving a given call, to test ctx
// cancellation mid-pagination.
type pagedLister struct {
	pageAt          func(call int) *larkeventv1.ListSubscriptionResp
	calls           int
	cancel          context.CancelFunc
	cancelAfterCall int
}

func (p *pagedLister) List(_ context.Context, _ *larkeventv1.ListSubscriptionReq) (*larkeventv1.ListSubscriptionResp, error) {
	call := p.calls
	p.calls++
	if p.cancel != nil && call == p.cancelAfterCall {
		p.cancel()
	}
	return p.pageAt(call), nil
}

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

// ---- ReconcileExisting: state table ----

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

// ---- ReconcileExisting: pagination (issue: only scanning List's first page
// could misjudge Create when a real authority match sits on a later page) ----

// TestReconcileExisting_AuthorityMatchOnSecondPage_ReturnsReuseAction locks
// the core fix: a match on page 2 must be found (never misjudged as Create)
// even though page 1 alone has no match and reports has_more=true.
func TestReconcileExisting_AuthorityMatchOnSecondPage_ReturnsReuseAction(t *testing.T) {
	page1 := pagedListResp([]*larkeventv1.SubscriptionDetail{
		activeSub("sub_other", false, "app"), // wrong authority type for AsUser -- not a match
	}, true, "page-2-token")
	page2 := pagedListResp([]*larkeventv1.SubscriptionDetail{
		activeSub("sub_1", false, "user"),
	}, false, "")
	fake := &pagedLister{pageAt: func(call int) *larkeventv1.ListSubscriptionResp {
		if call == 0 {
			return page1
		}
		return page2
	}}

	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != PlanActionReuse {
		t.Errorf("Action = %q, want %q", plan.Action, PlanActionReuse)
	}
	if plan.Existing == nil || strVal(plan.Existing.SubscriptionId) != "sub_1" {
		t.Errorf("Existing = %+v, want sub_1 (found on page 2)", plan.Existing)
	}
	if plan.PaginationCapped {
		t.Error("PaginationCapped = true, want false (match found well before the cap)")
	}
	if fake.calls != 2 {
		t.Errorf("List called %d times, want exactly 2 (stop as soon as page 2's match is found)", fake.calls)
	}
}

// TestReconcileExisting_NoMatchAllPages_HasMoreFalse_ReturnsCreate_NotCapped
// locks the "genuinely not found" case: every page was actually read
// (has_more=false ends it for real), so PaginationCapped must stay false —
// this is a confirmed not-found, not an unconfirmed one.
func TestReconcileExisting_NoMatchAllPages_HasMoreFalse_ReturnsCreate_NotCapped(t *testing.T) {
	page1 := pagedListResp([]*larkeventv1.SubscriptionDetail{activeSub("sub_other", false, "app")}, true, "page-2-token")
	page2 := pagedListResp(nil, false, "")
	fake := &pagedLister{pageAt: func(call int) *larkeventv1.ListSubscriptionResp {
		if call == 0 {
			return page1
		}
		return page2
	}}

	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != PlanActionCreate {
		t.Errorf("Action = %q, want %q", plan.Action, PlanActionCreate)
	}
	if plan.PaginationCapped {
		t.Error("PaginationCapped = true, want false (has_more=false genuinely exhausted every page)")
	}
	if fake.calls != 2 {
		t.Errorf("List called %d times, want exactly 2", fake.calls)
	}
}

// TestReconcileExisting_PageCapReached_ReturnsCreate_PaginationCapped locks
// the bound: an unbounded has_more=true sequence that never matches and
// never ends must still terminate (never hang/loop forever), defaulting to
// Create but flagging PaginationCapped so the caller logs it instead of
// silently treating the scan as a confirmed not-found.
func TestReconcileExisting_PageCapReached_ReturnsCreate_PaginationCapped(t *testing.T) {
	fake := &pagedLister{pageAt: func(call int) *larkeventv1.ListSubscriptionResp {
		return pagedListResp([]*larkeventv1.SubscriptionDetail{activeSub("sub_other", false, "app")}, true, fmt.Sprintf("token-%d", call+1))
	}}

	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != PlanActionCreate {
		t.Errorf("Action = %q, want %q (an unconfirmed scan still defaults to Create)", plan.Action, PlanActionCreate)
	}
	if !plan.PaginationCapped {
		t.Error("PaginationCapped = false, want true (the scan never found has_more=false or a match)")
	}
	if fake.calls != MaxSubscriptionListPages {
		t.Errorf("List called %d times, want exactly %d (bounded by the page cap)", fake.calls, MaxSubscriptionListPages)
	}
}

// TestReconcileExisting_CtxCancelMidPagination_ReturnsError_NoCreate locks
// ctx-awareness: a caller cancelling mid-scan must get an error (never a
// silently-returned Create plan) and no further List call.
func TestReconcileExisting_CtxCancelMidPagination_ReturnsError_NoCreate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fake := &pagedLister{
		pageAt: func(call int) *larkeventv1.ListSubscriptionResp {
			return pagedListResp(nil, true, fmt.Sprintf("token-%d", call+1))
		},
		cancel:          cancel,
		cancelAfterCall: 0, // cancel right after the first page is served
	}

	plan, err := ReconcileExisting(ctx, fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false)
	if err == nil {
		t.Fatalf("expected an error from ctx cancellation mid-pagination, got plan: %+v", plan)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want it to wrap context.Canceled", err)
	}
	if plan != nil {
		t.Errorf("plan = %+v, want nil on a cancellation error (never a Create)", plan)
	}
	if fake.calls != 1 {
		t.Errorf("List called %d times, want exactly 1 (cancellation caught before a second call)", fake.calls)
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

// ---- ReconcileExisting: encryption conflict matrix ----
//
// Every test in this block passes requestedIncludeResourceData=true: the
// CLI's own fail-closed policy means "true"
// always means "the caller wants an ENCRYPTED subscription" — there is no
// CLI-level "plaintext resource_data" request to compare against. The
// existing requestedIncludeResourceData=false tests above (already GREEN,
// entirely unmodified) double as the "backward compatible, no
// WithEncryptKeyProber option supplied" proof: internal/event/consume/
// refined.go's own ReconcileExisting call site always passes false and zero
// options, so it is completely
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
// the conflict-matrix reuse row: an active
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
// locks the "encrypted match, key not retrievable" row: a successful
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
// locks the conflict row for an encrypted request against an existing
// include_resource_data=false match: when the remote match itself has include_resource_data=false
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

// TestReconcileExisting_EncryptedBothTrue_DeferredConfirmation_ReusesWithoutProbing
// locks the consume path: WithDeferredEncryptKeyConfirmation reuses an active,
// include_resource_data=true match WITHOUT probing its key — the bus Hello is
// the authoritative key gate, so the front-end never calls GetEncryptKey. (No
// prober is supplied at all here; the deferred branch must not require one.)
func TestReconcileExisting_EncryptedBothTrue_DeferredConfirmation_ReusesWithoutProbing(t *testing.T) {
	fake := &fakeLister{resp: listResp([]*larkeventv1.SubscriptionDetail{
		activeSub("sub_enc", true, "user"),
	})}

	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, true, WithDeferredEncryptKeyConfirmation())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != PlanActionReuse {
		t.Errorf("Action = %q, want %q (deferred confirmation reuses without probing)", plan.Action, PlanActionReuse)
	}
	if plan.Existing == nil || strVal(plan.Existing.SubscriptionId) != "sub_enc" {
		t.Errorf("Existing = %+v, want sub_enc", plan.Existing)
	}
}

// ---- ReconcileExisting: filter reuse dimension ----
//
// Reuse of an active match requires the requested filter to Equal the remote
// one; any difference (including empty-vs-filtered either way) is a conflict —
// the fail-closed rule mirroring include_resource_data. WithRequestedFilter is
// the option carrying the requested filter; omitting it means "no filter".

// filterAlt is a second valid filter, distinct from sampleFilter()
// (filter_sdk_test.go), used to force a filter mismatch.
func filterAlt() *Filter {
	return &Filter{Root: &FilterNode{
		LogicOp:  logicAnd,
		Children: []*FilterNode{{Condition: &FilterCond{Operand: "message_type", Op: opEq, Value: "text"}}},
	}}
}

// filteredSub is an active authority match carrying remote filter f.
func filteredSub(id, authorityType string, f *Filter) *larkeventv1.SubscriptionDetail {
	sub := activeSub(id, false, authorityType)
	sub.Filter = FilterToSDK(f)
	return sub
}

func TestReconcileExisting_FilterMismatch_ReturnsConflictWithFilterField(t *testing.T) {
	fake := &fakeLister{resp: listResp([]*larkeventv1.SubscriptionDetail{
		filteredSub("sub_1", "user", sampleFilter()),
	})}

	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false, WithRequestedFilter(filterAlt()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != PlanActionConflict {
		t.Fatalf("Action = %q, want %q", plan.Action, PlanActionConflict)
	}
	if len(plan.ConflictFields) != 1 || plan.ConflictFields[0].Name != "filter" {
		t.Fatalf("ConflictFields = %+v, want one entry naming filter", plan.ConflictFields)
	}
	if plan.ConflictFields[0].Reason == "" {
		t.Error("ConflictFields[0].Reason must not be empty")
	}
	// Never-leak: the reason must not echo any filter contents.
	if strings.Contains(plan.ConflictFields[0].Reason, "message_type") ||
		strings.Contains(plan.ConflictFields[0].Reason, "ou_abc") ||
		strings.Contains(plan.ConflictFields[0].Reason, "text") {
		t.Errorf("Reason must not leak filter contents: %q", plan.ConflictFields[0].Reason)
	}
}

func TestReconcileExisting_FilterEqual_ReturnsReuse(t *testing.T) {
	fake := &fakeLister{resp: listResp([]*larkeventv1.SubscriptionDetail{
		filteredSub("sub_1", "user", sampleFilter()),
	})}

	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false, WithRequestedFilter(sampleFilter()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != PlanActionReuse {
		t.Errorf("Action = %q, want %q (identical filter is compatible)", plan.Action, PlanActionReuse)
	}
}

func TestReconcileExisting_EmptyRequestedVsFilteredRemote_ReturnsConflict(t *testing.T) {
	fake := &fakeLister{resp: listResp([]*larkeventv1.SubscriptionDetail{
		filteredSub("sub_1", "user", sampleFilter()),
	})}

	// No WithRequestedFilter option -> requested is treated as empty.
	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != PlanActionConflict {
		t.Fatalf("Action = %q, want %q (empty request under-delivers vs a filtered remote)", plan.Action, PlanActionConflict)
	}
	if len(plan.ConflictFields) != 1 || plan.ConflictFields[0].Name != "filter" {
		t.Errorf("ConflictFields = %+v, want one entry naming filter", plan.ConflictFields)
	}
}

func TestReconcileExisting_FilteredRequestedVsEmptyRemote_ReturnsConflict(t *testing.T) {
	// activeSub carries no filter -> remote is unfiltered.
	fake := &fakeLister{resp: listResp([]*larkeventv1.SubscriptionDetail{
		activeSub("sub_1", false, "user"),
	})}

	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false, WithRequestedFilter(sampleFilter()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != PlanActionConflict {
		t.Fatalf("Action = %q, want %q (a filtered request against an unfiltered remote conflicts)", plan.Action, PlanActionConflict)
	}
	if len(plan.ConflictFields) != 1 || plan.ConflictFields[0].Name != "filter" {
		t.Errorf("ConflictFields = %+v, want one entry naming filter", plan.ConflictFields)
	}
}

func TestReconcileExisting_BothEmptyFilter_ReturnsReuse(t *testing.T) {
	fake := &fakeLister{resp: listResp([]*larkeventv1.SubscriptionDetail{
		activeSub("sub_1", false, "user"),
	})}

	// Explicit empty requested filter against an unfiltered remote -> reuse.
	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false, WithRequestedFilter(&Filter{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != PlanActionReuse {
		t.Errorf("Action = %q, want %q (both empty is compatible)", plan.Action, PlanActionReuse)
	}
}

// TestReconcileExisting_EncryptedFilterMismatch_ReturnsConflict_NeverProbes
// locks that the filter dimension blocks reuse on the encrypted (management)
// path too, and does so BEFORE the encrypt-key probe runs — a filter mismatch
// is decisive on its own.
func TestReconcileExisting_EncryptedFilterMismatch_ReturnsConflict_NeverProbes(t *testing.T) {
	match := activeSub("sub_enc", true, "user")
	match.Filter = FilterToSDK(sampleFilter())
	fake := &fakeLister{resp: listResp([]*larkeventv1.SubscriptionDetail{match})}
	prober := &fakeEncryptKeyProber{resp: encryptKeyResp("usable-key-value")}

	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, true, WithEncryptKeyProber(prober), WithRequestedFilter(filterAlt()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != PlanActionConflict {
		t.Fatalf("Action = %q, want %q", plan.Action, PlanActionConflict)
	}
	if len(plan.ConflictFields) != 1 || plan.ConflictFields[0].Name != "filter" {
		t.Errorf("ConflictFields = %+v, want one entry naming filter", plan.ConflictFields)
	}
	if prober.calls != 0 {
		t.Errorf("prober.calls = %d, want 0: a filter mismatch is decisive before the encrypt-key probe", prober.calls)
	}
}

// TestReconcileExisting_EncryptedDeferred_FilterMismatch_ReturnsConflict locks
// the same for the consume (deferred key confirmation) encrypted path.
func TestReconcileExisting_EncryptedDeferred_FilterMismatch_ReturnsConflict(t *testing.T) {
	match := activeSub("sub_enc", true, "user")
	match.Filter = FilterToSDK(sampleFilter())
	fake := &fakeLister{resp: listResp([]*larkeventv1.SubscriptionDetail{match})}

	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, true, WithDeferredEncryptKeyConfirmation(), WithRequestedFilter(filterAlt()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != PlanActionConflict {
		t.Fatalf("Action = %q, want %q", plan.Action, PlanActionConflict)
	}
	if len(plan.ConflictFields) != 1 || plan.ConflictFields[0].Name != "filter" {
		t.Errorf("ConflictFields = %+v, want one entry naming filter", plan.ConflictFields)
	}
}

// ---- ReconcileExisting: suspended-path filter reuse dimension ----
//
// A suspended match is reactivated and reused by the caller. Reactivating one
// whose remote filter differs from the requested filter would bind the caller
// to the wrong event stream, so the same fail-closed filter compare the active
// path uses applies here too: a mismatch is a conflict (never a silent
// reactivate-reuse), an equal filter leaves the reuse-eligible suspended plan.

// suspendedFilteredSub is a suspended authority match carrying remote filter f.
func suspendedFilteredSub(id, reason string, f *Filter) *larkeventv1.SubscriptionDetail {
	sub := suspendedSub(id, reason)
	sub.Filter = FilterToSDK(f)
	return sub
}

func TestReconcileExisting_Suspended_FilterMismatch_ReturnsConflict(t *testing.T) {
	fake := &fakeLister{resp: listResp([]*larkeventv1.SubscriptionDetail{
		suspendedFilteredSub("sub_1", "authority_revoked", sampleFilter()),
	})}

	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false, WithRequestedFilter(filterAlt()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != PlanActionConflict {
		t.Fatalf("Action = %q, want %q (an incompatible suspended sub must not be silently reactivated/reused)", plan.Action, PlanActionConflict)
	}
	if len(plan.ConflictFields) != 1 || plan.ConflictFields[0].Name != "filter" {
		t.Fatalf("ConflictFields = %+v, want one entry naming filter", plan.ConflictFields)
	}
	// Never-leak: the reason must name only the dimension, not any filter contents.
	if strings.Contains(plan.ConflictFields[0].Reason, "message_type") ||
		strings.Contains(plan.ConflictFields[0].Reason, "ou_abc") ||
		strings.Contains(plan.ConflictFields[0].Reason, "text") {
		t.Errorf("Reason must not leak filter contents: %q", plan.ConflictFields[0].Reason)
	}
}

func TestReconcileExisting_Suspended_FilterEqual_ReturnsSuspended(t *testing.T) {
	fake := &fakeLister{resp: listResp([]*larkeventv1.SubscriptionDetail{
		suspendedFilteredSub("sub_1", "authority_revoked", sampleFilter()),
	})}

	plan, err := ReconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false, WithRequestedFilter(sampleFilter()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != PlanActionSuspended {
		t.Errorf("Action = %q, want %q (an equal filter is compatible; reactivate-reuse stays OK)", plan.Action, PlanActionSuspended)
	}
	if plan.Existing == nil || strVal(plan.Existing.SubscriptionId) != "sub_1" {
		t.Errorf("Existing = %+v, want sub_1", plan.Existing)
	}
}
