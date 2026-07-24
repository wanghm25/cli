// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/credential"
	eventlib "github.com/larksuite/cli/internal/event"
)

// ---- fixtures ----

// registerCreateFixtures registers, for the duration of t, the same
// im.message.created_v1 refined-subscription base key + templates as
// internal/event/resolve_test.go's registerResolveFixtures (mirrored here:
// internal/event cannot be imported the other way around, and this package
// needs its own registered copy to exercise ResolveEventKey end-to-end),
// plus a legacy key for the "create rejects a legacy key" case.
func registerCreateFixtures(t *testing.T) {
	t.Helper()

	eventlib.RegisterKey(eventlib.KeyDefinition{
		Key:       "im.message.receive_v1",
		EventType: "im.message.receive_v1",
		Schema:    eventlib.SchemaDef{Custom: &eventlib.SchemaSpec{Raw: []byte(`{"type":"object"}`)}},
	})
	t.Cleanup(func() { eventlib.UnregisterKeyForTest("im.message.receive_v1") })

	eventlib.RegisterKey(eventlib.KeyDefinition{
		Key:                 "im.message.created_v1",
		EventType:           "im.message.created_v1",
		Schema:              eventlib.SchemaDef{Custom: &eventlib.SchemaSpec{Raw: []byte(`{"type":"object"}`)}},
		RefinedSubscription: true,
		ResourceType:        "im.message",
		AuthTypes:           []string{"user", "bot"},
		KeyTemplates: []eventlib.KeyTemplate{
			{
				Template:    "im.message.created_v1/chat-id/{chat_id}",
				Example:     "im.message.created_v1/chat-id/oc_9f3b1c2d8a",
				Description: "listen to new messages in a specific chat",
				SelectorKey: "chat_id",
				PathSegment: "chat-id",
				AuthTypes:   []string{"user", "bot"},
			},
			{
				Template:    "im.message.created_v1/owner/me",
				Example:     "im.message.created_v1/owner/me",
				Description: "listen to messages visible to the current user",
				SelectorKey: "owner",
				PathSegment: "owner",
				FixedValue:  "me",
				AuthTypes:   []string{"user"},
			},
		},
	})
	t.Cleanup(func() { eventlib.UnregisterKeyForTest("im.message.created_v1") })
}

// resolveCreatedChatID registers the fixtures and resolves the chat-id
// template, failing the test on any error (every reconcile/outcome test
// needs a valid ResolvedEventKey as a starting point, not the resolver's own
// behavior). Callers must not also call
// registerCreateFixtures directly — eventlib.RegisterKey panics on a
// duplicate key.
func resolveCreatedChatID(t *testing.T) eventlib.ResolvedEventKey {
	t.Helper()
	registerCreateFixtures(t)
	r, err := eventlib.ResolveEventKey("im.message.created_v1/chat-id/oc_aaa")
	if err != nil {
		t.Fatalf("ResolveEventKey: unexpected error: %v", err)
	}
	return r
}

// resolveCreatedOwnerMe is resolveCreatedChatID's owner/me counterpart; see
// its doc comment for the "do not also call registerCreateFixtures" caveat.
func resolveCreatedOwnerMe(t *testing.T) eventlib.ResolvedEventKey {
	t.Helper()
	registerCreateFixtures(t)
	r, err := eventlib.ResolveEventKey("im.message.created_v1/owner/me")
	if err != nil {
		t.Fatalf("ResolveEventKey: unexpected error: %v", err)
	}
	return r
}

// ---- fake createSubscriptionAPI ----

// fakeCreateAPI is a network-free stand-in for *eventlib.SubscriptionClient's
// List+Create — the createSubscriptionAPI test seam. Hook functions (rather
// than plain fields, cf. fakeListAPI/fakeGetAPI) so tests can vary the
// response by call count, which the "Create fails, reconcile via a second
// List" cases need.
type fakeCreateAPI struct {
	listFunc  func(call int) (*larkeventv1.ListSubscriptionResp, error)
	listCalls []*larkeventv1.ListSubscriptionReq

	createFunc  func() (*larkeventv1.CreateSubscriptionResp, error)
	createCalls int

	// getEncryptKeyFunc/getEncryptKeyCalls back the fake GetEncryptKey below
	// — the seam the encryption conflict-matrix's probe
	// (eventlib.ReconcileExisting's WithEncryptKeyProber) depends on. A nil
	// getEncryptKeyFunc defaults to "no usable key" (okEncryptKeyResp(""))
	// rather than success-with-a-key, so a test that forgets to set it up
	// fails toward the safe (conflict) outcome, not a silent reuse.
	getEncryptKeyFunc  func() (*larkeventv1.GetEncryptKeySubscriptionResp, error)
	getEncryptKeyCalls int
}

func (f *fakeCreateAPI) List(_ context.Context, req *larkeventv1.ListSubscriptionReq) (*larkeventv1.ListSubscriptionResp, error) {
	f.listCalls = append(f.listCalls, req)
	if f.listFunc == nil {
		return okListResp(nil, false, ""), nil
	}
	return f.listFunc(len(f.listCalls))
}

func (f *fakeCreateAPI) Create(_ context.Context, _ *larkeventv1.CreateSubscriptionReq) (*larkeventv1.CreateSubscriptionResp, error) {
	f.createCalls++
	if f.createFunc == nil {
		return okCreateResp(&larkeventv1.SubscriptionDetail{SubscriptionId: strPtr("sub_new")}), nil
	}
	return f.createFunc()
}

func (f *fakeCreateAPI) GetEncryptKey(_ context.Context, _ *larkeventv1.GetEncryptKeySubscriptionReq) (*larkeventv1.GetEncryptKeySubscriptionResp, error) {
	f.getEncryptKeyCalls++
	if f.getEncryptKeyFunc == nil {
		return okEncryptKeyResp(""), nil
	}
	return f.getEncryptKeyFunc()
}

// okEncryptKeyResp builds a synthetic, always-successful
// GetEncryptKeySubscriptionResp carrying key (which may be "" to model "no
// usable key returned").
func okEncryptKeyResp(key string) *larkeventv1.GetEncryptKeySubscriptionResp {
	return &larkeventv1.GetEncryptKeySubscriptionResp{
		ApiResp: &larkcore.ApiResp{RawBody: []byte(`{"code":0}`)},
		Data:    &larkeventv1.GetEncryptKeySubscriptionRespData{EncryptKey: strPtr(key)},
	}
}

// fixedList returns a fakeCreateAPI.listFunc that always returns the same
// response, for tests that only need one List behavior regardless of call
// count.
func fixedList(resp *larkeventv1.ListSubscriptionResp, err error) func(int) (*larkeventv1.ListSubscriptionResp, error) {
	return func(int) (*larkeventv1.ListSubscriptionResp, error) { return resp, err }
}

func okCreateResp(d *larkeventv1.SubscriptionDetail) *larkeventv1.CreateSubscriptionResp {
	return &larkeventv1.CreateSubscriptionResp{
		ApiResp: &larkcore.ApiResp{RawBody: []byte(`{"code":0}`)},
		Data:    &larkeventv1.CreateSubscriptionRespData{Subscription: d},
	}
}

func activeDetail(id string, includeResourceData bool, authorityType string) *larkeventv1.SubscriptionDetail {
	return &larkeventv1.SubscriptionDetail{
		SubscriptionId: strPtr(id),
		EventType:      strPtr("im.message.created_v1"),
		TargetResource: strPtr("im.message?chat_id=oc_aaa"),
		Authority:      &larkeventv1.Authority{Type: strPtr(authorityType), OpenId: strPtr("ou_aaa")},
		State:          strPtr("active"),
		PayloadOptions: &larkeventv1.PayloadOptions{IncludeResourceData: boolPtr(includeResourceData)},
	}
}

func suspendedDetail(id, reason string) *larkeventv1.SubscriptionDetail {
	return &larkeventv1.SubscriptionDetail{
		SubscriptionId: strPtr(id),
		EventType:      strPtr("im.message.created_v1"),
		TargetResource: strPtr("im.message?chat_id=oc_aaa"),
		Authority:      &larkeventv1.Authority{Type: strPtr("user"), OpenId: strPtr("ou_aaa")},
		State:          strPtr("suspended"),
		Suspension:     &larkeventv1.Suspension{Code: strPtr(reason)},
	}
}

// ---- reconcileExisting ----

func TestReconcileExisting_NotFound_ReturnsCreateAction(t *testing.T) {
	fake := &fakeCreateAPI{listFunc: fixedList(okListResp(nil, false, ""), nil)}

	plan, err := reconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != planActionCreate {
		t.Errorf("Action = %q, want %q", plan.Action, planActionCreate)
	}
	if plan.Existing != nil {
		t.Errorf("Existing = %+v, want nil", plan.Existing)
	}
}

func TestReconcileExisting_ActiveCompatible_ReturnsReuseAction(t *testing.T) {
	fake := &fakeCreateAPI{listFunc: fixedList(okListResp([]*larkeventv1.SubscriptionDetail{
		activeDetail("sub_1", false, "user"),
	}, false, ""), nil)}

	plan, err := reconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != planActionReuse {
		t.Errorf("Action = %q, want %q", plan.Action, planActionReuse)
	}
	if plan.Existing == nil || strVal(plan.Existing.SubscriptionId) != "sub_1" {
		t.Errorf("Existing = %+v, want sub_1", plan.Existing)
	}
}

func TestReconcileExisting_ActiveConflicting_ReturnsConflictActionWithFields(t *testing.T) {
	fake := &fakeCreateAPI{listFunc: fixedList(okListResp([]*larkeventv1.SubscriptionDetail{
		activeDetail("sub_1", true, "user"), // existing has include_resource_data=true
	}, false, ""), nil)}

	// requested include_resource_data=false (the only value that can reach
	// this point once the encryption check rejects true) mismatches the existing true.
	plan, err := reconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != planActionConflict {
		t.Fatalf("Action = %q, want %q", plan.Action, planActionConflict)
	}
	if len(plan.ConflictFields) != 1 || plan.ConflictFields[0].Name != "include_resource_data" {
		t.Errorf("ConflictFields = %+v, want one entry naming include_resource_data", plan.ConflictFields)
	}
	if plan.ConflictFields[0].Reason == "" {
		t.Error("ConflictFields[0].Reason must not be empty")
	}
}

func TestReconcileExisting_Suspended_ReturnsSuspendedAction(t *testing.T) {
	fake := &fakeCreateAPI{listFunc: fixedList(okListResp([]*larkeventv1.SubscriptionDetail{
		suspendedDetail("sub_1", "authority_revoked"),
	}, false, ""), nil)}

	plan, err := reconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != planActionSuspended {
		t.Errorf("Action = %q, want %q", plan.Action, planActionSuspended)
	}
}

func TestReconcileExisting_ExpiredOrDeleted_TreatedAsNew(t *testing.T) {
	for _, state := range []string{"expired", "deleted", "", "some_future_state"} {
		t.Run(state, func(t *testing.T) {
			fake := &fakeCreateAPI{listFunc: fixedList(okListResp([]*larkeventv1.SubscriptionDetail{
				{
					SubscriptionId: strPtr("sub_old"),
					EventType:      strPtr("im.message.created_v1"),
					TargetResource: strPtr("im.message?chat_id=oc_aaa"),
					Authority:      &larkeventv1.Authority{Type: strPtr("user"), OpenId: strPtr("ou_aaa")},
					State:          strPtr(state),
				},
			}, false, ""), nil)}

			plan, err := reconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if plan.Action != planActionCreate {
				t.Errorf("Action = %q, want %q (state %q treated as inert/not-found)", plan.Action, planActionCreate, state)
			}
		})
	}
}

func TestReconcileExisting_AuthorityMismatch_IgnoresOtherIdentityItems(t *testing.T) {
	// An "app" authority item must not be treated as a match for a "user"
	// identity's reconcile — never reuse/conflict against another identity's
	// subscription.
	fake := &fakeCreateAPI{listFunc: fixedList(okListResp([]*larkeventv1.SubscriptionDetail{
		activeDetail("sub_app", false, "app"),
	}, false, ""), nil)}

	plan, err := reconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != planActionCreate {
		t.Errorf("Action = %q, want %q (the only match belongs to a different authority)", plan.Action, planActionCreate)
	}
}

// TestReconcileExisting_BotIdentityMatchesAppAuthority_ReturnsReuseAction is
// the positive counterpart of the mismatch test above: a bot identity's own
// "app" authority item must be recognized as a match (the reuse/conflict
// classification is identity-symmetric, not just implemented for "user").
func TestReconcileExisting_BotIdentityMatchesAppAuthority_ReturnsReuseAction(t *testing.T) {
	fake := &fakeCreateAPI{listFunc: fixedList(okListResp([]*larkeventv1.SubscriptionDetail{
		activeDetail("sub_app", false, "app"),
	}, false, ""), nil)}

	plan, err := reconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsBot, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != planActionReuse {
		t.Errorf("Action = %q, want %q", plan.Action, planActionReuse)
	}
	if plan.Existing == nil || strVal(plan.Existing.SubscriptionId) != "sub_app" {
		t.Errorf("Existing = %+v, want sub_app", plan.Existing)
	}
}

func TestReconcileExisting_TransportError_PropagatesUnchanged(t *testing.T) {
	sentinel := errors.New("boom: connection reset")
	fake := &fakeCreateAPI{listFunc: fixedList(nil, sentinel)}

	_, err := reconcileExisting(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", core.AsUser, false)
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want it passed through unchanged (%v)", err, sentinel)
	}
}

// ---- createOrReuseSubscription ----

func TestCreateOrReuseSubscription_NotFound_CallsCreateExactlyOnce(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	fake := &fakeCreateAPI{
		listFunc: fixedList(okListResp(nil, false, ""), nil),
		createFunc: func() (*larkeventv1.CreateSubscriptionResp, error) {
			return okCreateResp(activeDetail("sub_new", false, "user")), nil
		},
	}

	outcome, err := createOrReuseSubscription(context.Background(), fake, resolved, core.AsUser, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Action != "created" {
		t.Errorf("Action = %q, want created", outcome.Action)
	}
	if fake.createCalls != 1 {
		t.Errorf("createCalls = %d, want 1", fake.createCalls)
	}
}

// TestCreateOrReuseSubscription_PaginationCapped_PropagatesToOutcome locks
// the #20 fix's wiring into `event subscription create`: when the reconcile
// List scan hits the page cap without finding a match, the resulting
// "created" outcome must carry PaginationCapped=true so runCreate can warn
// instead of silently treating the capped scan as a confirmed not-found.
func TestCreateOrReuseSubscription_PaginationCapped_PropagatesToOutcome(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	fake := &fakeCreateAPI{
		listFunc: func(call int) (*larkeventv1.ListSubscriptionResp, error) {
			// Every page: has_more=true, no matching authority item — an
			// unbounded scan would run forever.
			return okListResp([]*larkeventv1.SubscriptionDetail{
				activeDetail("sub_other", false, "app"), // "app" authority never matches AsUser
			}, true, fmt.Sprintf("token-%d", call)), nil
		},
		createFunc: func() (*larkeventv1.CreateSubscriptionResp, error) {
			return okCreateResp(activeDetail("sub_new", false, "user")), nil
		},
	}

	outcome, err := createOrReuseSubscription(context.Background(), fake, resolved, core.AsUser, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Action != "created" {
		t.Errorf("Action = %q, want created (a capped scan still defaults to create)", outcome.Action)
	}
	if !outcome.PaginationCapped {
		t.Error("PaginationCapped = false, want true")
	}
	if len(fake.listCalls) != eventlib.MaxSubscriptionListPages {
		t.Errorf("List called %d times, want exactly %d (bounded by the page cap)", len(fake.listCalls), eventlib.MaxSubscriptionListPages)
	}
}

func TestCreateOrReuseSubscription_ActiveCompatible_ReusesWithoutCallingCreate(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	fake := &fakeCreateAPI{
		listFunc: fixedList(okListResp([]*larkeventv1.SubscriptionDetail{
			activeDetail("sub_existing", false, "user"),
		}, false, ""), nil),
	}

	outcome, err := createOrReuseSubscription(context.Background(), fake, resolved, core.AsUser, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Action != "reused" {
		t.Errorf("Action = %q, want reused", outcome.Action)
	}
	if strVal(outcome.Detail.SubscriptionId) != "sub_existing" {
		t.Errorf("Detail.SubscriptionId = %q, want sub_existing", strVal(outcome.Detail.SubscriptionId))
	}
	if fake.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (idempotent reuse must never call Create)", fake.createCalls)
	}
}

func TestCreateOrReuseSubscription_ActiveConflict_ReturnsTypedFailedPrecondition(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	fake := &fakeCreateAPI{
		listFunc: fixedList(okListResp([]*larkeventv1.SubscriptionDetail{
			activeDetail("sub_conflict", true, "user"),
		}, false, ""), nil),
	}

	_, err := createOrReuseSubscription(context.Background(), fake, resolved, core.AsUser, false)
	if err == nil {
		t.Fatal("expected a conflict error, got nil")
	}
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	if ve.Param != "event_key" {
		t.Errorf("Param = %q, want event_key", ve.Param)
	}
	if len(ve.Params) != 1 || ve.Params[0].Name != "include_resource_data" {
		t.Errorf("Params = %+v, want one entry naming include_resource_data", ve.Params)
	}
	if !strings.Contains(ve.Hint, "sub_conflict") {
		t.Errorf("Hint = %q, want it to mention remote_subscription_id sub_conflict", ve.Hint)
	}
	if !strings.Contains(ve.Hint, "get") {
		t.Errorf("Hint = %q, want it to guide the caller to `get`", ve.Hint)
	}
	if fake.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (a conflict must never call Create)", fake.createCalls)
	}
}

func TestCreateOrReuseSubscription_Suspended_ReturnsTypedFailedPrecondition_GuidesReactivate(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	fake := &fakeCreateAPI{
		listFunc: fixedList(okListResp([]*larkeventv1.SubscriptionDetail{
			// Deliberately a different reason than the "authority_revoked"
			// used elsewhere in this file, so the assertion below on
			// ve.Error() proves suspendedError's `reason :=
			// strVal(...Suspension.Code)` extraction actually reads this
			// fixture's value rather than happening to match a hardcoded one.
			suspendedDetail("sub_susp", "identity_revoked"),
		}, false, ""), nil),
	}

	_, err := createOrReuseSubscription(context.Background(), fake, resolved, core.AsUser, false)
	if err == nil {
		t.Fatal("expected a suspended error, got nil")
	}
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	if !strings.Contains(ve.Hint, "reactivate") {
		t.Errorf("Hint = %q, want it to guide the caller to `reactivate`", ve.Hint)
	}
	if !strings.Contains(ve.Hint, "sub_susp") {
		t.Errorf("Hint = %q, want it to mention remote_subscription_id sub_susp", ve.Hint)
	}
	if !strings.Contains(ve.Error(), "identity_revoked") {
		t.Errorf("Error() = %q, want it to surface suspension_reason=identity_revoked from the fixture", ve.Error())
	}
	if fake.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0", fake.createCalls)
	}
}

func TestCreateOrReuseSubscription_CreateFails_ReconcileFindsCompatible_ReturnsReused(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	fake := &fakeCreateAPI{
		listFunc: func(call int) (*larkeventv1.ListSubscriptionResp, error) {
			if call == 1 {
				return okListResp(nil, false, ""), nil // first reconcile: nothing yet
			}
			// second reconcile (post-Create-failure): someone else's
			// concurrent Create already landed, and it is compatible.
			return okListResp([]*larkeventv1.SubscriptionDetail{
				activeDetail("sub_raced", false, "user"),
			}, false, ""), nil
		},
		createFunc: func() (*larkeventv1.CreateSubscriptionResp, error) {
			return nil, errors.New("duplicate")
		},
	}

	outcome, err := createOrReuseSubscription(context.Background(), fake, resolved, core.AsUser, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Action != "reused" {
		t.Errorf("Action = %q, want reused", outcome.Action)
	}
	if strVal(outcome.Detail.SubscriptionId) != "sub_raced" {
		t.Errorf("Detail.SubscriptionId = %q, want sub_raced", strVal(outcome.Detail.SubscriptionId))
	}
	if len(fake.listCalls) != 2 {
		t.Errorf("List call count = %d, want 2 (initial + post-failure reconcile)", len(fake.listCalls))
	}
	if fake.createCalls != 1 {
		t.Errorf("createCalls = %d, want 1 (no automatic re-Create after reconciling)", fake.createCalls)
	}
}

func TestCreateOrReuseSubscription_CreateFails_ReconcileFindsConflict_ReturnsTypedFail(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	fake := &fakeCreateAPI{
		listFunc: func(call int) (*larkeventv1.ListSubscriptionResp, error) {
			if call == 1 {
				return okListResp(nil, false, ""), nil
			}
			return okListResp([]*larkeventv1.SubscriptionDetail{
				activeDetail("sub_raced_conflict", true, "user"),
			}, false, ""), nil
		},
		createFunc: func() (*larkeventv1.CreateSubscriptionResp, error) {
			return nil, errors.New("duplicate")
		},
	}

	_, err := createOrReuseSubscription(context.Background(), fake, resolved, core.AsUser, false)
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	if !strings.Contains(ve.Hint, "sub_raced_conflict") {
		t.Errorf("Hint = %q, want it to mention sub_raced_conflict", ve.Hint)
	}
}

func TestCreateOrReuseSubscription_CreateFails_ReconcileFindsNothing_ReturnsOriginalError(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	sentinel := errors.New("boom: transport timeout")
	fake := &fakeCreateAPI{
		listFunc:   fixedList(okListResp(nil, false, ""), nil), // both reconciles find nothing
		createFunc: func() (*larkeventv1.CreateSubscriptionResp, error) { return nil, sentinel },
	}

	_, err := createOrReuseSubscription(context.Background(), fake, resolved, core.AsUser, false)
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want the original Create error passed through unchanged (%v)", err, sentinel)
	}
	if len(fake.listCalls) != 2 {
		t.Errorf("List call count = %d, want 2 (initial + one bounded reconcile-after-failure pass)", len(fake.listCalls))
	}
	if fake.createCalls != 1 {
		t.Errorf("createCalls = %d, want 1 (never auto-retry Create itself)", fake.createCalls)
	}
}

// ---- createRequiredScopes ----

func TestCreateRequiredScopes_False_ReturnsBaseMutationScopesOnly(t *testing.T) {
	got := createRequiredScopes(false)
	want := []string{"event:subscription:read", "event:subscription:write"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("createRequiredScopes(false) = %v, want %v", got, want)
	}
}

func TestCreateRequiredScopes_True_AlsoIncludesEncryptKeyReadScope(t *testing.T) {
	got := createRequiredScopes(true)
	want := map[string]bool{"event:subscription:read": true, "event:subscription:write": true, "event:encrypt_key:read": true}
	if len(got) != len(want) {
		t.Fatalf("createRequiredScopes(true) = %v, want exactly %v", got, want)
	}
	for _, s := range got {
		if !want[s] {
			t.Errorf("unexpected scope %q", s)
		}
	}
}

// TestCreateRequiredScopes_DoesNotMutateSharedBaseSlice guards against a
// classic Go append-aliasing bug: createRequiredScopes(true) must build a
// fresh slice rather than appending onto (and potentially reallocating
// into, or worse, overwriting) subscriptionMutationScopes's own backing
// array, which every OTHER caller of subscriptionMutationScopes (dry-run's
// own createDryRunResult default, other mutating subcommands) still relies
// on being exactly its original 2-element literal.
func TestCreateRequiredScopes_DoesNotMutateSharedBaseSlice(t *testing.T) {
	before := append([]string(nil), subscriptionMutationScopes...)
	_ = createRequiredScopes(true)
	if !reflect.DeepEqual(subscriptionMutationScopes, before) {
		t.Errorf("subscriptionMutationScopes mutated: got %v, want %v", subscriptionMutationScopes, before)
	}
}

// ---- dry-run must never generate a key ----

// TestDryRun_IncludeResourceDataTrue_NotFound_GeneratesNoKeyAndNoCreateCall
// drives the exact two calls runCreate's --dry-run branch makes
// (reconcileExisting then buildDryRunResult) with includeResourceData=true
// and asserts, via a spy substituted for the package-level key generator,
// that it is invoked exactly zero times — even though the plan lands on
// planActionCreate, the one row a REAL (non-dry-run) run would generate a
// key for.
func TestDryRun_IncludeResourceDataTrue_NotFound_GeneratesNoKeyAndNoCreateCall(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	fake := &fakeCreateAPI{listFunc: fixedList(okListResp(nil, false, ""), nil)}

	keyGenCalls := 0
	orig := newEncryptKeyFunc
	newEncryptKeyFunc = func() (string, error) { keyGenCalls++; return orig() }
	t.Cleanup(func() { newEncryptKeyFunc = orig })

	plan, err := reconcileExisting(context.Background(), fake, resolved.Definition.EventType, resolved.TargetResource, core.AsBot, true)
	if err != nil {
		t.Fatalf("reconcileExisting: unexpected error: %v", err)
	}
	if plan.Action != planActionCreate {
		t.Fatalf("Action = %q, want %q", plan.Action, planActionCreate)
	}
	result := buildDryRunResult(resolved, core.AsBot, plan, true)
	if result.PlannedChange.Action != planActionCreate {
		t.Errorf("PlannedChange.Action = %q, want %q", result.PlannedChange.Action, planActionCreate)
	}

	if keyGenCalls != 0 {
		t.Errorf("key generator invoked %d times during --dry-run, want 0", keyGenCalls)
	}
	if fake.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0: --dry-run must never issue a write", fake.createCalls)
	}
}

// TestDryRun_IncludeResourceDataTrue_ActiveMatch_ProbesButGeneratesNoKey
// covers the OTHER row a --dry-run preview can reach for an encrypted
// request: an existing active, include_resource_data=true match.
// --dry-run's plan step MAY probe remote state
// (GetEncryptKey, to report whether the plan would be reuse or conflict)
// but must still never generate a NEW key — this distinguishes "probing an
// existing key" (fine, informational) from "creating a new one" (never
// during --dry-run).
func TestDryRun_IncludeResourceDataTrue_ActiveMatch_ProbesButGeneratesNoKey(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	fake := &fakeCreateAPI{
		listFunc: fixedList(okListResp([]*larkeventv1.SubscriptionDetail{
			activeDetail("sub_enc", true, "app"), // "app" authority matches core.AsBot identity
		}, false, ""), nil),
		getEncryptKeyFunc: func() (*larkeventv1.GetEncryptKeySubscriptionResp, error) {
			return okEncryptKeyResp("usable-key-from-remote"), nil
		},
	}

	keyGenCalls := 0
	orig := newEncryptKeyFunc
	newEncryptKeyFunc = func() (string, error) { keyGenCalls++; return orig() }
	t.Cleanup(func() { newEncryptKeyFunc = orig })

	plan, err := reconcileExisting(context.Background(), fake, resolved.Definition.EventType, resolved.TargetResource, core.AsBot, true)
	if err != nil {
		t.Fatalf("reconcileExisting: unexpected error: %v", err)
	}
	if plan.Action != planActionReuse {
		t.Fatalf("Action = %q, want %q (usable remote key -> reuse)", plan.Action, planActionReuse)
	}
	_ = buildDryRunResult(resolved, core.AsBot, plan, true)

	if fake.getEncryptKeyCalls != 1 {
		t.Errorf("getEncryptKeyCalls = %d, want 1: dry-run's plan step may probe remote state", fake.getEncryptKeyCalls)
	}
	if keyGenCalls != 0 {
		t.Errorf("key generator invoked %d times during --dry-run, want 0: probing an existing key must never generate a new one", keyGenCalls)
	}
	if fake.createCalls != 0 {
		t.Error("createCalls must be 0 for --dry-run")
	}
}

// ---- encrypted create (--include-resource-data=true: atomic Create-time
// key injection, the encryption conflict matrix, and key redaction) ----

// TestBuildCreateSubscriptionBody_IncludeResourceDataTrueWithKey_SetsBothAtomically
// is the direct, structural proof of the atomicity requirement:
// includeResourceData and a non-empty encryptKey are always set on the SAME
// returned body value from ONE function call — there is no code path that
// could build/send them as two separate requests. See
// buildCreateSubscriptionBody's own doc comment for why this must be
// asserted against ITS return value rather than a captured
// *larkeventv1.CreateSubscriptionReq (the SDK request wrapper's Body field
// is never actually populated by its own builder — verified while writing
// this test).
func TestBuildCreateSubscriptionBody_IncludeResourceDataTrueWithKey_SetsBothAtomically(t *testing.T) {
	body := buildCreateSubscriptionBody("im.message.created_v1", "im.message?chat_id=oc_aaa", true, "the-generated-key")

	if body.EventType == nil || *body.EventType != "im.message.created_v1" {
		t.Errorf("EventType = %v, want im.message.created_v1", body.EventType)
	}
	if body.TargetResource == nil || *body.TargetResource != "im.message?chat_id=oc_aaa" {
		t.Errorf("TargetResource = %v, want im.message?chat_id=oc_aaa", body.TargetResource)
	}
	if body.PayloadOptions == nil {
		t.Fatal("PayloadOptions = nil, want set")
	}
	if !boolVal(body.PayloadOptions.IncludeResourceData) {
		t.Error("PayloadOptions.IncludeResourceData = false, want true")
	}
	if body.PayloadOptions.Encrypt == nil || strVal(body.PayloadOptions.Encrypt.EncryptKey) != "the-generated-key" {
		t.Fatalf("PayloadOptions.Encrypt = %+v, want EncryptKey=the-generated-key", body.PayloadOptions.Encrypt)
	}
}

// TestBuildCreateSubscriptionBody_EmptyKey_OmitsEncrypt locks the
// includeResourceData=false path (and any defensive empty-key call): Encrypt
// must be left nil, never an empty-but-present struct — mirroring how
// PayloadOptionsEncrypt is Create-only and must not appear at all when there
// is no key.
func TestBuildCreateSubscriptionBody_EmptyKey_OmitsEncrypt(t *testing.T) {
	body := buildCreateSubscriptionBody("im.message.created_v1", "im.message?chat_id=oc_aaa", false, "")

	if boolVal(body.PayloadOptions.IncludeResourceData) {
		t.Error("PayloadOptions.IncludeResourceData = true, want false")
	}
	if body.PayloadOptions.Encrypt != nil {
		t.Errorf("PayloadOptions.Encrypt = %+v, want nil when no key is supplied", body.PayloadOptions.Encrypt)
	}
}

// TestCreateOrReuseSubscription_Encrypted_NotFound_CreatesAtomicallyWithKey
// is the primary TDD case at createOrReuseSubscription's own level: no
// existing match -> generate a key -> Create exactly once (never a separate
// "create plain" + "add encryption" pair of calls) -> return the resulting
// subscription_id. See TestBuildCreateSubscriptionBody_* above for the
// direct proof of what that one call's body actually contains.
func TestCreateOrReuseSubscription_Encrypted_NotFound_CreatesAtomicallyWithKey(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	fake := &fakeCreateAPI{
		listFunc: fixedList(okListResp(nil, false, ""), nil),
		createFunc: func() (*larkeventv1.CreateSubscriptionResp, error) {
			return okCreateResp(activeDetail("sub_encrypted_new", true, "app")), nil
		},
	}

	outcome, err := createOrReuseSubscription(context.Background(), fake, resolved, core.AsBot, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Action != "created" {
		t.Errorf("Action = %q, want created", outcome.Action)
	}
	if strVal(outcome.Detail.SubscriptionId) != "sub_encrypted_new" {
		t.Errorf("Detail.SubscriptionId = %q, want sub_encrypted_new", strVal(outcome.Detail.SubscriptionId))
	}
	if fake.createCalls != 1 {
		t.Errorf("createCalls = %d, want exactly 1 (one atomic Create)", fake.createCalls)
	}
}

// TestCreateOrReuseSubscription_Encrypted_RemoteFalse_ReturnsConflict locks
// the conflict-matrix row "encrypted request vs existing include_resource_data=false
// -> conflict, human decision" via createOrReuseSubscription (create.go's own typed-error layer, not
// just the lower-level ReconcileExisting already locked in
// internal/event/reconcile_test.go).
func TestCreateOrReuseSubscription_Encrypted_RemoteFalse_ReturnsConflict(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	fake := &fakeCreateAPI{
		listFunc: fixedList(okListResp([]*larkeventv1.SubscriptionDetail{
			activeDetail("sub_plain", false, "app"), // "app" authority matches core.AsBot identity
		}, false, ""), nil),
	}

	_, err := createOrReuseSubscription(context.Background(), fake, resolved, core.AsBot, true)
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	if fake.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (a conflict must never call Create)", fake.createCalls)
	}
	if fake.getEncryptKeyCalls != 0 {
		t.Errorf("getEncryptKeyCalls = %d, want 0: the probe must never run when the state mismatch is already decisive", fake.getEncryptKeyCalls)
	}
}

// TestCreateOrReuseSubscription_Encrypted_RemoteTrueUsableKey_ReusesNoNewCreate
// locks the conflict-matrix reuse row: an
// active, include_resource_data=true match whose key GetEncryptKey confirms
// usable is reused as-is — never a new Create, never a new key.
func TestCreateOrReuseSubscription_Encrypted_RemoteTrueUsableKey_ReusesNoNewCreate(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	fake := &fakeCreateAPI{
		listFunc: fixedList(okListResp([]*larkeventv1.SubscriptionDetail{
			activeDetail("sub_already_encrypted", true, "app"), // "app" authority matches core.AsBot identity
		}, false, ""), nil),
		getEncryptKeyFunc: func() (*larkeventv1.GetEncryptKeySubscriptionResp, error) {
			return okEncryptKeyResp("usable-remote-key"), nil
		},
	}

	keyGenCalls := 0
	orig := newEncryptKeyFunc
	newEncryptKeyFunc = func() (string, error) { keyGenCalls++; return orig() }
	t.Cleanup(func() { newEncryptKeyFunc = orig })

	outcome, err := createOrReuseSubscription(context.Background(), fake, resolved, core.AsBot, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Action != "reused" {
		t.Errorf("Action = %q, want reused", outcome.Action)
	}
	if strVal(outcome.Detail.SubscriptionId) != "sub_already_encrypted" {
		t.Errorf("Detail.SubscriptionId = %q, want sub_already_encrypted", strVal(outcome.Detail.SubscriptionId))
	}
	if fake.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (reuse must never call Create)", fake.createCalls)
	}
	if keyGenCalls != 0 {
		t.Errorf("key generator invoked %d times, want 0: reusing an existing encrypted match must never generate a new key", keyGenCalls)
	}
}

// TestCreateOrReuseSubscription_Encrypted_RemoteTrueKeyUnavailable_ReturnsConflictHumanHint
// locks the conflict-matrix row for an encrypted match whose key is NOT
// retrievable: GetEncryptKey returning no usable key (here: a business
// error, e.g. missing scope/permission at the remote side) must conflict —
// with human-actionable guidance (delete+recreate / verify the subscription
// / check event:encrypt_key:read), never a silent reuse.
func TestCreateOrReuseSubscription_Encrypted_RemoteTrueKeyUnavailable_ReturnsConflictHumanHint(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	fake := &fakeCreateAPI{
		listFunc: fixedList(okListResp([]*larkeventv1.SubscriptionDetail{
			activeDetail("sub_key_unavailable", true, "app"), // "app" authority matches core.AsBot identity
		}, false, ""), nil),
		getEncryptKeyFunc: func() (*larkeventv1.GetEncryptKeySubscriptionResp, error) {
			return nil, errors.New("boom: synthetic permission failure")
		},
	}

	_, err := createOrReuseSubscription(context.Background(), fake, resolved, core.AsBot, true)
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	if !strings.Contains(ve.Hint, "sub_key_unavailable") {
		t.Errorf("Hint = %q, want it to mention remote_subscription_id sub_key_unavailable", ve.Hint)
	}
	if !strings.Contains(ve.Hint, "event:encrypt_key:read") {
		t.Errorf("Hint = %q, want it to mention scope event:encrypt_key:read", ve.Hint)
	}
	if !strings.Contains(ve.Hint, "delete") {
		t.Errorf("Hint = %q, want it to guide delete+recreate", ve.Hint)
	}
	if fake.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (a conflict must never call Create)", fake.createCalls)
	}
}

// TestCreateOrReuseSubscription_Encrypted_CreateFails_SecondReconcileStillRequiresEncryption
// locks the fail-closed invariant: "if create fails, do NOT retry without
// encryption / do NOT fall back to a plaintext sub." The post-failure
// bounded reconcile-and-retry pass (createOrReuseSubscription's own,
// unrelated-to-encryption "List raced us" recovery) must still request
// includeResourceData=true on its second reconcile — proven here by racing
// in a PLAINTEXT match on that second List: if the implementation ever
// silently downgraded to includeResourceData=false after an encrypted
// Create failure, this plaintext match would wrongly look like a compatible
// reuse instead of a conflict.
func TestCreateOrReuseSubscription_Encrypted_CreateFails_SecondReconcileStillRequiresEncryption(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	fake := &fakeCreateAPI{
		listFunc: func(call int) (*larkeventv1.ListSubscriptionResp, error) {
			if call == 1 {
				return okListResp(nil, false, ""), nil
			}
			return okListResp([]*larkeventv1.SubscriptionDetail{
				activeDetail("sub_raced_plaintext", false, "user"),
			}, false, ""), nil
		},
		createFunc: func() (*larkeventv1.CreateSubscriptionResp, error) {
			return nil, errors.New("boom: synthetic create failure")
		},
	}

	_, err := createOrReuseSubscription(context.Background(), fake, resolved, core.AsUser, true)
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError (conflict, not a silent plaintext fallback), got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	if fake.createCalls != 1 {
		t.Errorf("createCalls = %d, want 1 (never retry Create, encrypted or otherwise)", fake.createCalls)
	}
}

// ---- key redaction: the generated key must NEVER appear in stdout,
// stderr, the --json output, the dry-run plan preview, logs, or any
// error/Hint ----

// TestEncryptedCreate_Redaction_KeyNeverAppearsInAnyOutput captures every
// caller-visible output surface an encrypted, successful create produces —
// the --json result (json.Marshal of buildCreateResult's return value) and
// the human-text result (writeCreateText) — and asserts the actual
// generated key (captured directly via a spy on the package-level
// generator, i.e. ground truth, not a guess) is byte-for-byte absent from
// both. The control that this key really was generated and used for this
// exact create attempt (so the negative assertions below are not vacuous)
// is TestBuildCreateSubscriptionBody_IncludeResourceDataTrueWithKey_SetsBothAtomically
// plus this test's own createCalls==1 check: doCreateSubscription always
// builds its request body via buildCreateSubscriptionBody(..., encryptKey)
// with exactly the value newEncryptKeyFunc returned (createOrReuseSubscription
// threads it straight through with no intermediate transformation).
func TestEncryptedCreate_Redaction_KeyNeverAppearsInAnyOutput(t *testing.T) {
	resolved := resolveCreatedChatID(t)

	var capturedKey string
	orig := newEncryptKeyFunc
	newEncryptKeyFunc = func() (string, error) {
		k, err := orig()
		capturedKey = k
		return k, err
	}
	t.Cleanup(func() { newEncryptKeyFunc = orig })

	fake := &fakeCreateAPI{listFunc: fixedList(okListResp(nil, false, ""), nil)}

	outcome, err := createOrReuseSubscription(context.Background(), fake, resolved, core.AsBot, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedKey == "" {
		t.Fatal("test setup issue: no key was captured, cannot verify redaction")
	}
	if fake.createCalls != 1 {
		t.Fatalf("createCalls = %d, want exactly 1 (control: the generated key must have actually been used for a real create attempt)", fake.createCalls)
	}

	result := buildCreateResult(resolved, core.AsBot, outcome)

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if strings.Contains(string(jsonBytes), capturedKey) {
		t.Errorf("REDACTION VIOLATION: encrypt_key found in --json output: %s", jsonBytes)
	}

	var textBuf bytes.Buffer
	writeCreateText(&textBuf, result)
	if strings.Contains(textBuf.String(), capturedKey) {
		t.Errorf("REDACTION VIOLATION: encrypt_key found in human-text output: %s", textBuf.String())
	}

	// Also cover the dry-run preview shape (a plan built from the same
	// createOutcome-adjacent reconcilePlan) for completeness, even though
	// this particular call path (a completed, non-dry-run create) does not
	// itself render one.
	plan := &reconcilePlan{Action: planActionCreate}
	dryRunResult := buildDryRunResult(resolved, core.AsBot, plan, true)
	dryRunJSON, err := json.Marshal(dryRunResult)
	if err != nil {
		t.Fatalf("json.Marshal dry-run result: %v", err)
	}
	if strings.Contains(string(dryRunJSON), capturedKey) {
		t.Errorf("REDACTION VIOLATION: encrypt_key found in dry-run plan preview: %s", dryRunJSON)
	}
}

// TestEncryptedCreate_Redaction_KeyNeverAppearsInErrorOnCreateFailure covers
// the error path: even when the atomic Create call itself fails, the
// resulting typed error's Error()/Hint must never contain the key that was
// generated and submitted for that attempt.
func TestEncryptedCreate_Redaction_KeyNeverAppearsInErrorOnCreateFailure(t *testing.T) {
	resolved := resolveCreatedChatID(t)

	var capturedKey string
	orig := newEncryptKeyFunc
	newEncryptKeyFunc = func() (string, error) {
		k, err := orig()
		capturedKey = k
		return k, err
	}
	t.Cleanup(func() { newEncryptKeyFunc = orig })

	fake := &fakeCreateAPI{
		listFunc: fixedList(okListResp(nil, false, ""), nil), // both reconciles: nothing found
		createFunc: func() (*larkeventv1.CreateSubscriptionResp, error) {
			return nil, errors.New("boom: synthetic create failure")
		},
	}

	_, err := createOrReuseSubscription(context.Background(), fake, resolved, core.AsBot, true)
	if err == nil {
		t.Fatal("expected an error")
	}
	if capturedKey == "" {
		t.Fatal("test setup issue: no key was captured, cannot verify redaction")
	}
	if strings.Contains(err.Error(), capturedKey) {
		t.Errorf("REDACTION VIOLATION: encrypt_key found in error message: %v", err)
	}
}

// TestDoCreateSubscription_IncludeResourceDataTrueWithEmptyKey_FailsClosed
// is a defense-in-depth structural guard: doCreateSubscription itself must
// refuse to submit include_resource_data=true with an empty encryptKey,
// rather than ever letting an "encrypted intent, but actually unencrypted"
// Create request reach the platform. Every real caller
// (createOrReuseSubscription) already generates a key before reaching here
// whenever includeResourceData is true, and returns its own error early if
// generation fails — so this is unreachable via the production call graph
// today, but locks the invariant against future modification (e.g. a new
// caller that forgets to generate a key first).
func TestDoCreateSubscription_IncludeResourceDataTrueWithEmptyKey_FailsClosed(t *testing.T) {
	fake := &fakeCreateAPI{}

	_, err := doCreateSubscription(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", true, "")
	if err == nil {
		t.Fatal("expected an error (fail-closed, no plaintext-fallback), got nil")
	}
	if fake.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0: must refuse before ever calling Create", fake.createCalls)
	}
}

// ---- checkTemplateAuthTypes ----

func TestCheckTemplateAuthTypes_OwnerMeTemplate_RejectsBot(t *testing.T) {
	resolved := resolveCreatedOwnerMe(t)

	err := checkTemplateAuthTypes(core.AsBot, resolved)
	if err == nil {
		t.Fatal("expected an error for --as bot on the user-only owner/me template, got nil")
	}
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	if ve.Param != "--as" {
		t.Errorf("Param = %q, want --as", ve.Param)
	}
	if !strings.Contains(ve.Hint, "user") {
		t.Errorf("Hint = %q, want it to name the allowed identity (user)", ve.Hint)
	}
}

func TestCheckTemplateAuthTypes_OwnerMeTemplate_AllowsUser(t *testing.T) {
	resolved := resolveCreatedOwnerMe(t)

	if err := checkTemplateAuthTypes(core.AsUser, resolved); err != nil {
		t.Errorf("unexpected error for --as user on owner/me: %v", err)
	}
}

func TestCheckTemplateAuthTypes_ChatIDTemplate_AllowsBothIdentities(t *testing.T) {
	resolved := resolveCreatedChatID(t)

	for _, id := range []core.Identity{core.AsUser, core.AsBot} {
		if err := checkTemplateAuthTypes(id, resolved); err != nil {
			t.Errorf("--as %s: unexpected error on chat-id template: %v", id, err)
		}
	}
}

// ---- runCreate wiring (cobra-level; every case below must short-circuit
// before any network-capable client is built) ----

func TestRunCreate_BareRefinedBaseKey_ReturnsR1InvalidArgumentUnchanged(t *testing.T) {
	registerCreateFixtures(t)
	f := &cmdutil.Factory{}
	cmd := NewCmdCreate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"im.message.created_v1"})

	err := cmd.Execute()
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeInvalidArgument {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeInvalidArgument)
	}
	if !strings.Contains(ve.Hint, "event schema") {
		t.Errorf("Hint = %q, want it to point at `event schema`", ve.Hint)
	}
}

func TestRunCreate_UnknownBaseKey_ReturnsInvalidArgument(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdCreate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"totally.unknown.key_v1"})

	err := cmd.Execute()
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeInvalidArgument {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeInvalidArgument)
	}
}

func TestRunCreate_LegacyKey_RejectedAsNotRefined(t *testing.T) {
	registerCreateFixtures(t)
	f := &cmdutil.Factory{}
	cmd := NewCmdCreate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"im.message.receive_v1"})

	err := cmd.Execute()
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeInvalidArgument {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeInvalidArgument)
	}
	if ve.Param != "event_key" {
		t.Errorf("Param = %q, want event_key", ve.Param)
	}
}

func TestRunCreate_AsBotOnOwnerMeTemplate_RejectedByTemplateAuthTypes(t *testing.T) {
	registerCreateFixtures(t)
	f := &cmdutil.Factory{}
	cmd := NewCmdCreate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"im.message.created_v1/owner/me", "--as", "bot"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected an error for --as bot on owner/me, got nil")
	}
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	if ve.Param != "--as" {
		t.Errorf("Param = %q, want --as", ve.Param)
	}
}

// TestRunCreate_KeyLevelAuthTypesRejectsBeforeTemplateCheck locks tier 1:
// a refined key whose AuthTypes as a whole excludes the
// resolved identity must reject with invalid_argument (not
// failed_precondition — that is tier 2's, template-specific, error).
func TestRunCreate_KeyLevelAuthTypesRejectsBeforeTemplateCheck(t *testing.T) {
	eventlib.RegisterKey(eventlib.KeyDefinition{
		Key:                 "test.bot_only_refined_v1",
		EventType:           "test.bot_only_refined_v1",
		Schema:              eventlib.SchemaDef{Custom: &eventlib.SchemaSpec{Raw: []byte(`{"type":"object"}`)}},
		RefinedSubscription: true,
		ResourceType:        "test.resource",
		AuthTypes:           []string{"bot"},
		KeyTemplates: []eventlib.KeyTemplate{{
			Template:    "test.bot_only_refined_v1/id/{id}",
			Example:     "test.bot_only_refined_v1/id/abc",
			SelectorKey: "id",
			PathSegment: "id",
			AuthTypes:   []string{"bot"},
		}},
	})
	t.Cleanup(func() { eventlib.UnregisterKeyForTest("test.bot_only_refined_v1") })

	f := &cmdutil.Factory{}
	cmd := NewCmdCreate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"test.bot_only_refined_v1/id/abc", "--as", "user"})

	err := cmd.Execute()
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeInvalidArgument {
		t.Errorf("Subtype = %s, want %s (key-level AuthTypes rejection, tier 1)", ve.Subtype, errs.SubtypeInvalidArgument)
	}
}

// An earlier test locked a deferral gate that rejected
// --include-resource-data=true. That gate is gone — encrypted create is
// implemented instead;
// TestRunCreate_IncludeResourceDataTrue_MissingEncryptKeyReadScope_ReturnsPermissionError
// and the createOrReuseSubscription-level tests below
// (TestCreateOrReuseSubscription_Encrypted_*) are its replacement.

// TestRunCreate_IncludeResourceDataFalse_DoesNotRequireEncryptKeyReadScope
// locks that --include-resource-data=false (the default) never pulls in the
// extra event:encrypt_key:read scope requirement added for =true:
// with ONLY the base mutation scopes granted (deliberately omitting
// event:encrypt_key:read), the flow must proceed past scope preflight into
// the real reconcile List call — which then fails with an httpmock "no stub
// registered" error (no stub is registered here on purpose, mirroring the
// existing --include-resource-data=false convention in this file). That
// failure is expected and irrelevant to this test: the only thing asserted
// is that the failure is NOT a *errs.PermissionError naming
// event:encrypt_key:read.
func TestRunCreate_IncludeResourceDataFalse_DoesNotRequireEncryptKeyReadScope(t *testing.T) {
	registerCreateFixtures(t)
	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "cli_x"})
	f.Credential = credential.NewCredentialProvider(nil, nil, &fakeTokenResolver{
		result: &credential.TokenResult{Token: "t-tok", Scopes: "event:subscription:read event:subscription:write"},
	}, nil)

	cmd := NewCmdCreate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"im.message.created_v1/chat-id/oc_aaa", "--as", "bot", "--include-resource-data=false"})

	err := cmd.Execute()
	var permErr *errs.PermissionError
	if errors.As(err, &permErr) {
		t.Fatalf("--include-resource-data=false must not require event:encrypt_key:read, got: %v", err)
	}
}

// TestRunCreate_IncludeResourceDataTrue_MissingEncryptKeyReadScope_ReturnsPermissionError
// locks the scope wiring: --include-resource-data=true additionally
// requires event:encrypt_key:read (create's reconcile probes GetEncryptKey
// for an active include_resource_data=true match) on top of the usual
// event:subscription:{read,write} — missing it alone (both mutation scopes
// otherwise granted) must fail closed with a typed permission error naming
// it, never silently proceed without the probe capability.
func TestRunCreate_IncludeResourceDataTrue_MissingEncryptKeyReadScope_ReturnsPermissionError(t *testing.T) {
	registerCreateFixtures(t)
	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "cli_x"})
	f.Credential = credential.NewCredentialProvider(nil, nil, &fakeTokenResolver{
		result: &credential.TokenResult{Token: "t-tok", Scopes: "event:subscription:read event:subscription:write"},
	}, nil)

	cmd := NewCmdCreate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	// --as user: resource data is user-only, so the scope preflight (not the
	// user-only gate) is what must fire here.
	cmd.SetArgs([]string{"im.message.created_v1/chat-id/oc_aaa", "--as", "user", "--include-resource-data=true"})

	err := cmd.Execute()
	var permErr *errs.PermissionError
	if !errors.As(err, &permErr) {
		t.Fatalf("expected *errs.PermissionError, got %T: %v", err, err)
	}
	if len(permErr.MissingScopes) != 1 || permErr.MissingScopes[0] != "event:encrypt_key:read" {
		t.Errorf("MissingScopes = %v, want [event:encrypt_key:read]", permErr.MissingScopes)
	}
}

// TestRunCreate_IncludeResourceDataTrue_AllScopesGranted_PassesScopePreflight
// is --include-resource-data=true's counterpart of
// TestRunCreate_IncludeResourceDataFalse_DoesNotRequireEncryptKeyReadScope:
// with ALL THREE scopes granted (including event:encrypt_key:read), the
// flow must proceed past scope preflight into the real reconcile List call
// (which then fails with an expected, unregistered httpmock stub — the only
// thing asserted here is that it is NOT a *errs.PermissionError).
func TestRunCreate_IncludeResourceDataTrue_AllScopesGranted_PassesScopePreflight(t *testing.T) {
	registerCreateFixtures(t)
	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "cli_x"})
	f.Credential = credential.NewCredentialProvider(nil, nil, &fakeTokenResolver{
		result: &credential.TokenResult{Token: "t-tok", Scopes: "event:subscription:read event:subscription:write event:encrypt_key:read"},
	}, nil)

	cmd := NewCmdCreate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	// --as user: resource data is user-only, so the encrypted create path is
	// exercised as a user (the user-only gate passes; scope preflight passes too).
	cmd.SetArgs([]string{"im.message.created_v1/chat-id/oc_aaa", "--as", "user", "--include-resource-data=true"})

	err := cmd.Execute()
	var permErr *errs.PermissionError
	if errors.As(err, &permErr) {
		t.Fatalf("all three scopes were granted, must not report a permission error, got: %v", err)
	}
}

// TestRunCreate_IncludeResourceDataTrue_AsBot_RejectedRequiresUser locks #17:
// resource data is a user-only capability, so --include-resource-data=true with
// --as bot is a typed invalid_argument fired BEFORE any remote call or scope
// preflight (a bare Factory suffices — the gate runs before credentials are
// touched, mirroring TestRunCreate_AsBotOnOwnerMeTemplate).
func TestRunCreate_IncludeResourceDataTrue_AsBot_RejectedRequiresUser(t *testing.T) {
	registerCreateFixtures(t)
	f := &cmdutil.Factory{}
	cmd := NewCmdCreate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"im.message.created_v1/chat-id/oc_aaa", "--as", "bot", "--include-resource-data=true"})

	err := cmd.Execute()
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeInvalidArgument {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeInvalidArgument)
	}
	if ve.Param != "--include-resource-data" {
		t.Errorf("Param = %q, want --include-resource-data", ve.Param)
	}
	if !strings.Contains(ve.Message, "--as user") {
		t.Errorf("Message = %q, want it to require --as user", ve.Message)
	}
}

// TestRunCreate_MissingReadScope_ReturnsPermissionError and
// TestRunCreate_MissingWriteScope_ReturnsPermissionError together lock
// create's key differentiator from list/get: create hard-requires BOTH
// event:subscription:read AND event:subscription:write — missing EITHER one
// alone must still fail closed (read does not imply write, or vice versa).
func TestRunCreate_MissingReadScope_ReturnsPermissionError(t *testing.T) {
	registerCreateFixtures(t)
	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "cli_x"})
	f.Credential = credential.NewCredentialProvider(nil, nil, &fakeTokenResolver{
		result: &credential.TokenResult{Token: "u-tok", Scopes: "event:subscription:write"},
	}, nil)

	cmd := NewCmdCreate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"im.message.created_v1/chat-id/oc_aaa", "--as", "user"})

	err := cmd.Execute()
	var permErr *errs.PermissionError
	if !errors.As(err, &permErr) {
		t.Fatalf("expected *errs.PermissionError, got %T: %v", err, err)
	}
	if len(permErr.MissingScopes) != 1 || permErr.MissingScopes[0] != "event:subscription:read" {
		t.Errorf("MissingScopes = %v, want [event:subscription:read]", permErr.MissingScopes)
	}
}

func TestRunCreate_MissingWriteScope_ReturnsPermissionError(t *testing.T) {
	registerCreateFixtures(t)
	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "cli_x"})
	f.Credential = credential.NewCredentialProvider(nil, nil, &fakeTokenResolver{
		result: &credential.TokenResult{Token: "u-tok", Scopes: "event:subscription:read"},
	}, nil)

	cmd := NewCmdCreate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"im.message.created_v1/chat-id/oc_aaa", "--as", "user"})

	err := cmd.Execute()
	var permErr *errs.PermissionError
	if !errors.As(err, &permErr) {
		t.Fatalf("expected *errs.PermissionError, got %T: %v", err, err)
	}
	if len(permErr.MissingScopes) != 1 || permErr.MissingScopes[0] != "event:subscription:write" {
		t.Errorf("MissingScopes = %v, want [event:subscription:write]", permErr.MissingScopes)
	}
}

func TestRunCreate_MissingBothScopes_ReturnsPermissionErrorListingBoth(t *testing.T) {
	registerCreateFixtures(t)
	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "cli_x"})
	f.Credential = credential.NewCredentialProvider(nil, nil, &fakeTokenResolver{
		result: &credential.TokenResult{Token: "u-tok", Scopes: "im:message:send"},
	}, nil)

	cmd := NewCmdCreate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"im.message.created_v1/chat-id/oc_aaa", "--as", "user"})

	err := cmd.Execute()
	var permErr *errs.PermissionError
	if !errors.As(err, &permErr) {
		t.Fatalf("expected *errs.PermissionError, got %T: %v", err, err)
	}
	want := map[string]bool{"event:subscription:read": true, "event:subscription:write": true}
	if len(permErr.MissingScopes) != len(want) {
		t.Fatalf("MissingScopes = %v, want both %v", permErr.MissingScopes, want)
	}
	for _, s := range permErr.MissingScopes {
		if !want[s] {
			t.Errorf("unexpected missing scope %q", s)
		}
	}
}

// ---- dry-run output shape ----

func TestBuildDryRunResult_NotFound_ShapeAndNoRemoteBefore(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	plan := &reconcilePlan{Action: planActionCreate}

	result := buildDryRunResult(resolved, core.AsUser, plan, false)

	if result.Operation != "create" {
		t.Errorf("Operation = %q, want create", result.Operation)
	}
	if !result.DryRun {
		t.Error("DryRun = false, want true")
	}
	if result.EventKey != resolved.MaterializedKey {
		t.Errorf("EventKey = %q, want %q", result.EventKey, resolved.MaterializedKey)
	}
	if result.TargetResource != resolved.TargetResource {
		t.Errorf("TargetResource = %q, want %q", result.TargetResource, resolved.TargetResource)
	}
	if len(result.RequiredScopes) != 2 {
		t.Errorf("RequiredScopes = %v, want both read+write scopes", result.RequiredScopes)
	}
	if result.RemoteBefore != nil {
		t.Errorf("RemoteBefore = %+v, want nil when nothing was found", result.RemoteBefore)
	}
	if result.PlannedChange.Action != planActionCreate {
		t.Errorf("PlannedChange.Action = %q, want %q", result.PlannedChange.Action, planActionCreate)
	}
	if result.LocalImpact.LocalConsumerAffected {
		t.Error("LocalImpact.LocalConsumerAffected = true, want false: create never touches a local consumer")
	}
	if result.NextAction == "" {
		t.Error("NextAction must not be empty")
	}
}

func TestBuildDryRunResult_ActiveCompatible_RemoteBeforePopulated(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	existing := activeDetail("sub_existing", false, "user")
	plan := &reconcilePlan{Action: planActionReuse, Existing: existing}

	result := buildDryRunResult(resolved, core.AsUser, plan, false)

	if result.RemoteBefore == nil {
		t.Fatal("RemoteBefore = nil, want the existing subscription row")
	}
	if result.RemoteBefore.RemoteSubscriptionID != "sub_existing" {
		t.Errorf("RemoteBefore.RemoteSubscriptionID = %q, want sub_existing", result.RemoteBefore.RemoteSubscriptionID)
	}
	if result.PlannedChange.RemoteSubscriptionID != "sub_existing" {
		t.Errorf("PlannedChange.RemoteSubscriptionID = %q, want sub_existing", result.PlannedChange.RemoteSubscriptionID)
	}
}

// TestBuildDryRunResult_IncludeResourceDataTrue_RequiredScopesIncludesEncryptKeyRead
// locks that dry-run's own reported required_scopes accurately reflects
// the conditional third scope: a --dry-run preview must never claim a
// smaller scope requirement than the real run it is previewing actually
// checked (both share the same createRequiredScopes call in runCreate).
func TestBuildDryRunResult_IncludeResourceDataTrue_RequiredScopesIncludesEncryptKeyRead(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	plan := &reconcilePlan{Action: planActionCreate}

	result := buildDryRunResult(resolved, core.AsUser, plan, true)

	want := map[string]bool{"event:subscription:read": true, "event:subscription:write": true, "event:encrypt_key:read": true}
	if len(result.RequiredScopes) != len(want) {
		t.Fatalf("RequiredScopes = %v, want exactly %v", result.RequiredScopes, want)
	}
	for _, s := range result.RequiredScopes {
		if !want[s] {
			t.Errorf("unexpected required scope %q", s)
		}
	}
}

// TestDryRun_NotFound_EndToEndViaFakeService_JSONShapeAndNoCreateCall is the
// primary TDD case from the task brief: `create <refined key> --dry-run`
// must produce the full dry-run JSON shape and must NEVER call Create. This
// drives the exact same two calls runCreate's --dry-run branch makes
// (reconcileExisting then buildDryRunResult) against the fakeCreateAPI seam
// — mirroring how list_test.go/get_test.go test listSubscriptions/
// getSubscription directly rather than through cobra Execute + a real
// network-capable client, per the task's "fake service, no network" mandate.
func TestDryRun_NotFound_EndToEndViaFakeService_JSONShapeAndNoCreateCall(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	fake := &fakeCreateAPI{listFunc: fixedList(okListResp(nil, false, ""), nil)}

	plan, err := reconcileExisting(context.Background(), fake, resolved.Definition.EventType, resolved.TargetResource, core.AsBot, false)
	if err != nil {
		t.Fatalf("reconcileExisting: unexpected error: %v", err)
	}
	result := buildDryRunResult(resolved, core.AsBot, plan, false)

	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var generic map[string]interface{}
	if err := json.Unmarshal(raw, &generic); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	for _, field := range []string{"operation", "dry_run", "event_key", "target_resource", "required_scopes", "preflight", "remote_before", "planned_change", "local_impact", "next_action"} {
		if _, ok := generic[field]; !ok {
			t.Errorf("dry-run JSON missing field %q; got: %s", field, raw)
		}
	}
	if dryRun, _ := generic["dry_run"].(bool); !dryRun {
		t.Errorf(`dry-run JSON "dry_run" = %v, want true`, generic["dry_run"])
	}
	if generic["remote_before"] != nil {
		t.Errorf(`dry-run JSON "remote_before" = %v, want null (nothing found)`, generic["remote_before"])
	}
	plannedChange, ok := generic["planned_change"].(map[string]interface{})
	if !ok || plannedChange["action"] != planActionCreate {
		t.Errorf(`dry-run JSON "planned_change.action" = %v, want %q`, generic["planned_change"], planActionCreate)
	}

	if fake.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0: --dry-run must never issue a write", fake.createCalls)
	}
}

// TestDryRun_ActiveConflicting_EndToEndViaFakeService_ReportsInformationallyNoCreateCall
// and TestDryRun_Suspended_EndToEndViaFakeService_ReportsInformationallyNoCreateCall extend
// TestDryRun_NotFound_EndToEndViaFakeService_JSONShapeAndNoCreateCall to the plan's other
// two shapes; dry-run always reports the reconcile plan
// informationally and never itself errors on conflict/suspended — only the
// preflight steps (identity/template/scope, already passed by the time
// runCreate reaches --dry-run) are real dry-run failures. This is the
// counterpart of TestCreateOrReuseSubscription_ActiveConflict_
// ReturnsTypedFailedPrecondition and TestCreateOrReuseSubscription_Suspended_
// ReturnsTypedFailedPrecondition_GuidesReactivate, which turn the exact same
// two plan shapes into typed errors on a REAL (non-dry-run) run.
func TestDryRun_ActiveConflicting_EndToEndViaFakeService_ReportsInformationallyNoCreateCall(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	fake := &fakeCreateAPI{listFunc: fixedList(okListResp([]*larkeventv1.SubscriptionDetail{
		activeDetail("sub_conflict", true, "user"), // existing has include_resource_data=true
	}, false, ""), nil)}

	// requested include_resource_data=false (the default) mismatches the
	// existing true -> conflict.
	plan, err := reconcileExisting(context.Background(), fake, resolved.Definition.EventType, resolved.TargetResource, core.AsUser, false)
	if err != nil {
		t.Fatalf("reconcileExisting: unexpected error: %v", err)
	}
	if plan.Action != planActionConflict {
		t.Fatalf("Action = %q, want %q", plan.Action, planActionConflict)
	}
	result := buildDryRunResult(resolved, core.AsUser, plan, false)

	if result.PlannedChange.Action != planActionConflict {
		t.Errorf("PlannedChange.Action = %q, want %q", result.PlannedChange.Action, planActionConflict)
	}
	if result.PlannedChange.RemoteSubscriptionID != "sub_conflict" {
		t.Errorf("PlannedChange.RemoteSubscriptionID = %q, want sub_conflict", result.PlannedChange.RemoteSubscriptionID)
	}
	if len(result.PlannedChange.ConflictFields) != 1 || result.PlannedChange.ConflictFields[0].Name != "include_resource_data" {
		t.Errorf("PlannedChange.ConflictFields = %+v, want one entry naming include_resource_data", result.PlannedChange.ConflictFields)
	}
	if result.RemoteBefore == nil || result.RemoteBefore.RemoteSubscriptionID != "sub_conflict" {
		t.Errorf("RemoteBefore = %+v, want the conflicting subscription row", result.RemoteBefore)
	}

	if fake.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0: --dry-run must never issue a write, even for a plan a real run would reject", fake.createCalls)
	}
}

func TestDryRun_Suspended_EndToEndViaFakeService_ReportsInformationallyNoCreateCall(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	fake := &fakeCreateAPI{listFunc: fixedList(okListResp([]*larkeventv1.SubscriptionDetail{
		suspendedDetail("sub_susp", "authority_revoked"),
	}, false, ""), nil)}

	plan, err := reconcileExisting(context.Background(), fake, resolved.Definition.EventType, resolved.TargetResource, core.AsUser, false)
	if err != nil {
		t.Fatalf("reconcileExisting: unexpected error: %v", err)
	}
	if plan.Action != planActionSuspended {
		t.Fatalf("Action = %q, want %q", plan.Action, planActionSuspended)
	}
	result := buildDryRunResult(resolved, core.AsUser, plan, false)

	if result.PlannedChange.Action != planActionSuspended {
		t.Errorf("PlannedChange.Action = %q, want %q", result.PlannedChange.Action, planActionSuspended)
	}
	if result.PlannedChange.RemoteSubscriptionID != "sub_susp" {
		t.Errorf("PlannedChange.RemoteSubscriptionID = %q, want sub_susp", result.PlannedChange.RemoteSubscriptionID)
	}
	if result.RemoteBefore == nil || result.RemoteBefore.RemoteSubscriptionID != "sub_susp" {
		t.Errorf("RemoteBefore = %+v, want the suspended subscription row", result.RemoteBefore)
	}
	if result.RemoteBefore.Remote.SuspensionReason != "authority_revoked" {
		t.Errorf("RemoteBefore.Remote.SuspensionReason = %q, want authority_revoked", result.RemoteBefore.Remote.SuspensionReason)
	}

	if fake.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0: --dry-run must never issue a write, even for a plan a real run would reject", fake.createCalls)
	}
}

// ---- command wiring ----

// TestNewCmdSubscription_RegistersCreateAsWrite locks that create is
// registered in the subscription group (without disturbing list/get — see
// TestNewCmdSubscription_RegistersListAndGetAsRead, unmodified) and carries
// risk=write, not read like list/get.
func TestNewCmdSubscription_RegistersCreateAsWrite(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdSubscription(f)

	var create *cobra.Command
	for _, c := range cmd.Commands() {
		if c.Name() == "create" {
			create = c
		}
	}
	if create == nil {
		t.Fatal(`subscription command group missing "create" subcommand`)
	}
	level, ok := cmdutil.GetRisk(create)
	if !ok || level != cmdutil.RiskWrite {
		t.Errorf(`"create" risk = (%q, %v), want (%q, true)`, level, ok, cmdutil.RiskWrite)
	}
}

// TestNewCmdCreate_HasExpectedFlagsAndNoYes mirrors
// TestNewCmdList_HasExpectedFlags, plus locks that create must NOT
// expose --yes (only update/delete do — create is additive and
// conflict-precheck'd, not a high-risk confirmation-gated action).
func TestNewCmdCreate_HasExpectedFlagsAndNoYes(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdCreate(f)
	for _, name := range []string{"include-resource-data", "dry-run", "json", "as"} {
		if cmd.Flags().Lookup(name) == nil {
			t.Errorf("NewCmdCreate missing --%s flag", name)
		}
	}
	if cmd.Flags().Lookup("yes") != nil {
		t.Error(`NewCmdCreate must not expose --yes (only update/delete require confirmation)`)
	}
	if level, ok := cmdutil.GetRisk(cmd); !ok || level != cmdutil.RiskWrite {
		t.Errorf("risk = (%q, %v), want (%q, true)", level, ok, cmdutil.RiskWrite)
	}
}
