// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"context"
	"encoding/json"
	"errors"
	"io"
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
// behavior — that is Task 3's). Callers must not also call
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
		Suspension:     &larkeventv1.Suspension{Code: strPtr("authority_revoked")},
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
	// this point once the E-gate rejects true) mismatches the existing true.
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
			suspendedDetail("sub_susp", "authority_revoked"),
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
		t.Errorf("Hint = %q, want it to point at `event schema` (R1)", ve.Hint)
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

// TestRunCreate_KeyLevelAuthTypesRejectsBeforeTemplateCheck locks tier 1
// (spec §2.8): a refined key whose AuthTypes as a whole excludes the
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

func TestRunCreate_IncludeResourceDataTrue_ReturnsEGateFailedPrecondition(t *testing.T) {
	registerCreateFixtures(t)
	f := &cmdutil.Factory{}
	cmd := NewCmdCreate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"im.message.created_v1/chat-id/oc_aaa", "--as", "bot", "--include-resource-data=true"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected an E-gate error, got nil")
	}
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	if ve.Param != "--include-resource-data" {
		t.Errorf("Param = %q, want --include-resource-data", ve.Param)
	}
	if !strings.Contains(ve.Hint, "resource_data_encryption_deferred") {
		t.Errorf("Hint = %q, want it to name reason resource_data_encryption_deferred", ve.Hint)
	}
}

func TestRunCreate_IncludeResourceDataFalse_PassesEGate(t *testing.T) {
	// --include-resource-data=false (the default) must never hit the E-gate.
	// This uses cmdutil.TestFactory (a real, network-free httpmock-backed
	// client) with both scopes granted so the flow proceeds past the E-gate
	// and scope preflight into the real reconcile List call — which then
	// fails with an httpmock "no stub registered" error (no stub is
	// registered here on purpose). That failure is expected and irrelevant
	// to this test: the only thing asserted is that the failure is NOT the
	// E-gate's specific typed error.
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
	var ve *errs.ValidationError
	if errors.As(err, &ve) && ve.Subtype == errs.SubtypeFailedPrecondition && ve.Param == "--include-resource-data" {
		t.Fatalf("--include-resource-data=false must not trip the E-gate, got: %v", err)
	}
}

// TestRunCreate_MissingReadScope_ReturnsPermissionError and
// TestRunCreate_MissingWriteScope_ReturnsPermissionError together lock spec
// §3.5's key differentiator from list/get: create hard-requires BOTH
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

// ---- dry-run output shape (spec §3.4) ----

func TestBuildDryRunResult_NotFound_ShapeAndNoRemoteBefore(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	plan := &reconcilePlan{Action: planActionCreate}

	result := buildDryRunResult(resolved, core.AsUser, plan)

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

	result := buildDryRunResult(resolved, core.AsUser, plan)

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

// TestDryRun_NotFound_EndToEndViaFakeService_JSONShapeAndNoCreateCall is the
// primary TDD case from the task brief: `create <refined key> --dry-run`
// must produce the full §3.4 JSON shape and must NEVER call Create. This
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
	result := buildDryRunResult(resolved, core.AsBot, plan)

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

// ---- command wiring ----

// TestNewCmdSubscription_RegistersCreateAsWrite locks that create is
// registered in the subscription group (without disturbing list/get — see
// TestNewCmdSubscription_RegistersListAndGetAsRead, unmodified) and carries
// risk=write (spec §3.1), not read like list/get.
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
// TestNewCmdList_HasExpectedFlags, plus locks spec §3.7: create must NOT
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
		t.Error(`NewCmdCreate must not expose --yes (spec §3.7: only update/delete require confirmation)`)
	}
	if level, ok := cmdutil.GetRisk(cmd); !ok || level != cmdutil.RiskWrite {
		t.Errorf("risk = (%q, %v), want (%q, true)", level, ok, cmdutil.RiskWrite)
	}
}
