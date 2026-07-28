// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/credential"
	eventlib "github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/model"
	larkgw "github.com/larksuite/cli/internal/event/platform/lark"
	subown "github.com/larksuite/cli/internal/event/subscription"
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
// template, failing the test on any error. Callers must not also call
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

// resolveCreatedOwnerMe is resolveCreatedChatID's owner/me counterpart.
func resolveCreatedOwnerMe(t *testing.T) eventlib.ResolvedEventKey {
	t.Helper()
	registerCreateFixtures(t)
	r, err := eventlib.ResolveEventKey("im.message.created_v1/owner/me")
	if err != nil {
		t.Fatalf("ResolveEventKey: unexpected error: %v", err)
	}
	return r
}

// activeDetail / suspendedDetail are SDK-typed SubscriptionDetail fixtures kept
// here because sibling tests (delete_test.go/update_test.go/subscription_test.go)
// depend on them. create's own flow now works on the domain projection, for which
// activeRemote / suspendedRemote below are the fixtures.
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

// activeRemote / suspendedRemote are the domain-projected (platform/lark)
// fixtures create's Observe -> Plan -> Apply flow actually sees.
func activeRemote(id string, includeResourceData bool, authorityType string) model.RemoteSubscription {
	ird := includeResourceData
	return model.RemoteSubscription{
		ID:                    model.RemoteSubscriptionID(id),
		EventType:             "im.message.created_v1",
		TargetResource:        "im.message?chat_id=oc_aaa",
		Authority:             model.RemoteAuthority{Type: authorityType, OpenID: "ou_aaa"},
		State:                 "active",
		PayloadOptionsPresent: true,
		IncludeResourceData:   &ird,
		Filter:                &eventlib.Filter{},
	}
}

func suspendedRemote(id, reason string) model.RemoteSubscription {
	return model.RemoteSubscription{
		ID:               model.RemoteSubscriptionID(id),
		EventType:        "im.message.created_v1",
		TargetResource:   "im.message?chat_id=oc_aaa",
		Authority:        model.RemoteAuthority{Type: "user", OpenID: "ou_aaa"},
		State:            "suspended",
		SuspensionReason: reason,
		Filter:           &eventlib.Filter{},
	}
}

// stateRemote builds an authority-matching remote in an arbitrary state, for the
// unrecognized-state fail-closed path.
func stateRemote(id, state string) model.RemoteSubscription {
	return model.RemoteSubscription{
		ID:             model.RemoteSubscriptionID(id),
		EventType:      "im.message.created_v1",
		TargetResource: "im.message?chat_id=oc_aaa",
		Authority:      model.RemoteAuthority{Type: "user", OpenID: "ou_aaa"},
		State:          state,
		Filter:         &eventlib.Filter{},
	}
}

func remotePtr(s model.RemoteSubscription) *model.RemoteSubscription { return &s }

// ---- fake gateway (subown.Gateway) ----

// fakeCreateGateway is a network-free stand-in for the subown.Gateway surface.
// walkFunc lets a test vary the List result by call (the Controller's
// reconcile-after-a-failed-create pass reads a second time); createSpec captures
// the exact spec create built (for the atomic-encrypt-key assertion).
type fakeCreateGateway struct {
	walkItems  []model.RemoteSubscription
	walkCapped bool
	walkErr    error
	walkFunc   func(call int) ([]model.RemoteSubscription, bool, error)
	walkCalls  int

	createSpec  *larkgw.CreateSpec
	createResp  *model.RemoteSubscription
	createErr   error
	createCalls int

	reactivateResp  *model.RemoteSubscription
	reactivateErr   error
	reactivateCalls int

	encryptKey         string
	encryptErr         error
	getEncryptKeyCalls int
}

func (g *fakeCreateGateway) WalkSubscriptions(_ context.Context, _ larkgw.ListParams, visit func(model.RemoteSubscription) bool) (bool, error) {
	call := g.walkCalls
	g.walkCalls++
	items, capped, err := g.walkItems, g.walkCapped, g.walkErr
	if g.walkFunc != nil {
		items, capped, err = g.walkFunc(call)
	}
	if err != nil {
		return false, err
	}
	for _, it := range items {
		if !visit(it) {
			return false, nil
		}
	}
	return capped, nil
}

func (g *fakeCreateGateway) Create(_ context.Context, spec larkgw.CreateSpec) (*model.RemoteSubscription, error) {
	g.createCalls++
	s := spec
	g.createSpec = &s
	if g.createErr != nil {
		return nil, g.createErr
	}
	return g.createResp, nil
}

func (g *fakeCreateGateway) Reactivate(_ context.Context, id string) (*model.RemoteSubscription, error) {
	g.reactivateCalls++
	if g.reactivateErr != nil {
		return nil, g.reactivateErr
	}
	return g.reactivateResp, nil
}

func (g *fakeCreateGateway) GetEncryptKey(_ context.Context, _ string) (string, error) {
	g.getEncryptKeyCalls++
	return g.encryptKey, g.encryptErr
}

// countingKeyGen is a spy key generator: it counts calls and returns a fixed key.
func countingKeyGen(calls *int, key string) func() (string, error) {
	return func() (string, error) { *calls++; return key, nil }
}

// ---- applyCreate flow (real Controller over a fake gateway) ----

func TestApplyCreate_NotFound_Creates(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	gw := &fakeCreateGateway{createResp: remotePtr(activeRemote("sub_new", false, "user"))}
	var buf bytes.Buffer

	err := applyCreate(context.Background(), subown.NewController(gw), &buf, resolved, core.AsUser, createOpts{asJSON: true}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gw.createCalls != 1 {
		t.Errorf("createCalls = %d, want 1", gw.createCalls)
	}
	var result createResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if result.Action != "created" {
		t.Errorf("action = %q, want created", result.Action)
	}
	if result.RemoteSubscriptionID != "sub_new" {
		t.Errorf("remote_subscription_id = %q, want sub_new", result.RemoteSubscriptionID)
	}
}

func TestApplyCreate_ActiveCompatible_ReusesWithoutCreate(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	gw := &fakeCreateGateway{walkItems: []model.RemoteSubscription{activeRemote("sub_existing", false, "user")}}
	var buf bytes.Buffer

	err := applyCreate(context.Background(), subown.NewController(gw), &buf, resolved, core.AsUser, createOpts{asJSON: true}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gw.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (idempotent reuse must never call Create)", gw.createCalls)
	}
	var result createResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if result.Action != "reused" || result.RemoteSubscriptionID != "sub_existing" {
		t.Errorf("result = %+v, want reused sub_existing", result)
	}
}

func TestApplyCreate_ActiveConflict_ReturnsTypedFailedPrecondition(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	gw := &fakeCreateGateway{walkItems: []model.RemoteSubscription{activeRemote("sub_conflict", true, "user")}}

	// requested include_resource_data=false mismatches the existing true.
	err := applyCreate(context.Background(), subown.NewController(gw), io.Discard, resolved, core.AsUser, createOpts{}, nil)
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
	if !strings.Contains(ve.Hint, "sub_conflict") || !strings.Contains(ve.Hint, "get") {
		t.Errorf("Hint = %q, want it to mention sub_conflict and guide to `get`", ve.Hint)
	}
	if gw.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (a conflict must never call Create)", gw.createCalls)
	}
}

func TestApplyCreate_Suspended_ReturnsTypedFailedPrecondition_GuidesReactivate(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	// A distinct reason so the assertion proves suspendedError read this fixture.
	gw := &fakeCreateGateway{walkItems: []model.RemoteSubscription{suspendedRemote("sub_susp", "identity_revoked")}}

	err := applyCreate(context.Background(), subown.NewController(gw), io.Discard, resolved, core.AsUser, createOpts{}, nil)
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	if !strings.Contains(ve.Hint, "reactivate") || !strings.Contains(ve.Hint, "sub_susp") {
		t.Errorf("Hint = %q, want it to guide `reactivate` and mention sub_susp", ve.Hint)
	}
	if !strings.Contains(ve.Error(), "identity_revoked") {
		t.Errorf("Error() = %q, want it to surface suspension_reason=identity_revoked", ve.Error())
	}
	if gw.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0", gw.createCalls)
	}
}

// TestApplyCreate_UnrecognizedState_ReturnsTypedFailedPrecondition_NoCreate
// (fail-fast #5a): a match in a state this CLI cannot classify must fail closed
// -- a typed failed_precondition naming the state and the id, never a Create.
func TestApplyCreate_UnrecognizedState_ReturnsTypedFailedPrecondition_NoCreate(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	gw := &fakeCreateGateway{walkItems: []model.RemoteSubscription{stateRemote("sub_weird", "pending")}}

	err := applyCreate(context.Background(), subown.NewController(gw), io.Discard, resolved, core.AsUser, createOpts{}, nil)
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	if len(ve.Params) != 1 || ve.Params[0].Name != "state" {
		t.Errorf("Params = %+v, want one entry naming the state dimension", ve.Params)
	}
	if !strings.Contains(ve.Error(), "pending") || !strings.Contains(ve.Error(), "sub_weird") {
		t.Errorf("Error() = %q, want it to name the unrecognized state and id", ve.Error())
	}
	if !strings.Contains(ve.Hint, "get") || !strings.Contains(ve.Hint, "sub_weird") {
		t.Errorf("Hint = %q, want it to guide `get` and mention sub_weird", ve.Hint)
	}
	if gw.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (an unrecognized state must never Create)", gw.createCalls)
	}
}

func TestApplyCreate_Indeterminate_ReturnsTypedFailedPrecondition_NoCreate(t *testing.T) {
	// A capped scan with no authority match -> Indeterminate -> must NOT create
	// (the #七 must-fix: an inconclusive scan no longer silently Creates).
	resolved := resolveCreatedChatID(t)
	gw := &fakeCreateGateway{
		walkItems:  []model.RemoteSubscription{activeRemote("sub_other", false, "app")}, // "app" never matches AsUser
		walkCapped: true,
	}

	err := applyCreate(context.Background(), subown.NewController(gw), io.Discard, resolved, core.AsUser, createOpts{}, nil)
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	if gw.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (an inconclusive scan must never Create)", gw.createCalls)
	}
}

func TestApplyCreate_CreateFails_ReconcileFindsCompatible_ReturnsReused(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	gw := &fakeCreateGateway{
		createErr: errors.New("duplicate"),
		walkFunc: func(call int) ([]model.RemoteSubscription, bool, error) {
			if call == 0 {
				return nil, false, nil // first Plan: nothing yet -> Create
			}
			// reconcile-after-failure: someone raced a compatible one in.
			return []model.RemoteSubscription{activeRemote("sub_raced", false, "user")}, false, nil
		},
	}
	var buf bytes.Buffer

	err := applyCreate(context.Background(), subown.NewController(gw), &buf, resolved, core.AsUser, createOpts{asJSON: true}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var result createResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if result.Action != "reused" || result.RemoteSubscriptionID != "sub_raced" {
		t.Errorf("result = %+v, want reused sub_raced", result)
	}
	if gw.createCalls != 1 {
		t.Errorf("createCalls = %d, want 1 (never auto-retry Create)", gw.createCalls)
	}
}

func TestApplyCreate_CreateFails_ReconcileFindsConflict_ReturnsTypedFail(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	gw := &fakeCreateGateway{
		createErr: errors.New("duplicate"),
		walkFunc: func(call int) ([]model.RemoteSubscription, bool, error) {
			if call == 0 {
				return nil, false, nil
			}
			return []model.RemoteSubscription{activeRemote("sub_raced_conflict", true, "user")}, false, nil
		},
	}

	err := applyCreate(context.Background(), subown.NewController(gw), io.Discard, resolved, core.AsUser, createOpts{}, nil)
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

func TestApplyCreate_CreateFails_ReconcileFindsNothing_ReturnsOriginalError(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	sentinel := errors.New("boom: transport timeout")
	gw := &fakeCreateGateway{createErr: sentinel} // both reconciles find nothing

	err := applyCreate(context.Background(), subown.NewController(gw), io.Discard, resolved, core.AsUser, createOpts{}, nil)
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want the original Create error passed through unchanged (%v)", err, sentinel)
	}
	if gw.createCalls != 1 {
		t.Errorf("createCalls = %d, want 1 (never auto-retry Create)", gw.createCalls)
	}
}

// ---- encrypted create ----

func TestApplyCreate_Encrypted_NotFound_CreatesAtomicallyWithKey(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	gw := &fakeCreateGateway{createResp: remotePtr(activeRemote("sub_encrypted_new", true, "app"))}
	keyGen := 0
	ctrl := subown.NewController(gw, subown.WithEncryptKeyGenerator(countingKeyGen(&keyGen, "THE-GENERATED-KEY")))

	err := applyCreate(context.Background(), ctrl, io.Discard, resolved, core.AsBot, createOpts{includeResourceData: true}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gw.createCalls != 1 {
		t.Errorf("createCalls = %d, want exactly 1 (one atomic Create)", gw.createCalls)
	}
	if keyGen != 1 {
		t.Errorf("key generator called %d times, want exactly 1", keyGen)
	}
	if gw.createSpec == nil || !gw.createSpec.IncludeResourceData || gw.createSpec.EncryptKey != "THE-GENERATED-KEY" {
		t.Errorf("createSpec = %+v, want include_resource_data=true with the generated key injected atomically", gw.createSpec)
	}
}

func TestApplyCreate_Encrypted_RemoteFalse_ReturnsConflict_NeverProbes(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	gw := &fakeCreateGateway{
		walkItems:  []model.RemoteSubscription{activeRemote("sub_plain", false, "app")}, // "app" matches AsBot
		encryptKey: "usable-key",
	}

	err := applyCreate(context.Background(), subown.NewController(gw), io.Discard, resolved, core.AsBot, createOpts{includeResourceData: true}, nil)
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	if gw.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0", gw.createCalls)
	}
	if gw.getEncryptKeyCalls != 0 {
		t.Errorf("getEncryptKeyCalls = %d, want 0: the probe must never run when the state mismatch is already decisive", gw.getEncryptKeyCalls)
	}
}

func TestApplyCreate_Encrypted_RemoteTrueUsableKey_ReusesNoNewCreateNoNewKey(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	gw := &fakeCreateGateway{
		walkItems:  []model.RemoteSubscription{activeRemote("sub_already_encrypted", true, "app")},
		encryptKey: "usable-remote-key",
	}
	keyGen := 0
	ctrl := subown.NewController(gw, subown.WithEncryptKeyGenerator(countingKeyGen(&keyGen, "unused")))
	var buf bytes.Buffer

	err := applyCreate(context.Background(), ctrl, &buf, resolved, core.AsBot, createOpts{includeResourceData: true, asJSON: true}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var result createResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if result.Action != "reused" || result.RemoteSubscriptionID != "sub_already_encrypted" {
		t.Errorf("result = %+v, want reused sub_already_encrypted", result)
	}
	if gw.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (reuse must never call Create)", gw.createCalls)
	}
	if keyGen != 0 {
		t.Errorf("key generator called %d times, want 0: reusing an existing encrypted match must never generate a new key", keyGen)
	}
	if gw.getEncryptKeyCalls != 1 {
		t.Errorf("getEncryptKeyCalls = %d, want 1 (the probe confirms the usable key)", gw.getEncryptKeyCalls)
	}
}

func TestApplyCreate_Encrypted_RemoteTrueKeyUnavailable_ReturnsConflictHumanHint(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	gw := &fakeCreateGateway{
		walkItems:  []model.RemoteSubscription{activeRemote("sub_key_unavailable", true, "app")},
		encryptErr: errors.New("boom: synthetic permission failure"),
	}

	err := applyCreate(context.Background(), subown.NewController(gw), io.Discard, resolved, core.AsBot, createOpts{includeResourceData: true}, nil)
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	for _, want := range []string{"sub_key_unavailable", "event:encrypt_key:read", "delete"} {
		if !strings.Contains(ve.Hint, want) {
			t.Errorf("Hint = %q, want it to mention %q", ve.Hint, want)
		}
	}
	if gw.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (a conflict must never call Create)", gw.createCalls)
	}
}

func TestApplyCreate_Encrypted_CreateFails_SecondReconcileStillRequiresEncryption(t *testing.T) {
	// The post-failure reconcile must keep requesting include_resource_data=true:
	// a raced PLAINTEXT match must look like a conflict, never a compatible reuse.
	resolved := resolveCreatedChatID(t)
	gw := &fakeCreateGateway{
		createErr: errors.New("boom: synthetic create failure"),
		walkFunc: func(call int) ([]model.RemoteSubscription, bool, error) {
			if call == 0 {
				return nil, false, nil
			}
			return []model.RemoteSubscription{activeRemote("sub_raced_plaintext", false, "user")}, false, nil
		},
	}

	err := applyCreate(context.Background(), subown.NewController(gw), io.Discard, resolved, core.AsUser, createOpts{includeResourceData: true}, nil)
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError (conflict, not a silent plaintext fallback), got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	if gw.createCalls != 1 {
		t.Errorf("createCalls = %d, want 1 (never retry Create, encrypted or otherwise)", gw.createCalls)
	}
}

// ---- key redaction ----

func TestEncryptedCreate_Redaction_KeyNeverAppearsInAnyOutput(t *testing.T) {
	const secret = "SPY-ENCRYPT-KEY-REDACT-ME"
	resolved := resolveCreatedChatID(t)

	for _, asJSON := range []bool{true, false} {
		gw := &fakeCreateGateway{createResp: remotePtr(activeRemote("sub_enc", true, "app"))}
		ctrl := subown.NewController(gw, subown.WithEncryptKeyGenerator(func() (string, error) { return secret, nil }))
		var buf bytes.Buffer

		err := applyCreate(context.Background(), ctrl, &buf, resolved, core.AsBot, createOpts{includeResourceData: true, asJSON: asJSON}, nil)
		if err != nil {
			t.Fatalf("asJSON=%v: unexpected error: %v", asJSON, err)
		}
		// Control: the key really was generated and threaded into the Create.
		if gw.createCalls != 1 || gw.createSpec == nil || gw.createSpec.EncryptKey != secret {
			t.Fatalf("asJSON=%v: control failed — createCalls=%d createSpec=%+v (the key must actually have been used)", asJSON, gw.createCalls, gw.createSpec)
		}
		if strings.Contains(buf.String(), secret) {
			t.Errorf("REDACTION VIOLATION (asJSON=%v): encrypt_key found in output: %s", asJSON, buf.String())
		}
	}
}

func TestEncryptedCreate_Redaction_KeyNeverAppearsInErrorOnCreateFailure(t *testing.T) {
	const secret = "SPY-ENCRYPT-KEY-REDACT-ME"
	resolved := resolveCreatedChatID(t)
	gw := &fakeCreateGateway{createErr: errors.New("boom: synthetic create failure")} // both reconciles find nothing
	ctrl := subown.NewController(gw, subown.WithEncryptKeyGenerator(func() (string, error) { return secret, nil }))

	err := applyCreate(context.Background(), ctrl, io.Discard, resolved, core.AsBot, createOpts{includeResourceData: true}, nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	if gw.createSpec == nil || gw.createSpec.EncryptKey != secret {
		t.Fatal("control failed: the key must actually have been generated and used")
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("REDACTION VIOLATION: encrypt_key found in error message: %v", err)
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
// array, which every OTHER caller of subscriptionMutationScopes still relies
// on being exactly its original 2-element literal.
func TestCreateRequiredScopes_DoesNotMutateSharedBaseSlice(t *testing.T) {
	before := append([]string(nil), subscriptionMutationScopes...)
	_ = createRequiredScopes(true)
	if !reflect.DeepEqual(subscriptionMutationScopes, before) {
		t.Errorf("subscriptionMutationScopes mutated: got %v, want %v", subscriptionMutationScopes, before)
	}
}

// ---- dry-run: never generate a key ----

// TestDryRun_IncludeResourceDataTrue_NotFound_GeneratesNoKeyAndNoCreateCall
// locks that --dry-run's Plan step never generates a key nor writes, even when
// the plan lands on Create (the one row a REAL run would generate a key for).
func TestDryRun_IncludeResourceDataTrue_NotFound_GeneratesNoKeyAndNoCreateCall(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	gw := &fakeCreateGateway{}
	keyGen := 0
	ctrl := subown.NewController(gw, subown.WithEncryptKeyGenerator(countingKeyGen(&keyGen, "x")))

	err := applyCreate(context.Background(), ctrl, io.Discard, resolved, core.AsBot, createOpts{includeResourceData: true, dryRun: true}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if keyGen != 0 {
		t.Errorf("key generator invoked %d times during --dry-run, want 0", keyGen)
	}
	if gw.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0: --dry-run must never issue a write", gw.createCalls)
	}
}

// TestDryRun_IncludeResourceDataTrue_ActiveMatch_ProbesButGeneratesNoKey covers
// the OTHER dry-run row: an existing active include_resource_data=true match.
// --dry-run's plan step MAY probe remote state (GetEncryptKey, to report
// reuse-vs-conflict) but must still never generate a NEW key or write.
func TestDryRun_IncludeResourceDataTrue_ActiveMatch_ProbesButGeneratesNoKey(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	gw := &fakeCreateGateway{
		walkItems:  []model.RemoteSubscription{activeRemote("sub_enc", true, "app")},
		encryptKey: "usable-key-from-remote",
	}
	keyGen := 0
	ctrl := subown.NewController(gw, subown.WithEncryptKeyGenerator(countingKeyGen(&keyGen, "x")))

	err := applyCreate(context.Background(), ctrl, io.Discard, resolved, core.AsBot, createOpts{includeResourceData: true, dryRun: true}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gw.getEncryptKeyCalls != 1 {
		t.Errorf("getEncryptKeyCalls = %d, want 1: dry-run's plan step may probe remote state", gw.getEncryptKeyCalls)
	}
	if keyGen != 0 {
		t.Errorf("key generator invoked %d times during --dry-run, want 0: probing an existing key must never generate a new one", keyGen)
	}
	if gw.createCalls != 0 {
		t.Error("createCalls must be 0 for --dry-run")
	}
}

// ---- dry-run output shape ----

func TestBuildDryRunResult_NotFound_ShapeAndNoRemoteBefore(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	plan := subown.SubscriptionPlan{Action: subown.ActionCreate}

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
	if result.PlannedChange.Action != "create" {
		t.Errorf("PlannedChange.Action = %q, want create", result.PlannedChange.Action)
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
	plan := subown.SubscriptionPlan{Action: subown.ActionReuse, Before: remotePtr(activeRemote("sub_existing", false, "user"))}

	result := buildDryRunResult(resolved, core.AsUser, plan, false)

	if result.PlannedChange.Action != "reuse" {
		t.Errorf("PlannedChange.Action = %q, want reuse", result.PlannedChange.Action)
	}
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
// locks that dry-run's reported required_scopes reflects the conditional third
// scope: a --dry-run preview must never claim a smaller scope requirement than
// the real run it previews.
func TestBuildDryRunResult_IncludeResourceDataTrue_RequiredScopesIncludesEncryptKeyRead(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	plan := subown.SubscriptionPlan{Action: subown.ActionCreate}

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

// TestDryRun_NotFound_EndToEnd_JSONShapeAndNoCreateCall drives applyCreate's
// --dry-run branch over a fake gateway: it must produce the full dry-run JSON
// shape and never call Create.
func TestDryRun_NotFound_EndToEnd_JSONShapeAndNoCreateCall(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	gw := &fakeCreateGateway{}
	var buf bytes.Buffer

	err := applyCreate(context.Background(), subown.NewController(gw), &buf, resolved, core.AsBot, createOpts{dryRun: true, asJSON: true}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var generic map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &generic); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	for _, field := range []string{"operation", "dry_run", "event_key", "target_resource", "required_scopes", "preflight", "remote_before", "planned_change", "local_impact", "next_action"} {
		if _, ok := generic[field]; !ok {
			t.Errorf("dry-run JSON missing field %q; got: %s", field, buf.String())
		}
	}
	if dryRun, _ := generic["dry_run"].(bool); !dryRun {
		t.Errorf(`dry-run JSON "dry_run" = %v, want true`, generic["dry_run"])
	}
	if generic["remote_before"] != nil {
		t.Errorf(`dry-run JSON "remote_before" = %v, want null (nothing found)`, generic["remote_before"])
	}
	plannedChange, ok := generic["planned_change"].(map[string]interface{})
	if !ok || plannedChange["action"] != "create" {
		t.Errorf(`dry-run JSON "planned_change.action" = %v, want "create"`, generic["planned_change"])
	}
	if gw.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0: --dry-run must never issue a write", gw.createCalls)
	}
}

func TestDryRun_ActiveConflicting_EndToEnd_ReportsInformationallyNoCreateCall(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	gw := &fakeCreateGateway{walkItems: []model.RemoteSubscription{activeRemote("sub_conflict", true, "user")}}
	var buf bytes.Buffer

	// requested include_resource_data=false mismatches the existing true -> conflict.
	err := applyCreate(context.Background(), subown.NewController(gw), &buf, resolved, core.AsUser, createOpts{dryRun: true, asJSON: true}, nil)
	if err != nil {
		t.Fatalf("dry-run must report informationally, not error: %v", err)
	}
	var result createDryRunResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if result.PlannedChange.Action != "conflict" {
		t.Errorf("PlannedChange.Action = %q, want conflict", result.PlannedChange.Action)
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
	if gw.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0: --dry-run must never issue a write", gw.createCalls)
	}
}

func TestDryRun_Suspended_EndToEnd_ReportsInformationallyNoCreateCall(t *testing.T) {
	resolved := resolveCreatedChatID(t)
	gw := &fakeCreateGateway{walkItems: []model.RemoteSubscription{suspendedRemote("sub_susp", "authority_revoked")}}
	var buf bytes.Buffer

	err := applyCreate(context.Background(), subown.NewController(gw), &buf, resolved, core.AsUser, createOpts{dryRun: true, asJSON: true}, nil)
	if err != nil {
		t.Fatalf("dry-run must report informationally, not error: %v", err)
	}
	var result createDryRunResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if result.PlannedChange.Action != "suspended" {
		t.Errorf("PlannedChange.Action = %q, want suspended", result.PlannedChange.Action)
	}
	if result.RemoteBefore == nil || result.RemoteBefore.RemoteSubscriptionID != "sub_susp" {
		t.Errorf("RemoteBefore = %+v, want the suspended subscription row", result.RemoteBefore)
	}
	if result.RemoteBefore.Remote.SuspensionReason != "authority_revoked" {
		t.Errorf("RemoteBefore.Remote.SuspensionReason = %q, want authority_revoked", result.RemoteBefore.Remote.SuspensionReason)
	}
	if gw.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0: --dry-run must never issue a write", gw.createCalls)
	}
}

// ---- --filter parse (command level) ----

// TestRunCreate_InvalidFilter_ReturnsInvalidArgumentOnFilterParam locks that an
// invalid --filter is rejected as a typed invalid_argument on --filter, before
// any identity/scope resolution or network call (empty Factory proves it never
// reaches config).
func TestRunCreate_InvalidFilter_ReturnsInvalidArgumentOnFilterParam(t *testing.T) {
	registerCreateFixtures(t)
	f := &cmdutil.Factory{}
	cmd := NewCmdCreate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{
		"im.message.created_v1/chat-id/oc_aaa",
		"--filter", `{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"not_a_real_operand","op":"eq","value":"x"}}]}}`,
	})

	err := cmd.Execute()
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeInvalidArgument {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeInvalidArgument)
	}
	if ve.Param != "--filter" {
		t.Errorf("Param = %q, want --filter", ve.Param)
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
// before any network write) ----

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
// a refined key whose AuthTypes as a whole excludes the resolved identity must
// reject with invalid_argument (not failed_precondition — that is tier 2's).
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

// TestRunCreate_IncludeResourceDataFalse_DoesNotRequireEncryptKeyReadScope
// locks that --include-resource-data=false (the default) never pulls in the
// extra event:encrypt_key:read scope: with ONLY the base mutation scopes
// granted, the flow must proceed past scope preflight into the real remote read
// (which then fails on an unregistered stub — irrelevant here; the only thing
// asserted is that the failure is NOT a *errs.PermissionError).
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
// locks the scope wiring: --include-resource-data=true additionally requires
// event:encrypt_key:read on top of the usual event:subscription:{read,write} —
// missing it alone must fail closed naming it.
func TestRunCreate_IncludeResourceDataTrue_MissingEncryptKeyReadScope_ReturnsPermissionError(t *testing.T) {
	registerCreateFixtures(t)
	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "cli_x"})
	f.Credential = credential.NewCredentialProvider(nil, nil, &fakeTokenResolver{
		result: &credential.TokenResult{Token: "t-tok", Scopes: "event:subscription:read event:subscription:write"},
	}, nil)

	cmd := NewCmdCreate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	// --as user: resource data is user-only, so the scope preflight is what fires.
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
// is the =true counterpart: with all three scopes granted the flow proceeds
// past scope preflight into the real remote read (which then fails on an
// unregistered stub — only asserted here is that it is NOT a PermissionError).
func TestRunCreate_IncludeResourceDataTrue_AllScopesGranted_PassesScopePreflight(t *testing.T) {
	registerCreateFixtures(t)
	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "cli_x"})
	f.Credential = credential.NewCredentialProvider(nil, nil, &fakeTokenResolver{
		result: &credential.TokenResult{Token: "t-tok", Scopes: "event:subscription:read event:subscription:write event:encrypt_key:read"},
	}, nil)

	cmd := NewCmdCreate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"im.message.created_v1/chat-id/oc_aaa", "--as", "user", "--include-resource-data=true"})

	err := cmd.Execute()
	var permErr *errs.PermissionError
	if errors.As(err, &permErr) {
		t.Fatalf("all three scopes were granted, must not report a permission error, got: %v", err)
	}
}

// TestRunCreate_IncludeResourceDataTrue_AsBot_RejectedRequiresUser locks that
// resource data is user-only: --include-resource-data=true with --as bot is a
// typed invalid_argument fired BEFORE any remote call or scope preflight.
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

// TestRunCreate_MissingReadScope / MissingWriteScope together lock create's key
// differentiator from list/get: it hard-requires BOTH event:subscription:read
// AND event:subscription:write — missing EITHER one alone must still fail closed.
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

// ---- command wiring ----

// TestNewCmdSubscription_RegistersCreateAsWrite locks that create is registered
// in the subscription group and carries risk=write.
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

// TestNewCmdCreate_HasExpectedFlagsAndNoYes locks the flag set and that create
// does NOT expose --yes (additive, conflict-prechecked, not confirmation-gated).
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
