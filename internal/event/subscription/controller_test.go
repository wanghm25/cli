// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"context"
	"errors"
	"testing"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/core"
	lark "github.com/larksuite/cli/internal/event/platform/lark"
)

// fakeGateway is a network-free stand-in for the subscription.Gateway surface.
// walkFunc lets a test vary the List result by call (the reconcile-after-failure
// pass reads a second time); createSpec captures the exact spec create built.
type fakeGateway struct {
	walkItems  []lark.RemoteSubscription
	walkCapped bool
	walkErr    error
	walkFunc   func(call int) ([]lark.RemoteSubscription, bool, error)
	walkCalls  int

	createSpec  *lark.CreateSpec
	createResp  *lark.RemoteSubscription
	createErr   error
	createCalls int

	reactivateResp  *lark.RemoteSubscription
	reactivateErr   error
	reactivateID    string
	reactivateCalls int

	encryptKey string
	encryptErr error
}

func (g *fakeGateway) WalkSubscriptions(_ context.Context, _ lark.ListParams, visit func(lark.RemoteSubscription) bool) (bool, error) {
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

func (g *fakeGateway) Create(_ context.Context, spec lark.CreateSpec) (*lark.RemoteSubscription, error) {
	g.createCalls++
	s := spec
	g.createSpec = &s
	if g.createErr != nil {
		return nil, g.createErr
	}
	return g.createResp, nil
}

func (g *fakeGateway) Reactivate(_ context.Context, id string) (*lark.RemoteSubscription, error) {
	g.reactivateCalls++
	g.reactivateID = id
	if g.reactivateErr != nil {
		return nil, g.reactivateErr
	}
	return g.reactivateResp, nil
}

func (g *fakeGateway) GetEncryptKey(_ context.Context, _ string) (string, error) {
	return g.encryptKey, g.encryptErr
}

func subPtr(s lark.RemoteSubscription) *lark.RemoteSubscription { return &s }

// ---- Apply: writable actions ----

func TestApply_Create_CallsCreateOnce_CreatedByAttempt(t *testing.T) {
	g := &fakeGateway{createResp: subPtr(activeSub("sub_new", false, "user"))}
	c := NewController(g)
	receipt, err := c.Apply(context.Background(), SubscriptionPlan{Action: ActionCreate}, ManagementCreate, req(false, nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if receipt.Action != ActionCreate || !receipt.CreatedByAttempt || receipt.RemoteID.String() != "sub_new" {
		t.Errorf("receipt = %+v, want created sub_new by-attempt", receipt)
	}
	if g.createCalls != 1 {
		t.Errorf("createCalls = %d, want 1", g.createCalls)
	}
}

func TestApply_Reuse_NeverWrites(t *testing.T) {
	g := &fakeGateway{}
	c := NewController(g)
	before := activeSub("sub_existing", false, "user")
	receipt, err := c.Apply(context.Background(), SubscriptionPlan{Action: ActionReuse, Before: &before}, ManagementCreate, req(false, nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if receipt.Action != ActionReuse || receipt.CreatedByAttempt || receipt.RemoteID.String() != "sub_existing" {
		t.Errorf("receipt = %+v, want reuse sub_existing not-by-attempt", receipt)
	}
	if g.createCalls != 0 || g.reactivateCalls != 0 {
		t.Errorf("reuse must never write (create=%d reactivate=%d)", g.createCalls, g.reactivateCalls)
	}
}

func TestApply_Reactivate_CallsReactivateNotCreate(t *testing.T) {
	g := &fakeGateway{reactivateResp: subPtr(activeSub("sub_susp", false, "user"))}
	c := NewController(g)
	before := suspendedSub("sub_susp", "authority_revoked")
	receipt, err := c.Apply(context.Background(), SubscriptionPlan{Action: ActionReactivate, Before: &before}, ConsumeBootstrap, req(false, nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if receipt.Action != ActionReactivate || receipt.CreatedByAttempt || receipt.RemoteID.String() != "sub_susp" {
		t.Errorf("receipt = %+v, want reactivate sub_susp not-by-attempt", receipt)
	}
	if g.reactivateCalls != 1 || g.reactivateID != "sub_susp" {
		t.Errorf("reactivateCalls=%d id=%q, want 1 sub_susp", g.reactivateCalls, g.reactivateID)
	}
	if g.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (reactivate never creates)", g.createCalls)
	}
}

// ---- Apply: empty remote id -> InvalidResponse (must-fix) ----

func TestApply_Reuse_EmptyID_ReturnsInvalidResponse(t *testing.T) {
	g := &fakeGateway{}
	c := NewController(g)
	before := activeSub("", false, "user") // wire anomaly: empty id projected from a List item
	_, err := c.Apply(context.Background(), SubscriptionPlan{Action: ActionReuse, Before: &before}, ManagementCreate, req(false, nil))
	var ie *errs.InternalError
	if !errors.As(err, &ie) || ie.Subtype != errs.SubtypeInvalidResponse {
		t.Fatalf("err = %v, want InternalError/InvalidResponse for an empty reused id", err)
	}
}

func TestApply_Create_EmptyID_ReturnsInvalidResponse(t *testing.T) {
	g := &fakeGateway{createResp: subPtr(activeSub("", false, "user"))}
	c := NewController(g)
	_, err := c.Apply(context.Background(), SubscriptionPlan{Action: ActionCreate}, ManagementCreate, req(false, nil))
	var ie *errs.InternalError
	if !errors.As(err, &ie) || ie.Subtype != errs.SubtypeInvalidResponse {
		t.Fatalf("err = %v, want InternalError/InvalidResponse for an empty created id", err)
	}
}

// ---- Apply: encrypt_key generation (encrypted create only) ----

func TestApply_EncryptedCreate_GeneratesKeyOnce_InjectsIntoSpec(t *testing.T) {
	g := &fakeGateway{createResp: subPtr(activeSub("sub_enc_new", true, "user"))}
	keyGen := 0
	c := NewController(g, WithEncryptKeyGenerator(func() (string, error) { keyGen++; return "the-generated-key", nil }))
	receipt, err := c.Apply(context.Background(), SubscriptionPlan{Action: ActionCreate}, ManagementCreate, req(true, nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if keyGen != 1 {
		t.Errorf("key generator called %d times, want exactly 1", keyGen)
	}
	if g.createSpec == nil || !g.createSpec.IncludeResourceData || g.createSpec.EncryptKey != "the-generated-key" {
		t.Errorf("createSpec = %+v, want include_resource_data=true with the generated key injected atomically", g.createSpec)
	}
	if !receipt.CreatedByAttempt {
		t.Error("CreatedByAttempt = false, want true")
	}
}

func TestApply_PlaintextCreateReuseReactivate_NeverGenerateKey(t *testing.T) {
	keyGen := 0
	gen := WithEncryptKeyGenerator(func() (string, error) { keyGen++; return "x", nil })

	// plaintext create
	NewController(&fakeGateway{createResp: subPtr(activeSub("s", false, "user"))}, gen).
		Apply(context.Background(), SubscriptionPlan{Action: ActionCreate}, ManagementCreate, req(false, nil))
	// reuse (even for an encrypted request, reuse never generates a new key)
	before := activeSub("s", true, "user")
	NewController(&fakeGateway{}, gen).
		Apply(context.Background(), SubscriptionPlan{Action: ActionReuse, Before: &before}, ManagementCreate, req(true, nil))
	// reactivate
	sus := suspendedSub("s", "authority_revoked")
	NewController(&fakeGateway{reactivateResp: subPtr(activeSub("s", false, "user"))}, gen).
		Apply(context.Background(), SubscriptionPlan{Action: ActionReactivate, Before: &sus}, ConsumeBootstrap, req(false, nil))

	if keyGen != 0 {
		t.Errorf("key generator called %d times across plaintext-create/reuse/reactivate, want 0", keyGen)
	}
}

func TestApply_EncryptedCreate_KeyGenFailure_FailsClosed(t *testing.T) {
	g := &fakeGateway{}
	c := NewController(g, WithEncryptKeyGenerator(func() (string, error) { return "", errors.New("csprng down") }))
	_, err := c.Apply(context.Background(), SubscriptionPlan{Action: ActionCreate}, ManagementCreate, req(true, nil))
	if err == nil {
		t.Fatal("expected key-gen failure to abort the create")
	}
	if g.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (a key-gen failure must never fall back to a plaintext create)", g.createCalls)
	}
}

// ---- Apply: non-writable actions are the caller's job ----

func TestApply_NonWritableAction_IsInternalError(t *testing.T) {
	c := NewController(&fakeGateway{})
	for _, a := range []Action{ActionBlock, ActionIndeterminate} {
		if _, err := c.Apply(context.Background(), SubscriptionPlan{Action: a}, ManagementCreate, req(false, nil)); err == nil {
			t.Errorf("Apply(%q) err = nil, want an internal error", a)
		}
	}
}

// ---- Apply: bounded reconcile-after-create-failure (ManagementCreate only) ----

func TestApply_CreateFails_ManagementReconcileFindsReuse_ReturnsReused(t *testing.T) {
	g := &fakeGateway{
		createErr: errors.New("duplicate"),
		// The reconcile pass (walk call 0) finds a raced compatible active match.
		walkFunc: func(int) ([]lark.RemoteSubscription, bool, error) {
			return []lark.RemoteSubscription{activeSub("sub_raced", false, "user")}, false, nil
		},
	}
	c := NewController(g)
	receipt, err := c.Apply(context.Background(), SubscriptionPlan{Action: ActionCreate}, ManagementCreate, Request{EventType: "im.message.created_v1", TargetResource: "im.message?chat_id=oc_aaa", Identity: core.AsUser})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if receipt.Action != ActionReuse || receipt.RemoteID.String() != "sub_raced" {
		t.Errorf("receipt = %+v, want reuse sub_raced", receipt)
	}
	if g.createCalls != 1 {
		t.Errorf("createCalls = %d, want 1 (never auto-retry Create)", g.createCalls)
	}
}

func TestApply_CreateFails_ManagementReconcileFindsBlock_ReturnsPlanBlockedError(t *testing.T) {
	g := &fakeGateway{
		createErr: errors.New("duplicate"),
		walkFunc: func(int) ([]lark.RemoteSubscription, bool, error) {
			return []lark.RemoteSubscription{activeSub("sub_raced_conflict", true, "user")}, false, nil // include mismatch vs false request
		},
	}
	c := NewController(g)
	_, err := c.Apply(context.Background(), SubscriptionPlan{Action: ActionCreate}, ManagementCreate, Request{EventType: "im.message.created_v1", TargetResource: "im.message?chat_id=oc_aaa", Identity: core.AsUser})
	var be *PlanBlockedError
	if !errors.As(err, &be) {
		t.Fatalf("err = %v, want *PlanBlockedError", err)
	}
	if be.Plan.Action != ActionBlock || be.Plan.Before == nil || be.Plan.Before.ID.String() != "sub_raced_conflict" {
		t.Errorf("blocked plan = %+v, want Block on sub_raced_conflict", be.Plan)
	}
}

func TestApply_CreateFails_ManagementReconcileFindsNothing_ReturnsOriginalError(t *testing.T) {
	sentinel := errors.New("boom: transport timeout")
	g := &fakeGateway{createErr: sentinel} // both walks find nothing (default empty)
	c := NewController(g)
	_, err := c.Apply(context.Background(), SubscriptionPlan{Action: ActionCreate}, ManagementCreate, Request{EventType: "e", TargetResource: "t", Identity: core.AsUser})
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want the original create error (%v)", err, sentinel)
	}
	if g.walkCalls != 1 {
		t.Errorf("walkCalls = %d, want 1 (one bounded reconcile-after-failure pass)", g.walkCalls)
	}
}

func TestApply_CreateFails_ConsumeBootstrap_NoReconcile_PropagatesError(t *testing.T) {
	sentinel := errors.New("boom")
	g := &fakeGateway{
		createErr: sentinel,
		// Even if a compatible match now exists, ConsumeBootstrap must not re-list.
		walkFunc: func(int) ([]lark.RemoteSubscription, bool, error) {
			return []lark.RemoteSubscription{activeSub("sub_raced", false, "user")}, false, nil
		},
	}
	c := NewController(g)
	_, err := c.Apply(context.Background(), SubscriptionPlan{Action: ActionCreate}, ConsumeBootstrap, req(false, nil))
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want the original create error (%v)", err, sentinel)
	}
	if g.walkCalls != 0 {
		t.Errorf("walkCalls = %d, want 0 (consume bootstrap never re-lists after a failed create)", g.walkCalls)
	}
}

// ---- Plan: Observe+Plan wired through the gateway ----

func TestControllerPlan_Wires_Observe_And_Plan(t *testing.T) {
	g := &fakeGateway{walkItems: []lark.RemoteSubscription{activeSub("sub_1", false, "user")}}
	plan, err := NewController(g).Plan(context.Background(), ManagementCreate, Request{EventType: "im.message.created_v1", TargetResource: "im.message?chat_id=oc_aaa", Identity: core.AsUser})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != ActionReuse || plan.Before == nil || plan.Before.ID.String() != "sub_1" {
		t.Errorf("plan = %+v, want Reuse sub_1", plan)
	}
}
