// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/event/bus/lifecycle"
)

// --- Task 18: subscriptionLifecycleAction (spec §5.3/§5.4/§5.5/§8) ---------
//
// These tests exercise subscriptionLifecycleAction.Handle directly (the real
// lifecycleAction bus.go wires into newLifecycleExecutor in place of Task
// 17's summaryLifecycleAction) against a fake lifecycle.SubscriptionClient and a
// real identityGate (itself wired to fake resolveCurrent/resolveUAT/bindUser
// funcs, exactly like identity_test.go) -- no *lark.Client, no network, no
// disk/keychain.

// --- fakes -------------------------------------------------------------

// fakeSubscriptionActionClient records how many times each of
// Get/Reactivate/Renew was called and returns pre-configured (resp, err)
// pairs, so tests assert EXACTLY how many times each was invoked (the spec
// §5.2/§5.3 "at most ONE such call per event" guarantee) without any
// *lark.Client or network call involved. Requests aren't inspected for
// their subscription_id: larkeventv1's Req types keep their apiReq
// (PathParams) unexported, exactly like this SDK's OTHER existing test
// fakes (e.g. cmd/event/subscription/get_test.go's fakeGetAPI) — the
// remote_subscription_id passed to NewXxxReqBuilder is opaque from outside
// package larkeventv1.
type fakeSubscriptionActionClient struct {
	mu sync.Mutex

	getCalls        int
	reactivateCalls int
	renewCalls      int

	getResp       *larkeventv1.GetSubscriptionResp
	getErr        error
	reactivateErr error
	renewErr      error
}

func (f *fakeSubscriptionActionClient) Get(_ context.Context, _ *larkeventv1.GetSubscriptionReq) (*larkeventv1.GetSubscriptionResp, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls++
	return f.getResp, f.getErr
}

func (f *fakeSubscriptionActionClient) Reactivate(_ context.Context, _ *larkeventv1.ReactivateSubscriptionReq) (*larkeventv1.ReactivateSubscriptionResp, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reactivateCalls++
	if f.reactivateErr != nil {
		return nil, f.reactivateErr
	}
	return &larkeventv1.ReactivateSubscriptionResp{}, nil
}

func (f *fakeSubscriptionActionClient) Renew(_ context.Context, _ *larkeventv1.RenewSubscriptionReq) (*larkeventv1.RenewSubscriptionResp, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewCalls++
	if f.renewErr != nil {
		return nil, f.renewErr
	}
	return &larkeventv1.RenewSubscriptionResp{}, nil
}

func (f *fakeSubscriptionActionClient) getCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.getCalls
}
func (f *fakeSubscriptionActionClient) reactivateCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reactivateCalls
}
func (f *fakeSubscriptionActionClient) renewCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.renewCalls
}

// buildGetResp constructs a minimal GetSubscriptionResp reporting state
// (and, when non-empty, a verbatim suspension code) -- exactly the shape
// reconcileWithGet reads.
func buildGetResp(state, suspensionCode string) *larkeventv1.GetSubscriptionResp {
	b := larkeventv1.NewSubscriptionDetailBuilder().State(state)
	if suspensionCode != "" {
		b = b.Suspension(larkeventv1.NewSuspensionBuilder().Code(suspensionCode).Build())
	}
	return &larkeventv1.GetSubscriptionResp{Data: &larkeventv1.GetSubscriptionRespData{Subscription: b.Build()}}
}

// buildGetRespActive constructs a state="active" GetSubscriptionResp that
// ALSO carries target_resource/authority/payload_options.include_resource_data
// -- the 3 dimensions issue #23's reconcileWithGet compatibility check
// projects and compares against the lead conn's own stored intent.
// authorityUserOpenID=="" builds an "app" authority; non-empty builds
// "user:<id>" (mirrors lifecycle.AuthorityMatchesOwner's own vocabulary).
func buildGetRespActive(targetResource, authorityUserOpenID string, includeResourceData bool) *larkeventv1.GetSubscriptionResp {
	authorityBuilder := larkeventv1.NewAuthorityBuilder().Type("app")
	if authorityUserOpenID != "" {
		authorityBuilder = larkeventv1.NewAuthorityBuilder().Type("user").OpenId(authorityUserOpenID)
	}
	d := larkeventv1.NewSubscriptionDetailBuilder().
		State("active").
		TargetResource(targetResource).
		Authority(authorityBuilder.Build()).
		PayloadOptions(larkeventv1.NewPayloadOptionsBuilder().IncludeResourceData(includeResourceData).Build()).
		Build()
	return &larkeventv1.GetSubscriptionResp{Data: &larkeventv1.GetSubscriptionRespData{Subscription: d}}
}

// newLifecycleDispatchTestConn builds a *Conn registered on hub with BOTH a
// remote_subscription_id AND an owner identity fixed -- lifecycle_test.go's
// newConnWithRemoteSub only sets the former, identity_test.go's
// newIdentityTestConn only sets the latter; Task 18's dispatch needs both
// simultaneously.
func newLifecycleDispatchTestConn(t *testing.T, pid int, remoteSubID, ownerIdentity, ownerAppID, ownerUserOpenID string) *Conn {
	t.Helper()
	conn, _ := net.Pipe()
	t.Cleanup(func() { conn.Close() })
	c := NewConn(conn, nil, "im.msg", []string{"im.message.receive_v1"}, pid, "")
	c.SetRemoteSubscriptionID(remoteSubID)
	c.SetOwnerIdentity(ownerIdentity, ownerAppID, ownerUserOpenID)
	return c
}

// testActionDeps bundles the fakes newSubscriptionLifecycleActionForTest
// wires together, so each test can reach into whichever it needs to assert
// on (the fake client's call counts, the fake bindUser's call count, ...).
type testActionDeps struct {
	action *lifecycle.SubscriptionAction
	gate   *identityGate
	client *fakeSubscriptionActionClient
	bind   *fakeBindUser
	uat    *fakeUATResolver
}

// newTestAction wires a subscriptionLifecycleAction with fake
// identityGate+SubscriptionClient deps. connID/bindUser are memoized via a
// real onConnReady call up front (mirrors production: the WS is already
// ready by the time any lifecycle event could possibly arrive) UNLESS
// wireBind is false, in which case bindUser is deliberately left
// unconfigured (nil) to test the "WS never became ready" path.
//
// MUST be called BEFORE any *Conn is registered on hub: onConnReady's own
// pass (spec §4.4) immediately binds every ALREADY-registered matching user
// conn, which would consume the fake bindUser's call count (and, for a
// failCall-configured fake, its call-INDEX) before the test's own Handle()
// call ever runs. Calling this first means hub.userConns() is still empty at
// memoization time -- only (connID, bindUser) get memoized, no bind is
// actually attempted until the test registers its own conn(s) and calls
// Handle().
func newTestAction(t *testing.T, hub *Hub, resolveCurrent func() (currentIdentity, error), wireBind bool) *testActionDeps {
	t.Helper()
	uat := &fakeUATResolver{uat: "uat-current"}
	fb := &fakeBindUser{}
	gate := newIdentityGate(hub, resolveCurrent, uat.resolve, discardTestLogger())
	if wireBind {
		gate.onConnReady(context.Background(), "conn-1", fb.bind)
	}

	client := &fakeSubscriptionActionClient{}
	action := lifecycle.NewSubscriptionAction(hub.lifecycleRegistry(), discardTestLogger())
	action.SetIdentityGate(gate)
	action.SetNewSubscriptionClient(func(_ core.Identity, _ string) (lifecycle.SubscriptionClient, error) {
		return client, nil
	})

	return &testActionDeps{action: action, gate: gate, client: client, bind: fb, uat: uat}
}

// =========================================================================
// §8 SECURITY RED LINE: suspended -- hit+current-owner+token -> Reactivate;
// owner!=current -> NO Reactivate/BindUser (mis-recovery blocked).
// =========================================================================

func TestSubscriptionLifecycleAction_Suspended_HitCurrentOwnerUser_SingleReactivate_BindConsumer(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	hub.RegisterAndIsFirst(c)

	le := lifecycle.LifecycleEvent{EventType: "event.subscription.suspended_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "suspended", SuspensionCode: "authority_revoked"}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}

	if got := deps.client.reactivateCount(); got != 1 {
		t.Fatalf("Reactivate call count = %d, want 1", got)
	}
	if got := deps.bind.callCount(); got != 1 {
		t.Errorf("bindUser call count = %d, want 1 (user must be bound after a successful Reactivate)", got)
	}
	if got := c.BoundConnID(); got != "conn-1" {
		t.Errorf("BoundConnID() = %q, want %q", got, "conn-1")
	}
	if got := c.DegradedReason(); got != "" {
		t.Errorf("DegradedReason() = %q, want \"\" (Reactivate+bind both succeeded)", got)
	}
	if got := c.SuspensionReason(); got != "authority_revoked" {
		t.Errorf("SuspensionReason() = %q, want %q (saved verbatim, spec §5.4)", got, "authority_revoked")
	}
	if got := c.LastAction(); got != "reactivate" {
		t.Errorf("LastAction() = %q, want %q", got, "reactivate")
	}
	if got := c.LastActionError(); got != "" {
		t.Errorf("LastActionError() = %q, want \"\"", got)
	}
	if got := c.LastLifecycleEvent(); got != le.EventType {
		t.Errorf("LastLifecycleEvent() = %q, want %q", got, le.EventType)
	}
}

func TestSubscriptionLifecycleAction_Suspended_Bot_SingleReactivate_NoBindNeeded(t *testing.T) {
	hub := NewHub()
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "bot", "app1", "")
	hub.RegisterAndIsFirst(c)

	// wireBind=false: a bot must succeed WITHOUT ever needing a bindUser
	// call configured at all (spec §5.4: "bot 只需 Reactivate 成功").
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), false)

	le := lifecycle.LifecycleEvent{EventType: "event.subscription.suspended_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "suspended", SuspensionCode: "authority_revoked"}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}

	if got := deps.client.reactivateCount(); got != 1 {
		t.Fatalf("Reactivate call count = %d, want 1", got)
	}
	if got := deps.bind.callCount(); got != 0 {
		t.Errorf("bindUser call count = %d, want 0 (bot never needs BindUser)", got)
	}
	if got := c.DegradedReason(); got != "" {
		t.Errorf("DegradedReason() = %q, want \"\"", got)
	}
}

// THE core §8 mis-recovery test: an event for a consumer whose owner does
// NOT match the current identity must trigger NO Reactivate and NO
// BindUser, regardless of a valid suspension code.
func TestSubscriptionLifecycleAction_Suspended_OwnerMismatch_NoReactivateNoBindUser(t *testing.T) {
	hub := NewHub()
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	hub.RegisterAndIsFirst(c)

	// current is a DIFFERENT user than the owner fixed at registration.
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_bob"), true)

	le := lifecycle.LifecycleEvent{EventType: "event.subscription.suspended_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "suspended", SuspensionCode: "authority_revoked"}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}

	if got := deps.client.reactivateCount(); got != 0 {
		t.Fatalf("Reactivate call count = %d, want 0 (owner != current must NEVER auto-recover)", got)
	}
	if got := deps.client.getCount(); got != 0 {
		t.Errorf("Get call count = %d, want 0", got)
	}
	if got := deps.bind.callCount(); got != 0 {
		t.Errorf("bindUser call count = %d, want 0 (must NEVER BindUser a historical/non-current identity)", got)
	}
	if got := deps.uat.callCount(); got != 0 {
		t.Errorf("resolveUAT call count = %d, want 0 (must NEVER load a UAT for a historical/non-current identity)", got)
	}
	if !c.StaleIdentity() {
		t.Error("owner!=current must be marked stale_identity")
	}
	if got := c.LastAction(); got != "" {
		t.Errorf("LastAction() = %q, want \"\" (no remote action was ever attempted)", got)
	}
	// The suspension.code snapshot itself is still recorded verbatim (that's
	// bookkeeping/display, not an action) -- only the ACTION is blocked.
	if got := c.SuspensionReason(); got != "authority_revoked" {
		t.Errorf("SuspensionReason() = %q, want %q (summary recording is unaffected by the action gate)", got, "authority_revoked")
	}
}

func TestSubscriptionLifecycleAction_Suspended_Miss_NoConsumer_NoRemoteCall(t *testing.T) {
	hub := NewHub() // nothing registered -- a miss
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)

	le := lifecycle.LifecycleEvent{EventType: "event.subscription.suspended_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-missing", State: "suspended", SuspensionCode: "authority_revoked"}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}
	if got := deps.client.reactivateCount(); got != 0 {
		t.Errorf("Reactivate call count = %d, want 0 (a miss must never touch the remote subscription)", got)
	}
	if got := deps.bind.callCount(); got != 0 {
		t.Errorf("bindUser call count = %d, want 0", got)
	}
}

// §5.4 default branch: an UNRECOGNIZED suspension.code must not guess a
// recovery action -- Reactivate must NOT be attempted; instead exactly one
// Get reconciles, and the deserialization/handling of the unknown string
// must not crash.
func TestSubscriptionLifecycleAction_Suspended_UnknownCode_DefaultBranch_GetOnceNoReactivate(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	hub.RegisterAndIsFirst(c)
	deps.client.getResp = buildGetResp("suspended", "some_future_unrecognized_code")

	le := lifecycle.LifecycleEvent{EventType: "event.subscription.suspended_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "suspended", SuspensionCode: "some_future_unrecognized_code"}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}

	if got := deps.client.reactivateCount(); got != 0 {
		t.Errorf("Reactivate call count = %d, want 0 (must not guess a recovery action for an unrecognized code)", got)
	}
	if got := deps.client.getCount(); got != 1 {
		t.Errorf("Get call count = %d, want 1 (default branch reconciles once)", got)
	}
	if got := c.DegradedReason(); got == "" {
		t.Error("DegradedReason() = \"\", want non-empty (still suspended per the reconcile)")
	}
	if got := c.SuspensionReason(); got != "some_future_unrecognized_code" {
		t.Errorf("SuspensionReason() = %q, want %q (verbatim, no closed enum)", got, "some_future_unrecognized_code")
	}
	if got := c.LastAction(); got != "get" {
		t.Errorf("LastAction() = %q, want %q", got, "get")
	}
}

func TestSubscriptionLifecycleAction_Suspended_ReactivateFails_Degraded_NextActionReactivate(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	hub.RegisterAndIsFirst(c)
	deps.client.reactivateErr = errors.New("upstream: 500")

	le := lifecycle.LifecycleEvent{EventType: "event.subscription.suspended_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "suspended", SuspensionCode: "authority_revoked"}
	if err := deps.action.Handle(context.Background(), le); err == nil {
		t.Fatal("Handle should surface the Reactivate failure")
	}

	if got := c.DegradedReason(); got == "" {
		t.Error("DegradedReason() empty after a Reactivate failure")
	}
	if got := c.NextAction(); got != lifecycle.NextActionReactivate {
		t.Errorf("NextAction() = %q, want %q", got, lifecycle.NextActionReactivate)
	}
	if got := c.LastAction(); got != "reactivate" {
		t.Errorf("LastAction() = %q, want %q", got, "reactivate")
	}
	if got := c.LastActionError(); got == "" {
		t.Error("LastActionError() empty after a Reactivate failure")
	}
	if got := deps.bind.callCount(); got != 0 {
		t.Errorf("bindUser call count = %d, want 0 (Reactivate itself failed -- BindUser must never be attempted)", got)
	}
}

// Reactivate succeeds remotely but the LOCAL bindUser fails: per spec §5.4
// the two are independent and EITHER failing means NOT running.
func TestSubscriptionLifecycleAction_Suspended_ReactivateSucceeds_BindFails_NotRunning(t *testing.T) {
	hub := NewHub()

	uat := &fakeUATResolver{uat: "uat-1"}
	fb := &fakeBindUser{failCall: map[int]error{1: errors.New("bind_user: 500")}}
	gate := newIdentityGate(hub, staticCurrent("app1", "ou_alice"), uat.resolve, discardTestLogger())
	gate.onConnReady(context.Background(), "conn-1", fb.bind) // memoize only -- no conns registered yet

	client := &fakeSubscriptionActionClient{}
	action := lifecycle.NewSubscriptionAction(hub.lifecycleRegistry(), discardTestLogger())
	action.SetIdentityGate(gate)
	action.SetNewSubscriptionClient(func(_ core.Identity, _ string) (lifecycle.SubscriptionClient, error) { return client, nil })

	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	hub.RegisterAndIsFirst(c)

	le := lifecycle.LifecycleEvent{EventType: "event.subscription.suspended_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "suspended", SuspensionCode: "authority_revoked"}
	if err := action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v (want nil -- Reactivate itself succeeded, only bind failed)", err)
	}

	if got := client.reactivateCount(); got != 1 {
		t.Fatalf("Reactivate call count = %d, want 1", got)
	}
	if got := fb.callCount(); got != 1 {
		t.Fatalf("bindUser call count = %d, want 1", got)
	}
	if got := c.BoundConnID(); got != "" {
		t.Errorf("BoundConnID() = %q, want \"\" (bind failed -- must not be considered bound/running)", got)
	}
	if got := c.DegradedReason(); got == "" {
		t.Error("DegradedReason() empty -- a failed bind after a successful Reactivate must still be degraded (spec §5.4: NOT running)")
	}
	if got := c.NextAction(); got != lifecycle.NextActionRebind {
		t.Errorf("NextAction() = %q, want %q", got, lifecycle.NextActionRebind)
	}
}

// =========================================================================
// expiration_reminder -> single Renew
// =========================================================================

func TestSubscriptionLifecycleAction_ExpirationReminder_Hit_SingleRenew_Success(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	hub.RegisterAndIsFirst(c)
	c.SetDegraded("stale_marker_from_before")

	le := lifecycle.LifecycleEvent{EventType: "event.subscription.expiration_reminder_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", ExpireTime: 123}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}

	if got := deps.client.renewCount(); got != 1 {
		t.Fatalf("Renew call count = %d, want 1", got)
	}
	if got := c.LastAction(); got != "renew" {
		t.Errorf("LastAction() = %q, want %q", got, "renew")
	}
	if got := c.DegradedReason(); got != "" {
		t.Errorf("DegradedReason() = %q, want \"\" (a successful Renew clears a prior degraded state)", got)
	}
}

func TestSubscriptionLifecycleAction_ExpirationReminder_RenewFails_Degraded_NextActionRenew(t *testing.T) {
	hub := NewHub()
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "bot", "app1", "")
	hub.RegisterAndIsFirst(c)

	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), false)
	deps.client.renewErr = errors.New("upstream: 500")

	le := lifecycle.LifecycleEvent{EventType: "event.subscription.expiration_reminder_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1"}
	if err := deps.action.Handle(context.Background(), le); err == nil {
		t.Fatal("Handle should surface the Renew failure")
	}
	// Exact reason, not just non-empty: a failed RENEW must never be
	// reported as "remote_subscription_expired" (that reason is reserved for
	// the actual expired_v1 event/reconcile outcome) -- a renew failure only
	// means the subscription is at risk of expiring soon, a distinct fact.
	if got := c.DegradedReason(); got != lifecycle.ReasonRemoteSubscriptionExpiringSoon {
		t.Errorf("DegradedReason() = %q, want %q", got, lifecycle.ReasonRemoteSubscriptionExpiringSoon)
	}
	if got := c.NextAction(); got != lifecycle.NextActionRenew {
		t.Errorf("NextAction() = %q, want %q", got, lifecycle.NextActionRenew)
	}
}

func TestSubscriptionLifecycleAction_ExpirationReminder_OwnerMismatch_NoRenew(t *testing.T) {
	hub := NewHub()
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	hub.RegisterAndIsFirst(c)

	deps := newTestAction(t, hub, staticCurrent("app1", "ou_bob"), true)
	le := lifecycle.LifecycleEvent{EventType: "event.subscription.expiration_reminder_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1"}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}
	if got := deps.client.renewCount(); got != 0 {
		t.Errorf("Renew call count = %d, want 0 (owner != current)", got)
	}
	if !c.StaleIdentity() {
		t.Error("expected stale_identity")
	}
}

func TestSubscriptionLifecycleAction_ExpirationReminder_Miss_NoRenew(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	le := lifecycle.LifecycleEvent{EventType: "event.subscription.expiration_reminder_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-missing"}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}
	if got := deps.client.renewCount(); got != 0 {
		t.Errorf("Renew call count = %d, want 0", got)
	}
}

// =========================================================================
// expired -- degraded, NO Renew/auto-Reactivate, ever.
// =========================================================================

func TestSubscriptionLifecycleAction_Expired_Hit_Degraded_NoRemoteCallAtAll(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	hub.RegisterAndIsFirst(c)
	le := lifecycle.LifecycleEvent{EventType: "event.subscription.expired_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "expired"}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}

	if got := c.DegradedReason(); got != lifecycle.ReasonRemoteSubscriptionExpired {
		t.Errorf("DegradedReason() = %q, want %q", got, lifecycle.ReasonRemoteSubscriptionExpired)
	}
	if got := c.NextAction(); got != lifecycle.NextActionRebuild {
		t.Errorf("NextAction() = %q, want %q", got, lifecycle.NextActionRebuild)
	}
	if deps.client.reactivateCount() != 0 || deps.client.renewCount() != 0 || deps.client.getCount() != 0 {
		t.Errorf("expired must NEVER call Reactivate/Renew/Get: reactivate=%d renew=%d get=%d",
			deps.client.reactivateCount(), deps.client.renewCount(), deps.client.getCount())
	}
	if deps.bind.callCount() != 0 {
		t.Errorf("bindUser call count = %d, want 0", deps.bind.callCount())
	}
}

// Even an owner-mismatched consumer gets the (purely local, no-remote-call)
// expired bookkeeping -- this is informational, not an action.
func TestSubscriptionLifecycleAction_Expired_OwnerMismatch_StillMarkedLocally_NoRemoteCall(t *testing.T) {
	hub := NewHub()
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	hub.RegisterAndIsFirst(c)

	deps := newTestAction(t, hub, staticCurrent("app1", "ou_bob"), true)
	le := lifecycle.LifecycleEvent{EventType: "event.subscription.expired_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "expired"}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}
	if got := c.DegradedReason(); got != lifecycle.ReasonRemoteSubscriptionExpired {
		t.Errorf("DegradedReason() = %q, want %q", got, lifecycle.ReasonRemoteSubscriptionExpired)
	}
	if deps.client.reactivateCount() != 0 || deps.client.renewCount() != 0 {
		t.Error("expired must never issue a remote call regardless of owner match")
	}
}

// =========================================================================
// deleted -- degraded, NO rebuild, TTL in-memory tombstone blocks a later
// resurrection via activated/updated.
// =========================================================================

func TestSubscriptionLifecycleAction_Deleted_Hit_Degraded_NoRebuild(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	hub.RegisterAndIsFirst(c)
	le := lifecycle.LifecycleEvent{EventType: "event.subscription.deleted_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1"}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}

	if got := c.DegradedReason(); got != lifecycle.ReasonRemoteSubscriptionDeleted {
		t.Errorf("DegradedReason() = %q, want %q", got, lifecycle.ReasonRemoteSubscriptionDeleted)
	}
	if got := c.NextAction(); got != lifecycle.NextActionRebuild {
		t.Errorf("NextAction() = %q, want %q", got, lifecycle.NextActionRebuild)
	}
	if got := c.RemoteState(); got != "deleted" {
		t.Errorf("RemoteState() = %q, want %q (deleted_v1's body carries no state -- synthesized, spec §5.3 \"删 active 快照\")", got, "deleted")
	}
	if deps.client.reactivateCount() != 0 {
		t.Error("deleted must never Reactivate")
	}
}

func TestSubscriptionLifecycleAction_Tombstone_BlocksLateActivatedAfterDeletedMiss(t *testing.T) {
	hub := NewHub() // no consumer registered yet when "deleted" arrives -- a miss
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)

	deleted := lifecycle.LifecycleEvent{EventType: "event.subscription.deleted_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1"}
	if err := deps.action.Handle(context.Background(), deleted); err != nil {
		t.Fatalf("Handle(deleted) returned err: %v", err)
	}

	// NOW a consumer registers for the SAME remote_subscription_id (e.g. a
	// fresh `event consume` started against a stale local snapshot) and a
	// late/out-of-order activated_v1 for it arrives.
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	hub.RegisterAndIsFirst(c)
	c.SetDegraded("pre_existing_marker") // proves the activated-success path never ran

	activated := lifecycle.LifecycleEvent{EventType: "event.subscription.activated_v1", EventID: "evt-2", RemoteSubscriptionID: "sub-1", State: "active"}
	if err := deps.action.Handle(context.Background(), activated); err != nil {
		t.Fatalf("Handle(activated) returned err: %v", err)
	}

	if got := deps.bind.callCount(); got != 0 {
		t.Errorf("bindUser call count = %d, want 0 (tombstoned activated must never resurrect/bind)", got)
	}
	if got := c.DegradedReason(); got != "pre_existing_marker" {
		t.Errorf("DegradedReason() = %q, want unchanged %q (tombstoned event must not run the normal activated success path)", got, "pre_existing_marker")
	}
}

func TestSubscriptionLifecycleAction_Tombstone_ExpiresAfterTTL_AllowsLateResurrection(t *testing.T) {
	saved := lifecycle.TombstoneTTL
	lifecycle.TombstoneTTL = 20 * time.Millisecond
	defer func() { lifecycle.TombstoneTTL = saved }()

	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)

	deleted := lifecycle.LifecycleEvent{EventType: "event.subscription.deleted_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1"}
	if err := deps.action.Handle(context.Background(), deleted); err != nil {
		t.Fatalf("Handle(deleted) returned err: %v", err)
	}

	time.Sleep(40 * time.Millisecond) // past the shrunk TTL

	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "bot", "app1", "")
	hub.RegisterAndIsFirst(c)
	activated := lifecycle.LifecycleEvent{EventType: "event.subscription.activated_v1", EventID: "evt-2", RemoteSubscriptionID: "sub-1", State: "active"}
	if err := deps.action.Handle(context.Background(), activated); err != nil {
		t.Fatalf("Handle(activated) returned err: %v", err)
	}
	if got := c.LastLifecycleEvent(); got != activated.EventType {
		t.Errorf("LastLifecycleEvent() = %q, want %q (tombstone must have expired)", got, activated.EventType)
	}
	if got := c.DegradedReason(); got != "" {
		t.Errorf("DegradedReason() = %q, want \"\" (bot activated after tombstone expiry should resolve clean)", got)
	}
}

// =========================================================================
// updated -- compatible continue / incompatible degraded+conflict / unclear
// single Get.
// =========================================================================

func TestSubscriptionLifecycleAction_Updated_Compatible_Continue(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	// This consumer's own local listening intent (issue #7) — the event
	// below must match ALL THREE dimensions (authority + target_resource +
	// include_resource_data) for this to classify compatible.
	c.SetListenIntent("im.message?chat_id=oc_1", true)
	hub.RegisterAndIsFirst(c)
	le := lifecycle.LifecycleEvent{
		EventType: "event.subscription.updated_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "active",
		Authority: "user:ou_alice", TargetResource: "im.message?chat_id=oc_1",
		IncludeResourceData: true, PayloadOptionsPresent: true,
	}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}
	if got := c.DegradedReason(); got != "" {
		t.Errorf("DegradedReason() = %q, want \"\" (compatible update)", got)
	}
	if deps.client.getCount() != 0 {
		t.Errorf("Get call count = %d, want 0 (compatible -- no reconcile needed)", deps.client.getCount())
	}
}

// TestSubscriptionLifecycleAction_Updated_DifferingTargetResource_DegradedConflict
// locks issue #7: a remote target_resource change is caught even when
// Authority still matches — the old code compared Authority alone and would
// have mis-judged this "compatible".
func TestSubscriptionLifecycleAction_Updated_DifferingTargetResource_DegradedConflict(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	c.SetListenIntent("im.message?chat_id=oc_1", true)
	hub.RegisterAndIsFirst(c)

	le := lifecycle.LifecycleEvent{
		EventType: "event.subscription.updated_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "active",
		Authority: "user:ou_alice", TargetResource: "im.message?chat_id=oc_DIFFERENT",
		IncludeResourceData: true, PayloadOptionsPresent: true,
	}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}
	if got := c.DegradedReason(); got != lifecycle.ReasonRemoteSubscriptionConflict {
		t.Errorf("DegradedReason() = %q, want %q (target_resource changed remotely)", got, lifecycle.ReasonRemoteSubscriptionConflict)
	}
	if deps.client.getCount() != 0 {
		t.Errorf("Get call count = %d, want 0 (a clear target_resource mismatch needs no reconcile)", deps.client.getCount())
	}
}

// TestSubscriptionLifecycleAction_Updated_DifferingIncludeResourceData_DegradedConflict
// locks issue #7: a remote payload_options.include_resource_data change is
// caught even when Authority AND target_resource still match.
func TestSubscriptionLifecycleAction_Updated_DifferingIncludeResourceData_DegradedConflict(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	c.SetListenIntent("im.message?chat_id=oc_1", true) // this consumer's own ENCRYPTED intent
	hub.RegisterAndIsFirst(c)

	le := lifecycle.LifecycleEvent{
		EventType: "event.subscription.updated_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "active",
		Authority: "user:ou_alice", TargetResource: "im.message?chat_id=oc_1",
		IncludeResourceData: false, PayloadOptionsPresent: true, // remote flipped to plaintext
	}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}
	if got := c.DegradedReason(); got != lifecycle.ReasonRemoteSubscriptionConflict {
		t.Errorf("DegradedReason() = %q, want %q (include_resource_data changed remotely)", got, lifecycle.ReasonRemoteSubscriptionConflict)
	}
	if deps.client.getCount() != 0 {
		t.Errorf("Get call count = %d, want 0 (a clear include_resource_data mismatch needs no reconcile)", deps.client.getCount())
	}
}

// TestSubscriptionLifecycleAction_Updated_MissingPayloadOptions_SingleGetReconcile
// locks issue #7's "insufficient info -> Get, never guess compatible" rule
// for the NEW dimension specifically: Authority and target_resource both
// match, but the after snapshot didn't carry payload_options at all.
func TestSubscriptionLifecycleAction_Updated_MissingPayloadOptions_SingleGetReconcile(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	c.SetListenIntent("im.message?chat_id=oc_1", true)
	hub.RegisterAndIsFirst(c)
	deps.client.getResp = buildGetResp("active", "")

	le := lifecycle.LifecycleEvent{
		EventType: "event.subscription.updated_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "active",
		Authority: "user:ou_alice", TargetResource: "im.message?chat_id=oc_1",
		// PayloadOptionsPresent deliberately left false: the after snapshot
		// didn't carry payload_options.include_resource_data at all.
	}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}
	if got := deps.client.getCount(); got != 1 {
		t.Errorf("Get call count = %d, want 1 (include_resource_data unknown -> single Get reconcile, never guessed compatible)", got)
	}
	if got := deps.client.reactivateCount(); got != 0 {
		t.Errorf("Reactivate call count = %d, want 0", got)
	}
}

func TestSubscriptionLifecycleAction_Updated_Incompatible_DegradedConflict(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	hub.RegisterAndIsFirst(c)
	le := lifecycle.LifecycleEvent{EventType: "event.subscription.updated_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "active", Authority: "app"}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}
	if got := c.DegradedReason(); got != lifecycle.ReasonRemoteSubscriptionConflict {
		t.Errorf("DegradedReason() = %q, want %q", got, lifecycle.ReasonRemoteSubscriptionConflict)
	}
	if deps.client.getCount() != 0 {
		t.Errorf("Get call count = %d, want 0 (a clear incompatibility needs no reconcile)", deps.client.getCount())
	}
}

func TestSubscriptionLifecycleAction_Updated_UnclearAuthority_SingleGetReconcile(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	hub.RegisterAndIsFirst(c)
	deps.client.getResp = buildGetResp("active", "")

	le := lifecycle.LifecycleEvent{EventType: "event.subscription.updated_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "active", Authority: ""}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}
	if got := deps.client.getCount(); got != 1 {
		t.Errorf("Get call count = %d, want 1 (order unclear -> single Get reconcile)", got)
	}
	if got := deps.client.reactivateCount(); got != 0 {
		t.Errorf("Reactivate call count = %d, want 0", got)
	}
	if got := deps.client.renewCount(); got != 0 {
		t.Errorf("Renew call count = %d, want 0", got)
	}
}

// =========================================================================
// reconcileWithGet's "active" branch (issue #23): state=="active" alone is
// not proof this consumer's own local intent is still honored -- the Get
// response is additionally projected into the same 3-dimension shape
// classifyUpdateCompatibility already compares an updated_v1 After snapshot
// through, and compared against the lead conn's stored intent.
// =========================================================================

// TestSubscriptionLifecycleAction_ReconcileWithGet_ActiveCompatible_ClearsDegraded
// locks the positive case: a Get response reporting active AND agreeing with
// this consumer's stored target_resource/authority/include_resource_data on
// all 3 dimensions clears a prior degraded state.
func TestSubscriptionLifecycleAction_ReconcileWithGet_ActiveCompatible_ClearsDegraded(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	c.SetListenIntent("im.message?chat_id=oc_1", true)
	c.SetDegraded(lifecycle.ReasonRemoteSubscriptionConflict) // simulate an earlier degraded evaluation
	hub.RegisterAndIsFirst(c)
	deps.client.getResp = buildGetRespActive("im.message?chat_id=oc_1", "ou_alice", true)

	// Authority=="" on the event forces classifyUpdateCompatibility to
	// "unclear", triggering the single Get reconcile this test exercises.
	le := lifecycle.LifecycleEvent{EventType: "event.subscription.updated_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "active", Authority: ""}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}
	if got := deps.client.getCount(); got != 1 {
		t.Fatalf("Get call count = %d, want 1", got)
	}
	if got := c.DegradedReason(); got != "" {
		t.Errorf("DegradedReason() = %q, want \"\" (Get confirmed active AND all 3 dimensions compatible)", got)
	}
}

// TestSubscriptionLifecycleAction_ReconcileWithGet_ActiveIncompatibleTargetResource_StaysDegraded
// locks the negative case for target_resource: the Get response reports
// active, but its target_resource disagrees with this consumer's own stored
// intent -- state=="active" must NOT by itself clear degraded.
func TestSubscriptionLifecycleAction_ReconcileWithGet_ActiveIncompatibleTargetResource_StaysDegraded(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	c.SetListenIntent("im.message?chat_id=oc_1", true)
	hub.RegisterAndIsFirst(c)
	deps.client.getResp = buildGetRespActive("im.message?chat_id=oc_DIFFERENT", "ou_alice", true)

	le := lifecycle.LifecycleEvent{EventType: "event.subscription.updated_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "active", Authority: ""}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}
	if got := c.DegradedReason(); got != lifecycle.ReasonRemoteSubscriptionConflict {
		t.Errorf("DegradedReason() = %q, want %q (Get's own target_resource disagrees)", got, lifecycle.ReasonRemoteSubscriptionConflict)
	}
	if got := c.NextAction(); got != lifecycle.NextActionGet {
		t.Errorf("NextAction() = %q, want %q (same guidance updated_v1's own incompatible path uses)", got, lifecycle.NextActionGet)
	}
}

// TestSubscriptionLifecycleAction_ReconcileWithGet_ActiveIncompatibleIncludeResourceData_StaysDegraded
// locks the negative case for include_resource_data.
func TestSubscriptionLifecycleAction_ReconcileWithGet_ActiveIncompatibleIncludeResourceData_StaysDegraded(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	c.SetListenIntent("im.message?chat_id=oc_1", true) // this consumer's own ENCRYPTED intent
	hub.RegisterAndIsFirst(c)
	deps.client.getResp = buildGetRespActive("im.message?chat_id=oc_1", "ou_alice", false) // Get reports plaintext

	le := lifecycle.LifecycleEvent{EventType: "event.subscription.updated_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "active", Authority: ""}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}
	if got := c.DegradedReason(); got != lifecycle.ReasonRemoteSubscriptionConflict {
		t.Errorf("DegradedReason() = %q, want %q (Get's own include_resource_data disagrees)", got, lifecycle.ReasonRemoteSubscriptionConflict)
	}
}

// TestSubscriptionLifecycleAction_ReconcileWithGet_ActiveIncompatibleAuthority_StaysDegraded
// locks the negative case for authority.
func TestSubscriptionLifecycleAction_ReconcileWithGet_ActiveIncompatibleAuthority_StaysDegraded(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	c.SetListenIntent("im.message?chat_id=oc_1", true)
	hub.RegisterAndIsFirst(c)
	deps.client.getResp = buildGetRespActive("im.message?chat_id=oc_1", "ou_SOMEONE_ELSE", true)

	le := lifecycle.LifecycleEvent{EventType: "event.subscription.updated_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "active", Authority: ""}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}
	if got := c.DegradedReason(); got != lifecycle.ReasonRemoteSubscriptionConflict {
		t.Errorf("DegradedReason() = %q, want %q (Get's own authority disagrees)", got, lifecycle.ReasonRemoteSubscriptionConflict)
	}
}

// TestSubscriptionLifecycleAction_ReconcileWithGet_Suspended_Unchanged locks
// that the suspended branch is untouched by the active-only compatibility
// check above — it never even looks at target_resource/authority/
// include_resource_data.
func TestSubscriptionLifecycleAction_ReconcileWithGet_Suspended_Unchanged(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	c.SetListenIntent("im.message?chat_id=oc_1", true)
	hub.RegisterAndIsFirst(c)
	deps.client.getResp = buildGetResp("suspended", "some_future_unrecognized_code")

	le := lifecycle.LifecycleEvent{EventType: "event.subscription.suspended_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "suspended", SuspensionCode: "some_future_unrecognized_code"}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}
	if got := c.DegradedReason(); got == "" {
		t.Error("DegradedReason() = \"\", want non-empty (still suspended per the reconcile)")
	}
	if got := c.SuspensionReason(); got != "some_future_unrecognized_code" {
		t.Errorf("SuspensionReason() = %q, want %q", got, "some_future_unrecognized_code")
	}
}

// TestSubscriptionLifecycleAction_ReconcileWithGet_Expired_Unchanged locks
// that the expired branch is likewise untouched.
func TestSubscriptionLifecycleAction_ReconcileWithGet_Expired_Unchanged(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	c.SetListenIntent("im.message?chat_id=oc_1", true)
	hub.RegisterAndIsFirst(c)
	deps.client.getResp = buildGetResp("expired", "")

	le := lifecycle.LifecycleEvent{EventType: "event.subscription.updated_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "active", Authority: ""}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}
	if got := c.DegradedReason(); got != lifecycle.ReasonRemoteSubscriptionExpired {
		t.Errorf("DegradedReason() = %q, want %q", got, lifecycle.ReasonRemoteSubscriptionExpired)
	}
	if got := c.NextAction(); got != lifecycle.NextActionRebuild {
		t.Errorf("NextAction() = %q, want %q", got, lifecycle.NextActionRebuild)
	}
}

func TestSubscriptionLifecycleAction_Updated_Miss_NoRemoteCall(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	le := lifecycle.LifecycleEvent{EventType: "event.subscription.updated_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-missing", State: "active"}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}
	if deps.client.getCount() != 0 {
		t.Error("a miss must never issue a remote Get")
	}
}

func TestSubscriptionLifecycleAction_Updated_OwnerMismatch_NoGet(t *testing.T) {
	hub := NewHub()
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	hub.RegisterAndIsFirst(c)

	deps := newTestAction(t, hub, staticCurrent("app1", "ou_bob"), true)
	le := lifecycle.LifecycleEvent{EventType: "event.subscription.updated_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "active", Authority: ""}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}
	if deps.client.getCount() != 0 {
		t.Errorf("Get call count = %d, want 0 (owner != current -- no remote call of any kind, including read-only Get)", deps.client.getCount())
	}
}

// =========================================================================
// activated -- clear suspension bookkeeping + suspension-degraded state, but
// NEVER bind (review #11: receiving activated does not prove the subscription
// is locally started; BindUser is left to consume start / Reactivate recovery
// / reconnect).
// =========================================================================

// A user consumer on activated_v1 clears its suspension + suspension-degraded
// state, but the bus NEVER binds — the core bind-discipline invariant.
func TestSubscriptionLifecycleAction_Activated_User_ClearsSuspension_NeverBinds(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	hub.RegisterAndIsFirst(c)
	c.SetSuspensionReason("authority_revoked")
	c.SetDegraded(lifecycle.ReasonRemoteSubscriptionSuspended)
	c.SetNextAction(lifecycle.NextActionReactivate)

	le := lifecycle.LifecycleEvent{EventType: "event.subscription.activated_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "active"}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}

	if got := c.SuspensionReason(); got != "" {
		t.Errorf("SuspensionReason() = %q, want \"\" (activated clears suspension)", got)
	}
	if got := c.DegradedReason(); got != "" {
		t.Errorf("DegradedReason() = %q, want \"\" (suspension-degraded cleared on activated)", got)
	}
	if got := c.NextAction(); got != "" {
		t.Errorf("NextAction() = %q, want \"\"", got)
	}
	// The whole point of #11: activated_v1 NEVER binds.
	if got := deps.bind.callCount(); got != 0 {
		t.Errorf("bindUser call count = %d, want 0 (activated_v1 must NEVER bind)", got)
	}
	if got := c.BoundConnID(); got != "" {
		t.Errorf("BoundConnID() = %q, want \"\" (activated must not bind)", got)
	}
}

// Even with a bind fully wired AND the consumer already bound on a prior
// connection, a fresh activated_v1 issues no NEW bind — proves the bind
// call-count is 0 regardless of surrounding state (invariant lock).
func TestSubscriptionLifecycleAction_Activated_User_BindWired_StillZeroBinds(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	hub.RegisterAndIsFirst(c)

	le := lifecycle.LifecycleEvent{EventType: "event.subscription.activated_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "active"}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}
	if got := deps.bind.callCount(); got != 0 {
		t.Errorf("bindUser call count = %d, want 0 (activated_v1 must NEVER bind even with bind wired)", got)
	}
	if got := deps.uat.callCount(); got != 0 {
		t.Errorf("resolveUAT call count = %d, want 0 (no bind ⇒ no UAT mint on activated)", got)
	}
}

func TestSubscriptionLifecycleAction_Activated_Bot_ClearsWithoutBind(t *testing.T) {
	hub := NewHub()
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "bot", "app1", "")
	hub.RegisterAndIsFirst(c)
	c.SetDegraded(lifecycle.ReasonRemoteSubscriptionSuspended)

	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), false) // no bindUser wired at all
	le := lifecycle.LifecycleEvent{EventType: "event.subscription.activated_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "active"}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}
	if got := c.DegradedReason(); got != "" {
		t.Errorf("DegradedReason() = %q, want \"\" (bot activated needs no bind)", got)
	}
	if got := deps.bind.callCount(); got != 0 {
		t.Errorf("bindUser call count = %d, want 0", got)
	}
}

func TestSubscriptionLifecycleAction_Activated_OwnerMismatch_NoBindConsumerCall(t *testing.T) {
	hub := NewHub()
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	hub.RegisterAndIsFirst(c)

	deps := newTestAction(t, hub, staticCurrent("app1", "ou_bob"), true)
	le := lifecycle.LifecycleEvent{EventType: "event.subscription.activated_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "active"}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}
	if got := deps.bind.callCount(); got != 0 {
		t.Errorf("bindUser call count = %d, want 0 (owner != current)", got)
	}
	if !c.StaleIdentity() {
		t.Error("expected stale_identity")
	}
}

func TestSubscriptionLifecycleAction_Activated_Miss_NoOp(t *testing.T) {
	hub := NewHub()
	deps := newTestAction(t, hub, staticCurrent("app1", "ou_alice"), true)
	le := lifecycle.LifecycleEvent{EventType: "event.subscription.activated_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-missing", State: "active"}
	if err := deps.action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}
	if got := deps.bind.callCount(); got != 0 {
		t.Errorf("bindUser call count = %d, want 0", got)
	}
}

// =========================================================================
// Every event still performs Task 17's summary recording, regardless of
// hit/miss/eligibility (a strict superset, never a regression).
// =========================================================================

func TestSubscriptionLifecycleAction_AlwaysRecordsSummary(t *testing.T) {
	hub := NewHub()
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "user", "app1", "ou_alice")
	hub.RegisterAndIsFirst(c)

	// Owner mismatch AND no subscription client configured at all -- proves
	// the summary write is unconditional, independent of every gate.
	action := lifecycle.NewSubscriptionAction(hub.lifecycleRegistry(), discardTestLogger())
	gate := newIdentityGate(hub, staticCurrent("app1", "ou_bob"), (&fakeUATResolver{}).resolve, discardTestLogger())
	action.SetIdentityGate(gate)

	le := lifecycle.LifecycleEvent{EventType: "event.subscription.suspended_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "suspended", SuspensionCode: "authority_revoked"}
	if err := action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}
	if got := c.LastLifecycleEvent(); got != le.EventType {
		t.Errorf("LastLifecycleEvent() = %q, want %q", got, le.EventType)
	}
	if got := c.LastLifecycleEventID(); got != le.EventID {
		t.Errorf("LastLifecycleEventID() = %q, want %q", got, le.EventID)
	}
	if got := c.RemoteState(); got != "suspended" {
		t.Errorf("RemoteState() = %q, want %q", got, "suspended")
	}
}

// A Bus that never configured a SubscriptionClient at all (every Task 17
// test, and any bus not yet given credentials) must never panic on a
// lifecycle event that WOULD otherwise trigger a remote call.
func TestSubscriptionLifecycleAction_NoSubscriptionClientConfigured_NoPanic_SummaryOnly(t *testing.T) {
	hub := NewHub()
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "bot", "app1", "")
	hub.RegisterAndIsFirst(c)

	action := lifecycle.NewSubscriptionAction(hub.lifecycleRegistry(), discardTestLogger()) // no identityGate, no subClient at all

	le := lifecycle.LifecycleEvent{EventType: "event.subscription.suspended_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "suspended", SuspensionCode: "authority_revoked"}
	if err := action.Handle(context.Background(), le); err == nil {
		t.Log("Handle returned nil even with no subscription client configured -- acceptable as long as no panic and no crash")
	}
	if got := c.LastLifecycleEvent(); got != le.EventType {
		t.Errorf("LastLifecycleEvent() = %q, want %q (summary must still be recorded)", got, le.EventType)
	}
}
