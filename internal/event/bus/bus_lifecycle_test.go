// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"context"
	"net"
	"testing"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"

	"github.com/larksuite/cli/internal/core"
)

// TestNewBus_ConstructsLifecycleExecutor locks bus.go's wiring: NewBus must
// always construct a lifecycleExecutor (Task 17 has no external dependency
// to inject -- unlike identityGate, the summary-only action needs nothing
// but the Bus's own Hub -- so this is unconditional, unlike SetIdentityProviders).
func TestNewBus_ConstructsLifecycleExecutor(t *testing.T) {
	b := NewBus("test-app", "test-secret", "", nil, discardTestLogger())
	defer b.lifecycleExecutor.Cancel()

	if b.lifecycleExecutor == nil {
		t.Fatal("NewBus did not construct a lifecycleExecutor")
	}
}

// TestNewBus_LifecycleExecutorWiredToOwnHub is a functional proof that the
// executor NewBus constructs operates on THIS Bus's own Hub instance: submit
// a lifecycle event straight through b.lifecycleExecutor (exactly as
// FeishuSource.OnLifecycleEvent would from startSources's wiring) and observe
// the summary land on a Conn registered on b.hub.
func TestNewBus_LifecycleExecutorWiredToOwnHub(t *testing.T) {
	b := NewBus("test-app", "test-secret", "", nil, discardTestLogger())
	defer b.lifecycleExecutor.Cancel()

	conn, _ := net.Pipe()
	defer conn.Close()
	c := NewConn(conn, nil, "im.msg", []string{"im.message.receive_v1"}, 1, "")
	c.SetRemoteSubscriptionID("sub-1")
	b.hub.RegisterAndIsFirst(c)

	b.lifecycleExecutor.Submit(context.Background(), LifecycleEvent{
		EventType: "event.subscription.activated_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "active",
	})

	deadline := time.Now().Add(2 * time.Second)
	for c.LastLifecycleEvent() == "" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := c.LastLifecycleEvent(); got != "event.subscription.activated_v1" {
		t.Errorf("LastLifecycleEvent() = %q, want %q (executor must be wired to b's own hub)", got, "event.subscription.activated_v1")
	}
}

// --- Task 18: NewBus's default action + SetSubscriptionClient/SetIdentityProviders wiring ---

// TestNewBus_DefaultLifecycleAction_IsSubscriptionLifecycleAction locks
// Task 18's bus.go change: NewBus must construct the REAL action (not Task
// 17's summaryLifecycleAction) so that once SetSubscriptionClient/
// SetIdentityProviders are wired, Reactivate/Renew/Get/BindUser become
// reachable without reconstructing anything.
func TestNewBus_DefaultLifecycleAction_IsSubscriptionLifecycleAction(t *testing.T) {
	b := NewBus("test-app", "test-secret", "", nil, discardTestLogger())
	defer b.lifecycleExecutor.Cancel()

	if b.lifecycleAction == nil {
		t.Fatal("NewBus did not construct a lifecycleAction")
	}
	if b.lifecycleAction.identityGate != nil {
		t.Error("a fresh Bus's lifecycleAction must start with identityGate unconfigured (nil)")
	}
	if b.lifecycleAction.newSubClient != nil {
		t.Error("a fresh Bus's lifecycleAction must start with newSubClient unconfigured (nil)")
	}
}

// TestBus_SetSubscriptionClient_WiresFactory locks the new injection seam:
// after SetSubscriptionClient(sdk), the lifecycle action's newSubClient
// factory must be non-nil and able to build a working client for a bot
// identity (which needs no uat) WITHOUT ever making a network call itself —
// eventlib.NewSubscriptionClient's own construction is purely local
// (identityOptions), matching NewSubscriptionClient's own doc.
func TestBus_SetSubscriptionClient_WiresFactory(t *testing.T) {
	b := NewBus("test-app", "test-secret", "", nil, discardTestLogger())
	defer b.lifecycleExecutor.Cancel()

	sdk := lark.NewClient("test-app", "test-secret")
	b.SetSubscriptionClient(sdk)

	if b.lifecycleAction.newSubClient == nil {
		t.Fatal("SetSubscriptionClient did not wire newSubClient")
	}
	client, err := b.lifecycleAction.newSubClient(core.AsBot, "")
	if err != nil {
		t.Fatalf("newSubClient(AsBot, \"\") returned err: %v", err)
	}
	if client == nil {
		t.Fatal("newSubClient(AsBot, \"\") returned a nil client")
	}
}

// TestBus_SetSubscriptionClient_Nil_NoOp mirrors SetIdentityProviders(nil)'s
// own no-op convention.
func TestBus_SetSubscriptionClient_Nil_NoOp(t *testing.T) {
	b := NewBus("test-app", "test-secret", "", nil, discardTestLogger())
	defer b.lifecycleExecutor.Cancel()

	b.SetSubscriptionClient(nil)
	if b.lifecycleAction.newSubClient != nil {
		t.Error("SetSubscriptionClient(nil) must be a no-op")
	}
}

// TestBus_SetIdentityProviders_WiresLifecycleActionsIdentityGate locks that
// Task 18's lifecycleAction shares the SAME identityGate SetIdentityProviders
// already constructs for the delivery/bind gates — not a second, independent
// one.
func TestBus_SetIdentityProviders_WiresLifecycleActionsIdentityGate(t *testing.T) {
	b := NewBus("test-app", "test-secret", "", nil, discardTestLogger())
	defer b.lifecycleExecutor.Cancel()

	b.SetIdentityProviders(func(ctx context.Context, appID, userOpenID string) (string, error) {
		return "uat", nil
	})

	if b.lifecycleAction.identityGate == nil {
		t.Fatal("SetIdentityProviders did not wire the lifecycle action's identityGate")
	}
	if b.lifecycleAction.identityGate != b.identityGate {
		t.Error("lifecycleAction.identityGate must be the SAME instance as b.identityGate, not a second one")
	}
}
