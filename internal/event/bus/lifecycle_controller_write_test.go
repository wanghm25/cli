// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"context"
	"testing"

	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/event/bus/lifecycle"
	"github.com/larksuite/cli/internal/event/model"
	larkgw "github.com/larksuite/cli/internal/event/platform/lark"
	subown "github.com/larksuite/cli/internal/event/subscription"
)

// --- lifecycle recovery writes flow through the subscription.Controller -------
//
// Production wires the lifecycle action's SubscriptionClient to a
// *subscription.Controller over the identity-bound gateway (bus.go
// SetSubscriptionClient), so the reducer's Reactivate/Renew EFFECT reaches the
// platform-level write THROUGH the Controller — the single remote-write path —
// rather than by the lifecycle package calling platform/lark directly. These
// tests wire that exact shape (a real Controller over a fake gateway) and assert
// the fake gateway records the write with the event's remote_subscription_id.

// fakeControllerGateway is a network-free subscription.Gateway that records the
// remote calls the Controller makes on the lifecycle action's behalf. Only the
// lifecycle-recovery surface (Reactivate/Renew, and Get for reconcile) is
// exercised; the rest are inert stubs to satisfy the interface.
type fakeControllerGateway struct {
	reactivateCalls int
	reactivateID    string
	renewCalls      int
	renewID         string
	getCalls        int
	createCalls     int
}

func (g *fakeControllerGateway) WalkSubscriptions(_ context.Context, _ larkgw.ListParams, _ func(model.RemoteSubscription) bool) (bool, error) {
	return false, nil
}

func (g *fakeControllerGateway) Get(_ context.Context, _ string) (*model.RemoteSubscription, error) {
	g.getCalls++
	return nil, nil
}

func (g *fakeControllerGateway) Create(_ context.Context, _ larkgw.CreateSpec) (*model.RemoteSubscription, error) {
	g.createCalls++
	return nil, nil
}

func (g *fakeControllerGateway) Reactivate(_ context.Context, id string) (*model.RemoteSubscription, error) {
	g.reactivateCalls++
	g.reactivateID = id
	return nil, nil
}

func (g *fakeControllerGateway) Renew(_ context.Context, id string) (*model.RemoteSubscription, error) {
	g.renewCalls++
	g.renewID = id
	return nil, nil
}

func (g *fakeControllerGateway) GetEncryptKey(_ context.Context, _ string) (string, error) {
	return "", nil
}

// newControllerBackedAction wires a lifecycle action whose SubscriptionClient is a
// real *subscription.Controller over gw — the production shape. A bot owner keeps
// the test focused on the write path (no identity gate / bind wiring needed).
func newControllerBackedAction(hub *Hub, gw subown.Gateway) *lifecycle.SubscriptionAction {
	action := lifecycle.NewSubscriptionAction(hub.lifecycleRegistry(), discardTestLogger())
	action.SetNewSubscriptionClient(func(_ core.Identity, _ string) (lifecycle.SubscriptionClient, error) {
		return subown.NewController(gw), nil
	})
	return action
}

func TestLifecycleAction_Suspended_ReactivateIssuedViaController(t *testing.T) {
	hub := NewHub()
	gw := &fakeControllerGateway{}
	action := newControllerBackedAction(hub, gw)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "bot", "app1", "")
	hub.RegisterAndIsFirst(c)

	le := lifecycle.LifecycleEvent{EventType: "event.subscription.suspended_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", State: "suspended", SuspensionCode: "authority_revoked"}
	if err := action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}

	if gw.reactivateCalls != 1 || gw.reactivateID != "sub-1" {
		t.Fatalf("gateway Reactivate via Controller = (%d, %q), want (1, \"sub-1\") — the recovery write must flow through the Controller", gw.reactivateCalls, gw.reactivateID)
	}
	if gw.createCalls != 0 || gw.renewCalls != 0 {
		t.Errorf("a reactivate must not create/renew (create=%d renew=%d)", gw.createCalls, gw.renewCalls)
	}
}

func TestLifecycleAction_ExpirationReminder_RenewIssuedViaController(t *testing.T) {
	hub := NewHub()
	gw := &fakeControllerGateway{}
	action := newControllerBackedAction(hub, gw)
	c := newLifecycleDispatchTestConn(t, 1, "sub-1", "bot", "app1", "")
	hub.RegisterAndIsFirst(c)

	le := lifecycle.LifecycleEvent{EventType: "event.subscription.expiration_reminder_v1", EventID: "evt-1", RemoteSubscriptionID: "sub-1", ExpireTime: 123}
	if err := action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle returned err: %v", err)
	}

	if gw.renewCalls != 1 || gw.renewID != "sub-1" {
		t.Fatalf("gateway Renew via Controller = (%d, %q), want (1, \"sub-1\") — the renewal write must flow through the Controller", gw.renewCalls, gw.renewID)
	}
	if gw.createCalls != 0 || gw.reactivateCalls != 0 {
		t.Errorf("a renew must not create/reactivate (create=%d reactivate=%d)", gw.createCalls, gw.reactivateCalls)
	}
}
