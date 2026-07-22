// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"context"
	"net"
	"testing"
	"time"
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
