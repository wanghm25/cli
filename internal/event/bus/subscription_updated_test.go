// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"log"
	"net"
	"testing"
	"time"

	"github.com/larksuite/cli/internal/event/bus/lifecycle"
	"github.com/larksuite/cli/internal/event/protocol"
)

// newLifecycleTestBus builds a Bus wired with a hub + a real lifecycle
// SubscriptionAction over that SAME hub (mirroring NewBus's own wiring) plus a
// test-friendly idleTimer, so a SubscriptionUpdated signal can be dispatched
// through the real handleConn path against a registered consumer. No identity
// gate is wired, so the lifecycle action's eligibility gate treats bot/legacy
// consumers as always eligible (exactly as the updated_v1 path does).
func newLifecycleTestBus(t *testing.T, logger *log.Logger) *Bus {
	t.Helper()
	hub := NewHub()
	reg := hub.lifecycleRegistry()
	action := lifecycle.NewSubscriptionAction(reg, logger)
	return &Bus{
		appID:           "app_123",
		hub:             hub,
		logger:          logger,
		conns:           make(map[*Conn]struct{}),
		idleTimer:       time.NewTimer(30 * time.Second),
		shutdownCh:      make(chan struct{}, 1),
		lifecycleAction: action,
	}
}

// deliverSubscriptionUpdated drives a SubscriptionUpdated frame through the REAL
// IPC dispatch (handleConn -> handleSubscriptionUpdated), blocking until the bus
// has processed it. handleSubscriptionUpdated degrades synchronously before it
// closes the conn, so the effect is observable once handleConn returns.
func deliverSubscriptionUpdated(t *testing.T, b *Bus, remoteSubscriptionID string) {
	t.Helper()
	server, client := net.Pipe()
	t.Cleanup(func() { server.Close(); client.Close() })
	done := make(chan struct{})
	go func() {
		b.handleConn(server)
		close(done)
	}()
	if err := protocol.Encode(client, protocol.NewSubscriptionUpdated(remoteSubscriptionID)); err != nil {
		t.Fatalf("encode SubscriptionUpdated: %v", err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handleConn did not return within 3s")
	}
}

// A SubscriptionUpdated signal for a running consumer's remote_subscription_id
// proactively degrades it with the SAME outcome an incompatible updated_v1
// produces: remote_subscription_conflict + next_action=get — reusing the
// lifecycle action's degrade path, never a bespoke routine.
func TestHandleSubscriptionUpdated_DegradesMatchedConsumer_ViaUpdatedV1Outcome(t *testing.T) {
	b := newLifecycleTestBus(t, discardTestLogger())

	// A running bot consumer bound to sub_upd (bot: always eligible, no gate).
	ack := readAckFromClient(t, b, &protocol.Hello{
		PID:                  6400,
		EventKey:             "im.message.created_v1/chat-id/oc_1",
		EventTypes:           []string{"im.message.created_v1"},
		Identity:             "bot",
		RemoteSubscriptionID: "sub_upd",
		TargetResource:       "im.message?chat_id=oc_1",
	})
	if ack.Rejected {
		t.Fatalf("bot consumer unexpectedly rejected: %q", ack.RejectReason)
	}
	conns := b.hub.connsByRemoteSubscriptionID("sub_upd")
	if len(conns) != 1 {
		t.Fatalf("connsByRemoteSubscriptionID(sub_upd) = %d, want 1", len(conns))
	}
	c := conns[0]
	if got := c.SubscriptionDegradedReason(); got != "" {
		t.Fatalf("consumer was degraded before any signal: %q", got)
	}

	deliverSubscriptionUpdated(t, b, "sub_upd")

	if got := c.SubscriptionDegradedReason(); got != lifecycle.ReasonRemoteSubscriptionConflict {
		t.Errorf("SubscriptionDegradedReason() = %q, want %q (the updated_v1 incompatible outcome)",
			got, lifecycle.ReasonRemoteSubscriptionConflict)
	}
	if got := c.NextAction(); got != lifecycle.NextActionGet {
		t.Errorf("NextAction() = %q, want %q", got, lifecycle.NextActionGet)
	}
}

// A SubscriptionUpdated for an id NO local consumer is bound to is a harmless
// no-op (a miss), never an error — mirroring the lifecycle handlers' own
// miss-is-not-an-error contract.
func TestHandleSubscriptionUpdated_UnknownID_NoOp(t *testing.T) {
	b := newLifecycleTestBus(t, discardTestLogger())

	ack := readAckFromClient(t, b, &protocol.Hello{
		PID:                  6401,
		EventKey:             "im.message.created_v1/chat-id/oc_2",
		EventTypes:           []string{"im.message.created_v1"},
		Identity:             "bot",
		RemoteSubscriptionID: "sub_other",
		TargetResource:       "im.message?chat_id=oc_2",
	})
	if ack.Rejected {
		t.Fatalf("bot consumer unexpectedly rejected: %q", ack.RejectReason)
	}

	// Signal a DIFFERENT id: the running consumer must be left untouched.
	deliverSubscriptionUpdated(t, b, "sub_nobody")

	c := b.hub.connsByRemoteSubscriptionID("sub_other")[0]
	if got := c.SubscriptionDegradedReason(); got != "" {
		t.Errorf("a non-matching signal degraded an unrelated consumer: %q", got)
	}
}
