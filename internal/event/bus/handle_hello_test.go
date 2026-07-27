// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/protocol"
)

// --- R2 #15: bus fetches the encrypt_key ONCE at handleHello for an encrypted
// refined consumer; a fetch failure REJECTS the Hello (decrypt_key_unavailable)
// so the consumer never registers/readies. ---

// readAckFromClient runs handleHello on a pipe and returns the decoded HelloAck.
func readAckFromClient(t *testing.T, b *Bus, hello *protocol.Hello) *protocol.HelloAck {
	t.Helper()
	server, client := net.Pipe()
	t.Cleanup(func() { server.Close(); client.Close() })
	go b.handleHello(server, bufio.NewReader(server), hello)
	line, err := protocol.ReadFrame(bufio.NewReader(client))
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}
	msg, err := protocol.Decode(bytes.TrimRight(line, "\n"))
	if err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	ack, ok := msg.(*protocol.HelloAck)
	if !ok {
		t.Fatalf("got %T, want *HelloAck", msg)
	}
	return ack
}

// newEncryptedHelloBus builds a Bus with a wired encrypt-key provider (fake
// GetEncryptKey client + static-identity gate) for the handleHello tests.
func newEncryptedHelloBus(t *testing.T, logger *log.Logger, cli encryptKeyClient, current currentIdentity) *Bus {
	t.Helper()
	hub := NewHub()
	p := newEncryptKeyProvider()
	p.setNewClient(func(core.Identity, string) (encryptKeyClient, error) { return cli, nil })
	p.setIdentityGate(gateWith(hub,
		func() (currentIdentity, error) { return current, nil },
		func(context.Context, string, string) (string, error) { return "uat-fresh", nil }))
	return &Bus{
		appID:              "app_123",
		hub:                hub,
		logger:             logger,
		conns:              make(map[*Conn]struct{}),
		idleTimer:          time.NewTimer(30 * time.Second),
		shutdownCh:         make(chan struct{}, 1),
		encryptKeyProvider: p,
	}
}

// Encrypted USER consumer (owner==current), key fetch succeeds -> ack NOT
// rejected, consumer registered, key cached in the SDK static provider
// (dispatcher will hit it). Resource data is user-only, so the owner is a user.
func TestHandleHello_EncryptedConsumer_KeyFetchSuccess_RegistersAndCaches(t *testing.T) {
	b := newEncryptedHelloBus(t, log.New(io.Discard, "", 0), &fakeEncryptKeyClient{key: "K_OK"},
		currentIdentity{appID: "app_123", userOpenID: "ou_me"})
	hello := &protocol.Hello{
		PID:                  7001,
		EventKey:             "im.message.receive_v1/chat-id/oc_1",
		EventTypes:           []string{"im.message.receive_v1"},
		Identity:             "user",
		UserOpenID:           "ou_me",
		RemoteSubscriptionID: "sub_enc_ok",
		TargetResource:       "im.message?chat_id=oc_1",
		IncludeResourceData:  true,
	}
	ack := readAckFromClient(t, b, hello)
	if ack.Rejected {
		t.Fatalf("encrypted consumer with a successful key fetch must be accepted, got rejected: %q", ack.RejectReason)
	}
	if got := b.hub.ConnCount(); got != 1 {
		t.Errorf("hub.ConnCount = %d, want 1 (consumer must register on success)", got)
	}
	if key, ok := b.encryptKeyProvider.dispatcherProvider().EncryptKey(context.Background(), "sub_enc_ok"); !ok || key != "K_OK" {
		t.Errorf("cached key = (%q,%v), want (K_OK,true) after a successful Hello-time fetch", key, ok)
	}
}

// Encrypted consumer, key fetch FAILS -> Hello REJECTED with
// decrypt_key_unavailable, consumer NOT registered, no key cached, and the
// error/key never appears on the wire or in the log (redaction).
func TestHandleHello_EncryptedConsumer_KeyFetchFailure_RejectsNotRegistered(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	const secretish = "leaky-detail-that-must-not-surface"
	b := newEncryptedHelloBus(t, logger, &fakeEncryptKeyClient{err: errors.New(secretish)},
		currentIdentity{appID: "app_123", userOpenID: "ou_me"})
	hello := &protocol.Hello{
		PID:                  7002,
		EventKey:             "im.message.receive_v1/chat-id/oc_2",
		EventTypes:           []string{"im.message.receive_v1"},
		Identity:             "user",
		UserOpenID:           "ou_me",
		RemoteSubscriptionID: "sub_enc_fail",
		TargetResource:       "im.message?chat_id=oc_2",
		IncludeResourceData:  true,
	}
	ack := readAckFromClient(t, b, hello)
	if !ack.Rejected {
		t.Fatal("encrypted consumer with a failed key fetch must be rejected")
	}
	if ack.RejectReason != protocol.RejectReasonDecryptKeyUnavailable {
		t.Errorf("reject reason = %q, want %q", ack.RejectReason, protocol.RejectReasonDecryptKeyUnavailable)
	}
	if got := b.hub.ConnCount(); got != 0 {
		t.Errorf("hub.ConnCount = %d, want 0 (a rejected consumer must NEVER register)", got)
	}
	if _, ok := b.encryptKeyProvider.dispatcherProvider().EncryptKey(context.Background(), "sub_enc_fail"); ok {
		t.Errorf("a failed fetch must never cache a key")
	}
	// Redaction: neither the wire reject reason nor the log carries the raw
	// fetch-error detail — only the fixed classification.
	if strings.Contains(ack.RejectReason, secretish) {
		t.Errorf("reject reason leaked the raw fetch error: %q", ack.RejectReason)
	}
	if strings.Contains(buf.String(), secretish) {
		t.Errorf("bus.log leaked the raw fetch error; log:\n%s", buf.String())
	}
}

// Encrypted USER consumer whose owner != current -> the §8 red line rejects the
// Hello (no fetch, no historical UAT), consumer not registered.
func TestHandleHello_EncryptedUserConsumer_OwnerMismatch_Rejected(t *testing.T) {
	// current is a DIFFERENT user than the Hello's owner.
	fake := &fakeEncryptKeyClient{key: "SHOULD_NOT_FETCH"}
	b := newEncryptedHelloBus(t, log.New(io.Discard, "", 0), fake,
		currentIdentity{appID: "app_123", userOpenID: "ou_current"})
	hello := &protocol.Hello{
		PID:                  7003,
		EventKey:             "im.message.receive_v1/chat-id/oc_3",
		EventTypes:           []string{"im.message.receive_v1"},
		Identity:             "user",
		UserOpenID:           "ou_owner",
		RemoteSubscriptionID: "sub_enc_user",
		TargetResource:       "im.message?chat_id=oc_3",
		IncludeResourceData:  true,
	}
	ack := readAckFromClient(t, b, hello)
	if !ack.Rejected || ack.RejectReason != protocol.RejectReasonDecryptKeyUnavailable {
		t.Fatalf("owner!=current encrypted consumer must be rejected with decrypt_key_unavailable, got rejected=%v reason=%q", ack.Rejected, ack.RejectReason)
	}
	if got := b.hub.ConnCount(); got != 0 {
		t.Errorf("hub.ConnCount = %d, want 0", got)
	}
	if got := fake.callCount(); got != 0 {
		t.Errorf("GetEncryptKey calls = %d, want 0 (owner!=current must never fetch)", got)
	}
}

// Defensive backstop: resource data is user-only, so an encrypted BOT Hello
// (which create/consume already reject up front) must be rejected by the bus
// with decrypt_key_unavailable — NO fetch as bot, consumer not registered.
func TestHandleHello_EncryptedBotConsumer_Unsupported_Rejected(t *testing.T) {
	fake := &fakeEncryptKeyClient{key: "SHOULD_NOT_FETCH"}
	b := newEncryptedHelloBus(t, log.New(io.Discard, "", 0), fake, currentIdentity{})
	hello := &protocol.Hello{
		PID:                  7005,
		EventKey:             "im.message.receive_v1/chat-id/oc_5",
		EventTypes:           []string{"im.message.receive_v1"},
		Identity:             "bot", // bot: no owner user_open_id
		RemoteSubscriptionID: "sub_enc_bot",
		TargetResource:       "im.message?chat_id=oc_5",
		IncludeResourceData:  true,
	}
	ack := readAckFromClient(t, b, hello)
	if !ack.Rejected || ack.RejectReason != protocol.RejectReasonDecryptKeyUnavailable {
		t.Fatalf("encrypted bot consumer must be rejected with decrypt_key_unavailable, got rejected=%v reason=%q", ack.Rejected, ack.RejectReason)
	}
	if got := b.hub.ConnCount(); got != 0 {
		t.Errorf("hub.ConnCount = %d, want 0 (a rejected consumer must never register)", got)
	}
	if got := fake.callCount(); got != 0 {
		t.Errorf("GetEncryptKey calls = %d, want 0 (the bus must never fetch a key as bot)", got)
	}
}

// --- issue #7: handleHello populates the registered Conn's own local
// listening intent (target_resource + include_resource_data) from
// Hello.TargetResource/Hello.IncludeResourceData, so the updated_v1
// lifecycle handler (lifecycle.go's classifyUpdateCompatibility) can later
// compare a remote change against what this consumer actually asked for. ---

func newPlainHelloBus(t *testing.T, logger *log.Logger) *Bus {
	t.Helper()
	return &Bus{
		appID:      "app_123",
		hub:        NewHub(),
		logger:     logger,
		conns:      make(map[*Conn]struct{}),
		idleTimer:  time.NewTimer(30 * time.Second),
		shutdownCh: make(chan struct{}, 1),
	}
}

func TestHandleHello_PopulatesListenIntentFromHelloV2(t *testing.T) {
	b := newPlainHelloBus(t, log.New(io.Discard, "", 0))
	hello := &protocol.Hello{
		PID:                  8001,
		EventKey:             "im.message.created_v1/chat-id/oc_1",
		EventTypes:           []string{"im.message.created_v1"},
		Identity:             "user",
		UserOpenID:           "ou_alice",
		RemoteSubscriptionID: "sub_listen_intent",
		TargetResource:       "im.message?chat_id=oc_1",
		IncludeResourceData:  true,
	}
	ack := readAckFromClient(t, b, hello)
	if ack.Rejected {
		t.Fatalf("hello unexpectedly rejected: %q", ack.RejectReason)
	}

	conns := b.hub.connsByRemoteSubscriptionID("sub_listen_intent")
	if len(conns) != 1 {
		t.Fatalf("connsByRemoteSubscriptionID = %d conns, want 1", len(conns))
	}
	c := conns[0]
	if got := c.TargetResource(); got != "im.message?chat_id=oc_1" {
		t.Errorf("TargetResource() = %q, want %q", got, "im.message?chat_id=oc_1")
	}
	if got := c.IncludeResourceDataIntent(); !got {
		t.Errorf("IncludeResourceDataIntent() = %v, want true", got)
	}
}

// A legacy Hello (no TargetResource/IncludeResourceData set) must leave the
// registered Conn's listen intent at its zero value — never fabricated.
func TestHandleHello_LegacyHello_LeavesListenIntentEmpty(t *testing.T) {
	b := newPlainHelloBus(t, log.New(io.Discard, "", 0))
	hello := &protocol.Hello{
		PID:        8002,
		EventKey:   "mail.x",
		EventTypes: []string{"mail.x"},
	}
	ack := readAckFromClient(t, b, hello)
	if ack.Rejected {
		t.Fatalf("hello unexpectedly rejected: %q", ack.RejectReason)
	}

	var found *Conn
	for c := range b.conns {
		found = c
	}
	if found == nil {
		t.Fatal("no Conn registered")
	}
	if got := found.TargetResource(); got != "" {
		t.Errorf("TargetResource() = %q, want \"\" for a legacy Hello", got)
	}
	if got := found.IncludeResourceDataIntent(); got {
		t.Errorf("IncludeResourceDataIntent() = %v, want false for a legacy Hello", got)
	}
}

// --- refined Hello completeness gate: a refined consumer
// (RemoteSubscriptionID set) MUST declare its target_resource, since it always
// builds one from the resolved selector key. An empty one is a malformed
// registration and the bus fails closed BEFORE acking — the consumer never
// registers or readies. Legacy Hellos are untouched. ---

// A refined Hello missing target_resource is rejected with the fixed
// incomplete_refined_hello reason and is never registered.
func TestHandleHello_IncompleteRefinedHello_Rejected(t *testing.T) {
	var buf bytes.Buffer
	b := newPlainHelloBus(t, log.New(&buf, "", 0))
	hello := &protocol.Hello{
		PID:                  8003,
		EventKey:             "im.message.created_v1/chat-id/oc_1",
		EventTypes:           []string{"im.message.created_v1"},
		Identity:             "user",
		UserOpenID:           "ou_alice",
		RemoteSubscriptionID: "sub_incomplete",
		// TargetResource intentionally omitted -> malformed refined registration.
	}
	ack := readAckFromClient(t, b, hello)
	if !ack.Rejected {
		t.Fatal("a refined Hello without target_resource must be rejected")
	}
	if ack.RejectReason != protocol.RejectReasonIncompleteRefinedHello {
		t.Errorf("reject reason = %q, want %q", ack.RejectReason, protocol.RejectReasonIncompleteRefinedHello)
	}
	if got := b.hub.ConnCount(); got != 0 {
		t.Errorf("hub.ConnCount = %d, want 0 (a rejected refined consumer must NEVER register)", got)
	}
	if got := len(b.hub.connsByRemoteSubscriptionID("sub_incomplete")); got != 0 {
		t.Errorf("connsByRemoteSubscriptionID(sub_incomplete) = %d conns, want 0", got)
	}
	// The reject reason is a fixed token — no resolved open_id / resource value.
	if strings.Contains(ack.RejectReason, "ou_alice") || strings.Contains(ack.RejectReason, "oc_1") {
		t.Errorf("reject reason leaked a resolved value: %q", ack.RejectReason)
	}
}

// A refined Hello that DOES declare its target_resource is accepted normally:
// the completeness gate must not over-reject a well-formed refined consumer.
func TestHandleHello_RefinedHelloWithTargetResource_Accepted(t *testing.T) {
	b := newPlainHelloBus(t, log.New(io.Discard, "", 0))
	hello := &protocol.Hello{
		PID:                  8004,
		EventKey:             "im.message.created_v1/chat-id/oc_1",
		EventTypes:           []string{"im.message.created_v1"},
		Identity:             "bot",
		RemoteSubscriptionID: "sub_complete",
		TargetResource:       "im.message?chat_id=oc_1",
	}
	ack := readAckFromClient(t, b, hello)
	if ack.Rejected {
		t.Fatalf("a complete refined Hello must not be rejected, got: %q", ack.RejectReason)
	}
	if got := b.hub.ConnCount(); got != 1 {
		t.Errorf("hub.ConnCount = %d, want 1 (a complete refined consumer registers)", got)
	}
}

// A plaintext consumer (IncludeResourceData=false) never triggers a key fetch —
// the fetch client would error if called, yet the consumer registers fine.
func TestHandleHello_PlaintextConsumer_NoKeyFetch(t *testing.T) {
	b := newEncryptedHelloBus(t, log.New(io.Discard, "", 0),
		&fakeEncryptKeyClient{err: errors.New("must not be called for a plaintext consumer")}, currentIdentity{})
	hello := &protocol.Hello{
		PID:                  7004,
		EventKey:             "im.message.receive_v1",
		EventTypes:           []string{"im.message.receive_v1"},
		Identity:             "bot",
		RemoteSubscriptionID: "sub_plain",
		TargetResource:       "im.message?chat_id=oc_4",
		IncludeResourceData:  false, // plaintext
	}
	ack := readAckFromClient(t, b, hello)
	if ack.Rejected {
		t.Fatalf("plaintext consumer must never be rejected for decryption, got: %q", ack.RejectReason)
	}
	if got := b.hub.ConnCount(); got != 1 {
		t.Errorf("hub.ConnCount = %d, want 1", got)
	}
	if _, ok := b.encryptKeyProvider.dispatcherProvider().EncryptKey(context.Background(), "sub_plain"); ok {
		t.Errorf("plaintext consumer must never cache a key")
	}
}

// HelloAck write failure must unregister the conn from hub and bus before returning.
func TestHandleHello_HelloAckWriteFailureUnregisters(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	hub := NewHub()
	b := &Bus{
		hub:        hub,
		logger:     logger,
		conns:      make(map[*Conn]struct{}),
		idleTimer:  time.NewTimer(30 * time.Second),
		shutdownCh: make(chan struct{}, 1),
	}

	server, client := net.Pipe()
	client.Close()
	defer server.Close()

	hello := &protocol.Hello{
		PID:        9999,
		EventKey:   "im.msg",
		EventTypes: []string{"im.message.receive_v1"},
	}

	br := bufio.NewReader(server)

	done := make(chan struct{})
	go func() {
		b.handleHello(server, br, hello)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handleHello did not return within 3s: stuck on write or not handling the error path")
	}

	if got := hub.ConnCount(); got != 0 {
		t.Errorf("hub.ConnCount after failed HelloAck = %d, want 0 (connection must be unregistered)", got)
	}
	if got := hub.EventKeyCount("im.msg"); got != 0 {
		t.Errorf("hub.EventKeyCount(im.msg) after failed HelloAck = %d, want 0", got)
	}
	b.mu.Lock()
	remaining := len(b.conns)
	b.mu.Unlock()
	if remaining != 0 {
		t.Errorf("b.conns after failed HelloAck = %d entries, want 0", remaining)
	}
}

// TestHandleHello_LegacyClient_FallsBackToEventKey: a Hello with empty
// subscription_id registers under EventKey (today's behavior preserved).
func TestHandleHello_LegacyClient_FallsBackToEventKey(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	hub := NewHub()
	b := &Bus{
		hub:        hub,
		logger:     logger,
		conns:      make(map[*Conn]struct{}),
		idleTimer:  time.NewTimer(30 * time.Second),
		shutdownCh: make(chan struct{}, 1),
	}

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	// Legacy client: no subscription_id field (empty string).
	hello := &protocol.Hello{
		PID:            9999,
		EventKey:       "im.message",
		EventTypes:     []string{"im.message.receive_v1"},
		SubscriptionID: "", // legacy: empty, should fallback to EventKey
	}

	br := bufio.NewReader(server)

	done := make(chan struct{})
	go func() {
		b.handleHello(server, br, hello)
		close(done)
	}()

	// Read the HelloAck from client side to let handleHello complete.
	clientReader := bufio.NewReader(client)
	ackLine, err := clientReader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read HelloAck: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handleHello did not return within 3s")
	}

	// Assertions: registered under EventKey (not a qualified subscription ID).
	if got := hub.ConnCount(); got != 1 {
		t.Errorf("hub.ConnCount = %d, want 1", got)
	}
	if got := hub.EventKeyCount("im.message"); got != 1 {
		t.Errorf("hub.EventKeyCount(im.message) = %d, want 1", got)
	}
	if got := hub.SubCount("im.message"); got != 1 {
		t.Errorf("hub.SubCount(im.message) = %d, want 1 (legacy fallback to EventKey)", got)
	}
	if got := hub.SubCount("im.message:something"); got != 0 {
		t.Errorf("hub.SubCount(im.message:something) = %d, want 0 (should not exist)", got)
	}

	if ackLine == "" {
		t.Fatal("HelloAck was empty")
	}
}

// TestHandleHello_ModernClient_UsesSubscriptionID: a Hello with
// non-empty subscription_id registers under that ID, not EventKey.
func TestHandleHello_ModernClient_UsesSubscriptionID(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	hub := NewHub()
	b := &Bus{
		hub:        hub,
		logger:     logger,
		conns:      make(map[*Conn]struct{}),
		idleTimer:  time.NewTimer(30 * time.Second),
		shutdownCh: make(chan struct{}, 1),
	}

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	// Modern client: subscription_id explicitly set.
	subscriptionID := "mail.message:alice@example.com"
	hello := &protocol.Hello{
		PID:            8888,
		EventKey:       "mail.message",
		EventTypes:     []string{"mail.message.receive_v1"},
		SubscriptionID: subscriptionID, // modern: per-resource subscription
	}

	br := bufio.NewReader(server)

	done := make(chan struct{})
	go func() {
		b.handleHello(server, br, hello)
		close(done)
	}()

	// Read the HelloAck from client side to let handleHello complete.
	clientReader := bufio.NewReader(client)
	ackLine, err := clientReader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read HelloAck: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handleHello did not return within 3s")
	}

	// Assertions: registered under the subscription_id, not bare EventKey.
	if got := hub.ConnCount(); got != 1 {
		t.Errorf("hub.ConnCount = %d, want 1", got)
	}
	if got := hub.EventKeyCount("mail.message"); got != 1 {
		t.Errorf("hub.EventKeyCount(mail.message) = %d, want 1", got)
	}
	if got := hub.SubCount(subscriptionID); got != 1 {
		t.Errorf("hub.SubCount(%q) = %d, want 1 (modern: uses SubscriptionID)", subscriptionID, got)
	}
	if got := hub.SubCount("mail.message"); got != 0 {
		t.Errorf("hub.SubCount(mail.message) = %d, want 0 (modern: NOT registered under bare EventKey)", got)
	}

	if ackLine == "" {
		t.Fatal("HelloAck was empty")
	}
}

// findSubscriberByPID whitebox-iterates hub.subscribers (same package) to
// recover the Conn handleHello registered — handleHello doesn't return it,
// and Hub.Consumers()/ConsumerInfo intentionally don't expose
// RemoteSubscriptionID (out of scope for this task).
func findSubscriberByPID(hub *Hub, pid int) (Subscriber, bool) {
	for s := range hub.subscribers {
		if s.PID() == pid {
			return s, true
		}
	}
	return nil, false
}

// TestHandleHello_PopulatesRemoteSubscriptionID: a Hello carrying a non-empty
// RemoteSubscriptionID (populated client-side by the Task-15 startup
// handshake; server-side reading of the field is this task's concern) must
// flow into the registered Conn, satisfying Subscriber.RemoteSubscriptionID()
// for Hub.Publish's refined routing (spec §4.3).
func TestHandleHello_PopulatesRemoteSubscriptionID(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	hub := NewHub()
	b := &Bus{
		hub:        hub,
		logger:     logger,
		conns:      make(map[*Conn]struct{}),
		idleTimer:  time.NewTimer(30 * time.Second),
		shutdownCh: make(chan struct{}, 1),
	}

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	const pid = 7777
	hello := &protocol.Hello{
		PID:                  pid,
		EventKey:             "im.message.receive_v1/chat-id/oc_1",
		EventTypes:           []string{"im.message.receive_v1"},
		RemoteSubscriptionID: "sub_xyz789",
		TargetResource:       "im.message?chat_id=oc_1",
	}

	br := bufio.NewReader(server)
	done := make(chan struct{})
	go func() {
		b.handleHello(server, br, hello)
		close(done)
	}()

	clientReader := bufio.NewReader(client)
	if _, err := clientReader.ReadString('\n'); err != nil {
		t.Fatalf("failed to read HelloAck: %v", err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handleHello did not return within 3s")
	}

	sub, found := findSubscriberByPID(hub, pid)
	if !found {
		t.Fatal("registered conn for pid=7777 not found in hub")
	}
	if got := sub.RemoteSubscriptionID(); got != "sub_xyz789" {
		t.Errorf("RemoteSubscriptionID() on the registered conn = %q, want %q", got, "sub_xyz789")
	}
}

// TestHandleHello_EmptyRemoteSubscriptionID_DefaultsToLegacy: a v1 Hello
// (RemoteSubscriptionID absent/empty) must register a conn whose
// RemoteSubscriptionID() is "" — the existing/legacy path is unchanged.
func TestHandleHello_EmptyRemoteSubscriptionID_DefaultsToLegacy(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	hub := NewHub()
	b := &Bus{
		hub:        hub,
		logger:     logger,
		conns:      make(map[*Conn]struct{}),
		idleTimer:  time.NewTimer(30 * time.Second),
		shutdownCh: make(chan struct{}, 1),
	}

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	const pid = 7778
	hello := &protocol.Hello{
		PID:        pid,
		EventKey:   "im.message.receive_v1",
		EventTypes: []string{"im.message.receive_v1"},
		// RemoteSubscriptionID intentionally left unset (legacy v1 client).
	}

	br := bufio.NewReader(server)
	done := make(chan struct{})
	go func() {
		b.handleHello(server, br, hello)
		close(done)
	}()

	clientReader := bufio.NewReader(client)
	if _, err := clientReader.ReadString('\n'); err != nil {
		t.Fatalf("failed to read HelloAck: %v", err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handleHello did not return within 3s")
	}

	sub, found := findSubscriberByPID(hub, pid)
	if !found {
		t.Fatal("registered conn for pid=7778 not found in hub")
	}
	if got := sub.RemoteSubscriptionID(); got != "" {
		t.Errorf("RemoteSubscriptionID() on a legacy-Hello conn = %q, want \"\" (legacy default)", got)
	}
}

// --- Task 14: owner identity fixed at registration (spec §4.4) ---

// TestHandleHello_PopulatesOwnerIdentityFromHelloFields: a Hello carrying
// Identity/Profile/UserOpenID (populated client-side by the Task-15 HelloV2;
// server-side reading is this task's concern) must flow into the registered
// Conn's owner fields. owner_app_id is the BUS's own AppID — a bus is
// per-app, and Hello carries no app_id field of its own.
func TestHandleHello_PopulatesOwnerIdentityFromHelloFields(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	hub := NewHub()
	b := &Bus{
		appID:      "app_123",
		hub:        hub,
		logger:     logger,
		conns:      make(map[*Conn]struct{}),
		idleTimer:  time.NewTimer(30 * time.Second),
		shutdownCh: make(chan struct{}, 1),
	}

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	const pid = 6001
	hello := &protocol.Hello{
		PID:        pid,
		EventKey:   "im.message.receive_v1",
		EventTypes: []string{"im.message.receive_v1"},
		Identity:   "user",
		Profile:    "work",
		UserOpenID: "ou_abc123",
	}

	br := bufio.NewReader(server)
	done := make(chan struct{})
	go func() {
		b.handleHello(server, br, hello)
		close(done)
	}()

	clientReader := bufio.NewReader(client)
	if _, err := clientReader.ReadString('\n'); err != nil {
		t.Fatalf("failed to read HelloAck: %v", err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handleHello did not return within 3s")
	}

	sub, found := findSubscriberByPID(hub, pid)
	if !found {
		t.Fatal("registered conn for pid=6001 not found in hub")
	}
	if got := sub.OwnerAppID(); got != "app_123" {
		t.Errorf("OwnerAppID() = %q, want %q (the BUS's own AppID)", got, "app_123")
	}
	if got := sub.OwnerUserOpenID(); got != "ou_abc123" {
		t.Errorf("OwnerUserOpenID() = %q, want %q", got, "ou_abc123")
	}
	c, ok := sub.(*Conn)
	if !ok {
		t.Fatalf("registered subscriber is %T, want *Conn", sub)
	}
	if got := c.OwnerIdentity(); got != "user" {
		t.Errorf("OwnerIdentity() = %q, want %q", got, "user")
	}
}

// TestHandleHello_BotIdentity_OwnerUserOpenIDStaysEmpty: a bot Hello
// (Identity=="bot") always carries UserOpenID=="" (protocol/messages.go's
// documented contract) — the registered Conn's OwnerUserOpenID() must stay
// "" so the identity gate's bot-bypass triggers (spec §4.4: bot consumers
// are NEVER identity-gated or BindUser'd).
func TestHandleHello_BotIdentity_OwnerUserOpenIDStaysEmpty(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	hub := NewHub()
	b := &Bus{
		appID:      "app_123",
		hub:        hub,
		logger:     logger,
		conns:      make(map[*Conn]struct{}),
		idleTimer:  time.NewTimer(30 * time.Second),
		shutdownCh: make(chan struct{}, 1),
	}

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	const pid = 6002
	hello := &protocol.Hello{
		PID:        pid,
		EventKey:   "im.message.receive_v1",
		EventTypes: []string{"im.message.receive_v1"},
		Identity:   "bot",
		// UserOpenID intentionally left unset, matching a real bot Hello.
	}

	br := bufio.NewReader(server)
	done := make(chan struct{})
	go func() {
		b.handleHello(server, br, hello)
		close(done)
	}()

	clientReader := bufio.NewReader(client)
	if _, err := clientReader.ReadString('\n'); err != nil {
		t.Fatalf("failed to read HelloAck: %v", err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handleHello did not return within 3s")
	}

	sub, found := findSubscriberByPID(hub, pid)
	if !found {
		t.Fatal("registered conn for pid=6002 not found in hub")
	}
	if got := sub.OwnerUserOpenID(); got != "" {
		t.Errorf("OwnerUserOpenID() for a bot Hello = %q, want \"\" (bot consumers are never identity-gated)", got)
	}
}

// TestHandleHello_LegacyHello_OwnerFieldsDefaultEmpty: a v1 Hello (no
// Identity/UserOpenID at all, pre-Task-15) must register a Conn whose owner
// fields default to "" — indistinguishable from a bot for gating purposes,
// which is the correct "not gated yet" behavior until Task 15 ships.
func TestHandleHello_LegacyHello_OwnerFieldsDefaultEmpty(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	hub := NewHub()
	b := &Bus{
		appID:      "app_123",
		hub:        hub,
		logger:     logger,
		conns:      make(map[*Conn]struct{}),
		idleTimer:  time.NewTimer(30 * time.Second),
		shutdownCh: make(chan struct{}, 1),
	}

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	const pid = 6003
	hello := &protocol.Hello{
		PID:        pid,
		EventKey:   "im.message.receive_v1",
		EventTypes: []string{"im.message.receive_v1"},
	}

	br := bufio.NewReader(server)
	done := make(chan struct{})
	go func() {
		b.handleHello(server, br, hello)
		close(done)
	}()

	clientReader := bufio.NewReader(client)
	if _, err := clientReader.ReadString('\n'); err != nil {
		t.Fatalf("failed to read HelloAck: %v", err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handleHello did not return within 3s")
	}

	sub, found := findSubscriberByPID(hub, pid)
	if !found {
		t.Fatal("registered conn for pid=6003 not found in hub")
	}
	if got := sub.OwnerUserOpenID(); got != "" {
		t.Errorf("OwnerUserOpenID() for a legacy v1 Hello = %q, want \"\"", got)
	}
}

// --- handleHello: binding a user consumer that joins an already-ready WS ---
//
// onConnReady only binds the consumers registered at the moment the WS became
// ready (or on each reconnect). A user consumer that Hello's against an
// already-running bus is therefore never BindUser'd by onConnReady alone, so
// handleHello binds it — BEFORE acking — when the WS is already ready, and
// fails closed (rejecting the Hello) if that bind fails, so the consumer never
// readies while it would silently receive no events. When the WS is not ready
// it leaves the consumer for the next onConnReady (never prematurely degraded).
// Bot/legacy consumers (empty owner user_open_id) are never bound or rejected.

// newBindTestBus builds a Bus with a wired identity gate (and its hub) for the
// handleHello-bind tests — no encrypt-key provider (plaintext consumers only).
func newBindTestBus(t *testing.T, gate *identityGate) *Bus {
	t.Helper()
	return &Bus{
		appID:        "app1",
		hub:          gate.hub,
		logger:       discardTestLogger(),
		conns:        make(map[*Conn]struct{}),
		idleTimer:    time.NewTimer(30 * time.Second),
		shutdownCh:   make(chan struct{}, 1),
		identityGate: gate,
	}
}

// runHelloToCompletion runs handleHello and blocks until it fully returns,
// returning the decoded HelloAck. handleHello now binds a WS-already-ready user
// consumer BEFORE acking (rejecting the Hello if the bind fails), so waiting
// for the full return makes both the ack (accepted or rejected) and the bind's
// effect on the registered Conn observable without racing.
func runHelloToCompletion(t *testing.T, b *Bus, hello *protocol.Hello) *protocol.HelloAck {
	t.Helper()
	server, client := net.Pipe()
	t.Cleanup(func() { server.Close(); client.Close() })
	done := make(chan struct{})
	go func() {
		b.handleHello(server, bufio.NewReader(server), hello)
		close(done)
	}()
	line, err := protocol.ReadFrame(bufio.NewReader(client))
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}
	msg, err := protocol.Decode(bytes.TrimRight(line, "\n"))
	if err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	ack, ok := msg.(*protocol.HelloAck)
	if !ok {
		t.Fatalf("got %T, want *HelloAck", msg)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handleHello did not return within 3s")
	}
	return ack
}

// A user consumer registered while the WS is ALREADY ready is bound right away.
func TestHandleHello_UserConsumer_WSAlreadyReady_BindsImmediately(t *testing.T) {
	h := NewHub()
	uat := &fakeUATResolver{uat: "uat-for-alice"}
	fb := &fakeBindUser{}
	gate := newIdentityGate(h, staticCurrent("app1", "ou_alice"), uat.resolve, discardTestLogger())
	// Simulate the WS becoming ready BEFORE this consumer exists: memoizes the
	// (connID, bindUser) pair but binds nothing (no user conns registered yet).
	gate.onConnReady(context.Background(), "conn-1", fb.bind)

	b := newBindTestBus(t, gate)
	ack := runHelloToCompletion(t, b, &protocol.Hello{
		PID: 5101, EventKey: "im.message.receive_v1", EventTypes: []string{"im.message.receive_v1"},
		Identity: "user", UserOpenID: "ou_alice",
	})
	if ack.Rejected {
		t.Fatalf("a successful bind must produce a normal HelloAck, got rejected: %q", ack.RejectReason)
	}

	sub, found := findSubscriberByPID(h, 5101)
	if !found {
		t.Fatal("user consumer not registered")
	}
	if got := sub.(*Conn).BoundConnID(); got != "conn-1" {
		t.Errorf("BoundConnID() = %q, want %q (must bind on an already-ready WS)", got, "conn-1")
	}
	if fb.callCount() != 1 {
		t.Errorf("bindUser call count = %d, want 1", fb.callCount())
	}
	if fb.callCount() == 1 && fb.calls[0] != "uat-for-alice" {
		t.Errorf("bindUser uat = %q, want %q", fb.calls[0], "uat-for-alice")
	}
}

// A WS-already-ready user consumer whose owner != current identity fails the
// pre-ack bind: the Hello is REJECTED (identity_bind_failed) and the consumer
// is left neither registered nor ready — never bound, no UAT ever minted.
func TestHandleHello_UserConsumer_WSReady_BindOwnerMismatch_Rejected(t *testing.T) {
	h := NewHub()
	uat := &fakeUATResolver{uat: "uat-unused"}
	fb := &fakeBindUser{}
	// current identity is a DIFFERENT user than the Hello's owner.
	gate := newIdentityGate(h, staticCurrent("app1", "ou_current"), uat.resolve, discardTestLogger())
	gate.onConnReady(context.Background(), "conn-1", fb.bind) // WS ready: memoize (connID, bindUser)

	b := newBindTestBus(t, gate)
	ack := runHelloToCompletion(t, b, &protocol.Hello{
		PID: 5105, EventKey: "im.message.receive_v1", EventTypes: []string{"im.message.receive_v1"},
		Identity: "user", UserOpenID: "ou_owner",
	})

	if !ack.Rejected {
		t.Fatal("a failed pre-ack bind must reject the Hello, not ack it")
	}
	if ack.RejectReason != protocol.RejectReasonBindFailed {
		t.Errorf("reject reason = %q, want %q", ack.RejectReason, protocol.RejectReasonBindFailed)
	}
	if _, found := findSubscriberByPID(h, 5105); found {
		t.Error("a rejected consumer must NOT be left registered in the hub")
	}
	if got := h.ConnCount(); got != 0 {
		t.Errorf("hub.ConnCount = %d, want 0 (a rejected consumer must be fully unwound)", got)
	}
	b.mu.Lock()
	remaining := len(b.conns)
	b.mu.Unlock()
	if remaining != 0 {
		t.Errorf("b.conns = %d entries, want 0 (rejected consumer must be unwound from the bus too)", remaining)
	}
	if fb.callCount() != 0 {
		t.Errorf("bindUser call count = %d, want 0 (owner!=current must never bind)", fb.callCount())
	}
	if uat.callCount() != 0 {
		t.Errorf("resolveUAT call count = %d, want 0 (must never mint a UAT for a mismatched owner)", uat.callCount())
	}
}

// A WS-already-ready user consumer whose BindUser call fails ALSO rejects the
// Hello with the SAME fixed identity_bind_failed reason (the wire carries no
// oracle for which check failed), leaves nothing registered, and never leaks
// the raw bind error onto the wire.
func TestHandleHello_UserConsumer_WSReady_BindAPIFailure_Rejected(t *testing.T) {
	h := NewHub()
	const secretish = "bind-api-detail-that-must-not-reach-the-wire"
	uat := &fakeUATResolver{uat: "uat-for-alice"}
	fb := &fakeBindUser{failCall: map[int]error{1: errors.New(secretish)}}
	gate := newIdentityGate(h, staticCurrent("app1", "ou_alice"), uat.resolve, discardTestLogger())
	gate.onConnReady(context.Background(), "conn-1", fb.bind) // WS ready: memoize (connID, bindUser)

	b := newBindTestBus(t, gate)
	ack := runHelloToCompletion(t, b, &protocol.Hello{
		PID: 5106, EventKey: "im.message.receive_v1", EventTypes: []string{"im.message.receive_v1"},
		Identity: "user", UserOpenID: "ou_alice",
	})

	if !ack.Rejected || ack.RejectReason != protocol.RejectReasonBindFailed {
		t.Fatalf("a failed BindUser must reject with %q, got rejected=%v reason=%q",
			protocol.RejectReasonBindFailed, ack.Rejected, ack.RejectReason)
	}
	if strings.Contains(ack.RejectReason, secretish) {
		t.Errorf("reject reason leaked the raw bind error: %q", ack.RejectReason)
	}
	if fb.callCount() != 1 {
		t.Errorf("bindUser call count = %d, want 1 (the bind was attempted, then failed)", fb.callCount())
	}
	if _, found := findSubscriberByPID(h, 5106); found {
		t.Error("a rejected consumer must NOT be left registered in the hub")
	}
	if got := h.ConnCount(); got != 0 {
		t.Errorf("hub.ConnCount = %d, want 0 (a rejected consumer must be fully unwound)", got)
	}
}

// A user consumer registered BEFORE the WS is ready must NOT be prematurely
// degraded — and the next onConnReady must then bind it.
func TestHandleHello_UserConsumer_WSNotReady_NotDegraded_LaterOnConnReadyBinds(t *testing.T) {
	h := NewHub()
	uat := &fakeUATResolver{uat: "uat-for-alice"}
	fb := &fakeBindUser{}
	gate := newIdentityGate(h, staticCurrent("app1", "ou_alice"), uat.resolve, discardTestLogger())

	b := newBindTestBus(t, gate)
	ack := runHelloToCompletion(t, b, &protocol.Hello{
		PID: 5102, EventKey: "im.message.receive_v1", EventTypes: []string{"im.message.receive_v1"},
		Identity: "user", UserOpenID: "ou_alice",
	})
	if ack.Rejected {
		t.Fatalf("a WS-not-ready consumer must be acked (bound later by onConnReady), got rejected: %q", ack.RejectReason)
	}

	sub, found := findSubscriberByPID(h, 5102)
	if !found {
		t.Fatal("user consumer not registered")
	}
	c := sub.(*Conn)
	if got := c.BoundConnID(); got != "" {
		t.Errorf("BoundConnID() = %q, want \"\" (WS not ready yet)", got)
	}
	if got := c.DegradedReason(); got != "" {
		t.Errorf("DegradedReason() = %q, want \"\" (a brand-new consumer must not be prematurely degraded when the WS is not up)", got)
	}
	if fb.callCount() != 0 {
		t.Errorf("bindUser call count = %d, want 0 before the WS is ready", fb.callCount())
	}

	// The WS becomes ready: onConnReady binds the now-registered consumer.
	gate.onConnReady(context.Background(), "conn-1", fb.bind)
	if got := c.BoundConnID(); got != "conn-1" {
		t.Errorf("BoundConnID() after onConnReady = %q, want %q", got, "conn-1")
	}
	if fb.callCount() != 1 {
		t.Errorf("bindUser call count after onConnReady = %d, want 1", fb.callCount())
	}
}

// A bot consumer joining an already-ready WS is NEVER bound, stale-marked, or
// degraded — bots are never identity-gated.
func TestHandleHello_BotConsumer_WSReady_NeverBound(t *testing.T) {
	h := NewHub()
	uat := &fakeUATResolver{uat: "uat-unused"}
	fb := &fakeBindUser{}
	gate := newIdentityGate(h, staticCurrent("app1", "ou_alice"), uat.resolve, discardTestLogger())
	gate.onConnReady(context.Background(), "conn-1", fb.bind)

	b := newBindTestBus(t, gate)
	ack := runHelloToCompletion(t, b, &protocol.Hello{
		PID: 5103, EventKey: "im.message.receive_v1", EventTypes: []string{"im.message.receive_v1"},
		Identity: "bot", // bot: no owner user_open_id
	})
	if ack.Rejected {
		t.Fatalf("a bot consumer must be acked, never rejected, got: %q", ack.RejectReason)
	}

	sub, found := findSubscriberByPID(h, 5103)
	if !found {
		t.Fatal("bot consumer not registered")
	}
	c := sub.(*Conn)
	if got := c.BoundConnID(); got != "" {
		t.Errorf("bot BoundConnID() = %q, want \"\" (bots are never bound)", got)
	}
	if c.StaleIdentity() {
		t.Error("bot consumer must never be marked stale_identity")
	}
	if got := c.DegradedReason(); got != "" {
		t.Errorf("bot DegradedReason() = %q, want \"\"", got)
	}
	if fb.callCount() != 0 {
		t.Errorf("bindUser call count = %d, want 0 for a bot consumer", fb.callCount())
	}
}

// A consumer already bound by handleHello must not be re-bound when onConnReady
// re-runs for the SAME connID (idempotence across the two bind drivers).
func TestHandleHello_UserConsumer_NoDoubleBind_WhenOnConnReadyReruns(t *testing.T) {
	h := NewHub()
	uat := &fakeUATResolver{uat: "uat-for-alice"}
	fb := &fakeBindUser{}
	gate := newIdentityGate(h, staticCurrent("app1", "ou_alice"), uat.resolve, discardTestLogger())
	gate.onConnReady(context.Background(), "conn-1", fb.bind)

	b := newBindTestBus(t, gate)
	ack := runHelloToCompletion(t, b, &protocol.Hello{
		PID: 5104, EventKey: "im.message.receive_v1", EventTypes: []string{"im.message.receive_v1"},
		Identity: "user", UserOpenID: "ou_alice",
	})
	if ack.Rejected {
		t.Fatalf("bind should have succeeded, got rejected: %q", ack.RejectReason)
	}
	if fb.callCount() != 1 {
		t.Fatalf("bindUser call count after handleHello = %d, want 1", fb.callCount())
	}

	// onConnReady re-runs for the same connID (e.g. a spurious ready callback):
	// the consumer is already bound on conn-1, so no duplicate Bind.
	gate.onConnReady(context.Background(), "conn-1", fb.bind)
	if fb.callCount() != 1 {
		t.Errorf("bindUser call count after onConnReady rerun (same connID) = %d, want 1 (no double bind)", fb.callCount())
	}
}

// TestHandleHello_SingleConsumerRejectsSecond: a SingleConsumer EventKey accepts
// the first consumer and rejects the second for the same SubscriptionID.
func TestHandleHello_SingleConsumerRejectsSecond(t *testing.T) {
	const key = "test.handlehello.exclusive"
	event.RegisterKey(event.KeyDefinition{
		Key:            key,
		EventType:      key,
		SingleConsumer: true,
		Schema:         event.SchemaDef{Native: &event.SchemaSpec{Raw: []byte(`{"type":"object"}`)}},
	})
	defer event.UnregisterKeyForTest(key)

	logger := log.New(io.Discard, "", 0)
	hub := NewHub()
	b := &Bus{
		hub:        hub,
		logger:     logger,
		conns:      make(map[*Conn]struct{}),
		idleTimer:  time.NewTimer(30 * time.Second),
		shutdownCh: make(chan struct{}, 1),
	}

	readAck := func(t *testing.T, pid int) *protocol.HelloAck {
		t.Helper()
		server, client := net.Pipe()
		t.Cleanup(func() { server.Close(); client.Close() })
		hello := &protocol.Hello{PID: pid, EventKey: key, EventTypes: []string{key}}
		go b.handleHello(server, bufio.NewReader(server), hello)
		line, err := protocol.ReadFrame(bufio.NewReader(client))
		if err != nil {
			t.Fatalf("read ack (pid %d): %v", pid, err)
		}
		msg, err := protocol.Decode(bytes.TrimRight(line, "\n"))
		if err != nil {
			t.Fatalf("decode ack (pid %d): %v", pid, err)
		}
		ack, ok := msg.(*protocol.HelloAck)
		if !ok {
			t.Fatalf("got %T, want *HelloAck", msg)
		}
		return ack
	}

	ack1 := readAck(t, 100)
	if ack1.Rejected {
		t.Fatalf("first consumer should be accepted, got rejected: %q", ack1.RejectReason)
	}

	ack2 := readAck(t, 200)
	if !ack2.Rejected {
		t.Fatal("second consumer should be rejected")
	}
	if !strings.Contains(ack2.RejectReason, "already running") {
		t.Errorf("reject reason = %q, want mention of 'already running'", ack2.RejectReason)
	}
}
