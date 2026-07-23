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

// Encrypted bot consumer, key fetch succeeds -> ack NOT rejected, consumer
// registered, key cached in the SDK static provider (dispatcher will hit it).
func TestHandleHello_EncryptedConsumer_KeyFetchSuccess_RegistersAndCaches(t *testing.T) {
	b := newEncryptedHelloBus(t, log.New(io.Discard, "", 0), &fakeEncryptKeyClient{key: "K_OK"}, currentIdentity{})
	hello := &protocol.Hello{
		PID:                  7001,
		EventKey:             "im.message.receive_v1/chat-id/oc_1",
		EventTypes:           []string{"im.message.receive_v1"},
		Identity:             "bot",
		RemoteSubscriptionID: "sub_enc_ok",
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
	b := newEncryptedHelloBus(t, logger, &fakeEncryptKeyClient{err: errors.New(secretish)}, currentIdentity{})
	hello := &protocol.Hello{
		PID:                  7002,
		EventKey:             "im.message.receive_v1/chat-id/oc_2",
		EventTypes:           []string{"im.message.receive_v1"},
		Identity:             "bot",
		RemoteSubscriptionID: "sub_enc_fail",
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
