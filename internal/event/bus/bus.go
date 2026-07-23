// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

// Package bus implements the per-AppID event-bus daemon; lifecycle is driven by consumer presence (idle timeout) and explicit shutdown.
package bus

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"

	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/busdiscover"
	"github.com/larksuite/cli/internal/event/protocol"
	"github.com/larksuite/cli/internal/event/source"
	"github.com/larksuite/cli/internal/event/transport"
	"github.com/larksuite/cli/internal/lockfile"
)

const (
	idleTimeout = 30 * time.Second
)

// Bus is the central event bus daemon.
type Bus struct {
	appID     string
	appSecret string
	domain    string
	transport transport.IPC
	hub       *Hub
	listener  net.Listener
	logger    *log.Logger
	startTime time.Time

	mu         sync.Mutex
	conns      map[*Conn]struct{}
	idleTimer  *time.Timer
	shutdownCh chan struct{}

	// identityGate implements the owner/current identity gate +
	// BindUser wiring; nil (the default from NewBus) means no identity
	// gating configured — every existing NewBus(...) caller (tests, and any
	// _bus invocation that predates SetIdentityProviders) keeps exactly
	// today's behavior. Call SetIdentityProviders before Run() to enable it.
	identityGate *identityGate

	// lifecycleExecutor is the bounded, in-memory subscription lifecycle
	// executor every FeishuSource's 6 typed lifecycle
	// handlers feed into — always constructed by NewBus, never nil.
	lifecycleExecutor *lifecycleExecutor

	// lifecycleAction is the REAL per-event action
	// newLifecycleExecutor above was constructed with, kept as its
	// own typed field (rather than only living inside lifecycleExecutor) so
	// SetIdentityProviders/SetSubscriptionClient below can fill in its two
	// optional dependencies AFTER construction — mirroring identityGate's
	// own "nil until SetIdentityProviders" convention. Never nil itself:
	// only its OWN identityGate/newSubClient fields start nil, which keeps
	// it a strict superset of summaryLifecycleAction (full summary
	// recording, zero remote calls, zero panics) until wired.
	lifecycleAction *subscriptionLifecycleAction

	// encryptKeyProvider manages per-subscription encrypt_keys. The SDK
	// dispatcher is wired to its plain static cache
	// (encryptKeyProvider.dispatcherProvider()), so decryption on the event
	// path is a pure cache lookup — a miss fails CLOSED with no runtime remote
	// call. A key enters the cache only via encryptKeyProvider.fetchAndSet,
	// which handleHello calls ONCE when an encrypted refined consumer
	// registers. Always constructed by NewBus (never nil); its two remote
	// dependencies (identityGate, the GetEncryptKey client factory) start nil
	// and are filled in by SetIdentityProviders/SetSubscriptionClient. The key
	// it caches lives ONLY in the in-memory SDK StaticEncryptKeyProvider —
	// never logged, never on the IPC wire, never in status.
	encryptKeyProvider *encryptKeyProvider

	// pidHandle pins the alive.lock fd to the bus lifetime; OS releases on exit.
	pidHandle *busdiscover.Handle
}

func NewBus(appID, appSecret, domain string, tr transport.IPC, logger *log.Logger) *Bus {
	hub := NewHub()
	action := newSubscriptionLifecycleAction(hub, logger)
	b := &Bus{
		appID:     appID,
		appSecret: appSecret,
		domain:    domain,
		transport: tr,
		hub:       hub,
		logger:    logger,
		startTime: time.Now(),
		conns:     make(map[*Conn]struct{}),
		// Buffered so shutdown and source-exit paths never drop the signal.
		shutdownCh:         make(chan struct{}, 1),
		lifecycleExecutor:  newLifecycleExecutor(hub, action, logger),
		lifecycleAction:    action,
		encryptKeyProvider: newEncryptKeyProvider(),
	}
	// On deleted_v1 the lifecycle action releases the
	// subscription's cached encrypt_key from the provider. Wired
	// here (post-construction) since both the action and the provider exist by
	// now — mirrors setIdentityGate/setNewSubscriptionClient's own convention.
	b.lifecycleAction.setEncryptKeyRemover(b.encryptKeyProvider.Remove)
	return b
}

// SetIdentityProviders enables the real-time identity gate + BindUser.
// resolveUAT mints a UAT for exactly the (appID, userOpenID) pair the
// gate resolved as CURRENT — the daemon entrypoint (cmd/event/bus.go) wires
// this to the credential chain (f.Credential), which this package cannot
// reach directly without importing internal/credential. Fresh-current
// resolution itself (LoadMultiAppConfig -> CurrentAppConfig("") ->
// Users[0]) is always resolveCurrentIdentity (identity.go) —
// package bus already imports internal/core, so there is no reason for a
// caller outside this package to ever need to override it.
//
// nil disables gating entirely (the default) — call before Run() (single-
// goroutine setup, same convention as the rest of Bus's construction; not
// safe to call concurrently with Run()).
func (b *Bus) SetIdentityProviders(resolveUAT func(ctx context.Context, appID, userOpenID string) (string, error)) {
	if resolveUAT == nil {
		return
	}
	b.identityGate = newIdentityGate(b.hub, resolveCurrentIdentity, resolveUAT, b.logger)
	b.hub.SetCurrentResolver(resolveCurrentIdentity)
	// The real lifecycle action's owner==current gate and
	// bindConsumer (activated/suspended-recovery) both need this SAME gate.
	b.lifecycleAction.setIdentityGate(b.identityGate)
	// The encrypt-key provider reuses the SAME gate for a user
	// subscription's owner==current check + fresh-UAT mint.
	b.encryptKeyProvider.setIdentityGate(b.identityGate)
}

// SetSubscriptionClient injects the *lark.Client the real lifecycle
// action needs to issue the single Reactivate/Renew/Get
// call each event allows. Mirrors SetIdentityProviders's own shape: nil is
// tolerated (no-op, keeping the lifecycle action summary-only) and this is
// safe to call any number of times before Run() only — NOT concurrently
// with it, exactly like SetIdentityProviders.
//
// sdk is expected to already be bound to THIS bus's own (appID, appSecret)
// pair (a bus is per-app) — the SAME *lark.Client every
// `event subscription` command builds via f.LarkClient() (cmd/event/bus.go
// wires this). The per-call identity (bot, or a specific user's FRESH uat —
// never a historical one) is decided fresh for EVERY action by the
// lifecycle action itself via eventlib.NewSubscriptionClient(sdk, as, uat)
// — never by constructing a second
// *lark.Client.
func (b *Bus) SetSubscriptionClient(sdk *lark.Client) {
	if sdk == nil {
		return
	}
	b.lifecycleAction.setNewSubscriptionClient(func(as core.Identity, uat string) (subscriptionActionClient, error) {
		return event.NewSubscriptionClient(sdk, as, uat)
	})
	// The encrypt-key provider fetches keys via GetEncryptKey on
	// the SAME per-app *lark.Client, deciding the per-call identity (bot, or a
	// specific user's FRESH uat — never a historical one) fresh for
	// every fetch, exactly like the lifecycle action above.
	b.encryptKeyProvider.setNewClient(func(as core.Identity, uat string) (encryptKeyClient, error) {
		return event.NewSubscriptionClient(sdk, as, uat)
	})
}

// Run binds the IPC socket, starts event sources, and blocks in the accept loop until shutdown.
func (b *Bus) Run(ctx context.Context) error {
	addr := b.transport.Address(b.appID)

	// alive.lock before bind: closes the cleanup-TOCTOU race where two newly forked
	// buses each unlink and rebind the socket. Brief retry covers stop-then-restart.
	eventsDir := filepath.Join(core.GetConfigDir(), "events", event.SanitizeAppID(b.appID))
	pidHandle, pidErr := acquireAliveLock(eventsDir)
	if pidErr != nil {
		if errors.Is(pidErr, lockfile.ErrHeld) {
			b.logger.Printf("Another bus already holds %s/bus.alive.lock, exiting", eventsDir)
			return nil
		}
		b.logger.Printf("[bus] pid file write failed: %v (status discovery may miss this bus)", pidErr)
	} else {
		b.pidHandle = pidHandle
	}

	ln, err := b.transport.Listen(addr)
	if err != nil {
		if probe, dialErr := b.transport.Dial(addr); dialErr == nil {
			probe.Close()
			b.logger.Printf("Another bus is already running for %s, exiting", b.appID)
			return nil
		}
		b.transport.Cleanup(addr)
		ln, err = b.transport.Listen(addr)
		if err != nil {
			return fmt.Errorf("bus listen: %w", err)
		}
	}
	b.listener = ln
	b.logger.Printf("Bus started for app=%s pid=%d addr=%s", b.appID, os.Getpid(), addr)

	b.idleTimer = time.NewTimer(idleTimeout)

	sourceCtx, sourceCancel := context.WithCancel(ctx)
	defer sourceCancel()
	b.startSources(sourceCtx)

	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		b.acceptLoop(ctx)
	}()

	// Re-check live conn count under lock: a stale idle tick can linger past a concurrent Stop+Reset.
	for {
		select {
		case <-ctx.Done():
			b.logger.Printf("Bus shutting down (context cancelled)")
		case <-b.idleTimer.C:
			b.mu.Lock()
			active := len(b.conns)
			if active > 0 {
				b.idleTimer.Reset(idleTimeout)
				b.mu.Unlock()
				continue
			}
			b.mu.Unlock()
			b.logger.Printf("Bus shutting down (idle %v, no active connections)", idleTimeout)
		case <-b.shutdownCh:
			b.logger.Printf("Bus shutting down (shutdown command received)")
		}
		break
	}

	b.listener.Close()
	// Don't delete the socket: Run() handles stale sockets on startup, and deletion races a new bus.
	shutdownConns(b)
	<-acceptDone
	// Not-started lifecycle work is discarded; already-started runs finish
	// under their own timeout (see lifecycleExecutor.Cancel's own doc).
	b.lifecycleExecutor.Cancel()
	b.logger.Printf("Bus exited cleanly")
	return nil
}

// shutdownConns snapshots b.conns under lock then releases before Close() — Close→onClose reacquires b.mu.
func shutdownConns(b *Bus) {
	b.mu.Lock()
	conns := make([]*Conn, 0, len(b.conns))
	for c := range b.conns {
		conns = append(conns, c)
	}
	b.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
}

// startSources launches registered sources (or a default FeishuSource); any source exit triggers full bus shutdown.
func (b *Bus) startSources(ctx context.Context) {
	sources := source.All()
	if len(sources) == 0 {
		fs := &source.FeishuSource{
			AppID:     b.appID,
			AppSecret: b.appSecret,
			Domain:    b.domain,
			Logger:    b.logger,
		}
		if b.identityGate != nil {
			fs.OnConnReady = b.identityGate.onConnReady
		}
		fs.OnLifecycleEvent = b.lifecycleExecutor.Submit
		// The dispatcher decrypts encrypted subscription envelopes via the
		// plain static cache: a pure lookup, no runtime remote fetch (keys are
		// pre-loaded at handleHello by encryptKeyProvider.fetchAndSet). A miss
		// returns ("",false) → SDK fail-closed. Non-encrypted events never
		// reach it (the SDK only calls EncryptKey for envelopes carrying a
		// top-level encrypt_info), so wiring it is a no-op for the plaintext path.
		fs.EncryptKeyProvider = b.encryptKeyProvider.dispatcherProvider()
		// A fail-closed SDK decrypt failure surfaces via the SDK
		// logger; the source extracts the subscription_id and calls this so the
		// bus can count it + mark the matched consumer degraded. The
		// undecryptable event is already dropped by the SDK — never delivered.
		fs.OnDecryptFailure = b.onDecryptFailure
		sources = []source.Source{fs}
	}
	eventTypes := subscribedEventTypes()
	b.hub.SetLogger(b.logger)
	for _, src := range sources {
		go func(s source.Source) {
			b.logger.Printf("Starting source: %s", s.Name())
			err := s.Start(ctx, eventTypes, func(raw *event.RawEvent) {
				b.logger.Printf("Event received: type=%s id=%s", raw.EventType, raw.EventID)
				// Dedup runs INSIDE Hub.Publish, after routing-domain
				// identification — not here as a single global
				// event_id gate, which would swallow a refined event's second
				// delivery (same event_id, different remote_subscription_id)
				// before it ever reached its second refined consumer.
				b.hub.Publish(raw)
			}, func(state, detail string) {
				b.hub.BroadcastSourceStatus(s.Name(), state, detail)
			})
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				b.logger.Printf("Source %s exited with error: %v — shutting down bus", s.Name(), err)
			} else {
				b.logger.Printf("Source %s exited without error before shutdown — shutting down bus", s.Name())
			}
			select {
			case b.shutdownCh <- struct{}{}:
			default:
			}
		}(src)
	}
}

// subscribedEventTypes returns the deduplicated union of EventTypes from every registered EventKey.
func subscribedEventTypes() []string {
	seen := make(map[string]struct{})
	var types []string
	for _, def := range event.ListAll() {
		if _, ok := seen[def.EventType]; ok {
			continue
		}
		seen[def.EventType] = struct{}{}
		types = append(types, def.EventType)
	}
	return types
}

// acceptLoop accepts IPC connections until the listener is closed.
func (b *Bus) acceptLoop(ctx context.Context) {
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			default:
			}
			b.logger.Printf("Accept error: %v", err)
			return
		}
		go b.handleConn(conn)
	}
}

// handleConn reads the first protocol message and dispatches; the bufio.Reader is handed to Conn so buffered bytes carry over.
func (b *Bus) handleConn(conn net.Conn) {
	br := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := protocol.ReadFrame(br)
	if err != nil {
		conn.Close()
		return
	}
	conn.SetReadDeadline(time.Time{})

	msg, err := protocol.Decode(bytes.TrimRight(line, "\n"))
	if err != nil {
		conn.Close()
		return
	}

	switch m := msg.(type) {
	case *protocol.Hello:
		b.handleHello(conn, br, m)
	case *protocol.StatusQuery:
		b.handleStatusQuery(conn)
	case *protocol.Shutdown:
		b.handleShutdown(conn)
	default:
		conn.Close()
	}
}

// handleHello registers a consume connection with the hub; reader carries bytes already pulled off conn.
func (b *Bus) handleHello(conn net.Conn, reader *bufio.Reader, hello *protocol.Hello) {
	subID := hello.SubscriptionID
	if subID == "" {
		subID = hello.EventKey
	}
	bc := NewConn(conn, reader, hello.EventKey, hello.EventTypes, hello.PID, subID)
	// Server-side read of a Hello field the refined client populates; it is
	// "" (legacy) for a client that does not set it — existing
	// behavior is unchanged. Empty stays empty (Subscriber's "" = legacy
	// contract), same fallback shape as SubscriptionID above but WITHOUT a
	// fallback-to-EventKey: an absent remote_subscription_id must stay empty,
	// never be repurposed from another field.
	bc.SetRemoteSubscriptionID(hello.RemoteSubscriptionID)
	// Owner identity fixed at registration: read from the
	// Hello fields (populated client-side by the refined HelloV2 — "" until
	// then, which is the correct "not gated yet" behavior). owner_app_id is
	// the BUS's own AppID: a bus is per-app, so Hello carries no separate
	// app_id field to read instead.
	bc.SetOwnerIdentity(hello.Identity, b.appID, hello.UserOpenID)
	// Store this consumer's local listening intent. Legacy Hello frames leave
	// these fields empty/false; lifecycle compatibility checks only compare
	// refined consumers, whose RemoteSubscriptionID is non-empty.
	bc.SetListenIntent(hello.TargetResource, hello.IncludeResourceData)
	bc.SetLogger(b.logger)

	// Encrypted refined consumer: fetch the subscription's encrypt_key ONCE
	// now (under the owner==current gate) and register it with the SDK decrypt
	// provider BEFORE acking, so the dispatcher decrypts later events with zero
	// hot-path network. This runs before any hub/bus registration, so a
	// failure (owner mismatch / missing scope / no key / transient) simply
	// REJECTS the Hello — the consumer never registers or readies, leaving no
	// half-registered consumer behind. The reject reason is a fixed token,
	// never the raw fetch error or any key material.
	if hello.IncludeResourceData && b.encryptKeyProvider != nil {
		if err := b.encryptKeyProvider.fetchAndSet(context.Background(), bc.RemoteSubscriptionID(), bc); err != nil {
			b.logger.Printf("[encrypt-key] rejecting encrypted consumer pid=%d key=%q: key unavailable (%s)",
				hello.PID, hello.EventKey, encryptKeyFailureClass(err))
			if werr := bc.writeFrame(protocol.NewHelloAckRejected("v1", protocol.RejectReasonDecryptKeyUnavailable)); werr != nil {
				b.logger.Printf("WARN: reject hello_ack (decrypt_key_unavailable) write to pid=%d key=%q failed: %v", hello.PID, hello.EventKey, werr)
			}
			bc.Close()
			return
		}
	}

	// SingleConsumer EventKeys allow only one consumer per SubscriptionID: reject extras at handshake.
	exclusive := false
	if def, ok := event.Lookup(hello.EventKey); ok {
		exclusive = def.SingleConsumer
	}
	var firstForKey bool
	if exclusive {
		ok, reason := b.hub.TryRegisterExclusive(bc)
		if !ok {
			if err := bc.writeFrame(protocol.NewHelloAckRejected("v1", reason)); err != nil {
				b.logger.Printf("WARN: reject hello_ack write to pid=%d key=%q failed: %v", hello.PID, hello.EventKey, err)
			}
			bc.Close()
			return
		}
		firstForKey = true
	} else {
		// Register + isFirst under one lock; blocks on any in-progress cleanup lock for the same EventKey.
		firstForKey = b.hub.RegisterAndIsFirst(bc)
	}

	bc.SetCheckLastForKey(func(scope string) bool {
		return b.hub.AcquireCleanupLock(scope)
	})
	bc.SetOnClose(func(c *Conn) {
		b.hub.UnregisterAndIsLast(c)
		// Release is idempotent and must fire on every disconnect path so waiters don't block forever.
		b.hub.ReleaseCleanupLock(c.SubscriptionID())
		b.mu.Lock()
		delete(b.conns, c)
		remaining := len(b.conns)
		b.mu.Unlock()
		b.logger.Printf("Consumer disconnected: pid=%d key=%s (remaining=%d)", c.PID(), c.EventKey(), remaining)
		if remaining == 0 {
			// Stop+drain before Reset (Go docs) to avoid a stale fire in .C.
			if !b.idleTimer.Stop() {
				select {
				case <-b.idleTimer.C:
				default:
				}
			}
			b.idleTimer.Reset(idleTimeout)
		}
	})

	b.mu.Lock()
	b.conns[bc] = struct{}{}
	// Stop+drain under mu so a fire can't slip past a fresh registration.
	if !b.idleTimer.Stop() {
		select {
		case <-b.idleTimer.C:
		default:
		}
	}
	b.mu.Unlock()

	ack := protocol.NewHelloAck("v1", firstForKey)
	// writeFrame shares writeMu with every other write; bc.Close on failure unwinds hub+bus registration via onClose.
	if err := bc.writeFrame(ack); err != nil {
		b.logger.Printf("WARN: hello_ack write to pid=%d key=%q failed: %v (rejecting connection)",
			hello.PID, hello.EventKey, err)
		bc.Close()
		return
	}

	// Quote untrusted fields to prevent log forging via embedded newlines.
	b.logger.Printf("Consumer connected: pid=%d key=%q event_types=%q first=%v",
		hello.PID, hello.EventKey, hello.EventTypes, firstForKey)

	bc.Start()
}

// handleStatusQuery replies with status and closes.
//
// The v2 fields (ProtocolVersion/Capabilities/RegisteredEventTypes) are set
// after NewStatusResponse's construction rather than by changing
// NewStatusResponse's own signature (protocol/messages.go): that
// constructor has other call sites (cmd/event/status_orphan_test.go,
// internal/event/consume/startup_probe_test.go, protocol's own tests) that
// build a StatusResponse with no bus/hub in scope at all — this keeps them
// unchanged. An old (pre-v2) bus never sets these three fields at
// all; their absence is itself the incompatibility signal a later prober
// (ProbeBusEligibility) checks for.
func (b *Bus) handleStatusQuery(conn net.Conn) {
	defer conn.Close()
	resp := protocol.NewStatusResponse(
		os.Getpid(),
		int(time.Since(b.startTime).Seconds()),
		b.hub.ConnCount(),
		b.hub.Consumers(),
	)
	resp.ProtocolVersion = protocol.ProtocolVersionV2
	resp.Capabilities = []string{protocol.CapabilityRefinedRouting, protocol.CapabilityHelloV2}
	resp.RegisteredEventTypes = b.hub.RegisteredEventTypes()
	_ = protocol.EncodeWithDeadline(conn, resp, protocol.WriteTimeout)
}

// onDecryptFailure records a fail-closed SDK decryption failure for
// subscriptionID. The undecryptable event was
// already dropped by the SDK dispatcher (it never reached a handler, emit, the
// Hub, or stdout — no ciphertext is ever delivered); this only updates
// observability: every matched consumer's decrypt-failure counter/state, and,
// once failures persist, its degraded flag. The warning it logs to bus.log is
// non-sensitive — subscription_id + a fixed classification only, never a key,
// ciphertext, or decrypted plaintext.
func (b *Bus) onDecryptFailure(subscriptionID string) {
	if subscriptionID == "" {
		return
	}
	conns := b.hub.connsByRemoteSubscriptionID(subscriptionID)
	for _, c := range conns {
		c.RecordDecryptFailure()
	}
	b.logger.Printf("[decrypt] WARN: an event for subscription_id=%s could not be decrypted; dropped (fail-closed), matched_consumers=%d",
		subscriptionID, len(conns))
}

// handleShutdown signals Run() to exit.
func (b *Bus) handleShutdown(conn net.Conn) {
	defer conn.Close()
	b.logger.Printf("Received shutdown command")
	select {
	case b.shutdownCh <- struct{}{}:
	default:
	}
}

const (
	aliveLockMaxWait      = 2 * time.Second
	aliveLockPollInterval = 50 * time.Millisecond
)

// acquireAliveLock retries on ErrHeld so a stop-then-immediate-restart finds the lock free.
func acquireAliveLock(eventsDir string) (*busdiscover.Handle, error) {
	deadline := time.Now().Add(aliveLockMaxWait)
	for {
		h, err := busdiscover.WritePIDFile(eventsDir, os.Getpid())
		if err == nil {
			return h, nil
		}
		if !errors.Is(err, lockfile.ErrHeld) || time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(aliveLockPollInterval)
	}
}
