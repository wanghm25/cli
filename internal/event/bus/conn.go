// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"bufio"
	"bytes"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/larksuite/cli/internal/event/protocol"
)

const (
	sendChCap    = 100
	writeTimeout = 5 * time.Second
)

// Conn represents a single consume client connection in the Bus.
type Conn struct {
	conn       net.Conn
	reader     *bufio.Reader
	sendCh     chan interface{}
	sendMu     sync.Mutex // serialises drop+push atomically
	writeMu    sync.Mutex // serialises all net.Conn writes (Encode+SetWriteDeadline is a 2-call sequence)
	eventKey   string
	eventTypes []string
	subID      string
	pid        int
	// remoteSubscriptionID is set post-construction via SetRemoteSubscriptionID
	// (mirrors SetLogger/SetOnClose: no locking needed, populated once before
	// Start() and only read afterward). Empty ("") means legacy — populated
	// from Hello.RemoteSubscriptionID in handleHello (spec §4.3); the CLIENT
	// that sends a non-empty value is Task 15, so this is "" in practice
	// until then.
	remoteSubscriptionID string

	// owner* fields fix this consumer's identity at registration
	// (handleHello, spec §4.4) from Hello.Identity/Profile/UserOpenID + the
	// bus's own AppID — a bus is per-app, so there is no separate "hello
	// carries a different app_id" case to handle. Same zero-lock convention
	// as remoteSubscriptionID above: set once via SetOwnerIdentity before
	// Start(), read-only afterward.
	//
	// ownerUserOpenID == "" is the discriminator the identity gate (hub.go
	// Publish's delivery gate + identity.go's onConnReady bind gate) uses to
	// mean "bot or legacy/pre-Task-15 consumer" — such consumers are NEVER
	// identity-gated or BindUser'd (spec §4.4). A bot Hello (Identity=="bot")
	// always carries UserOpenID=="" (protocol/messages.go's Hello.UserOpenID
	// doc), so this falls out naturally with no extra bot-specific branch.
	ownerIdentity   string
	ownerAppID      string
	ownerUserOpenID string

	// identityMu guards the mutable post-registration identity-gate state
	// below. Unlike remoteSubscriptionID/owner* (write-once-then-read-only),
	// these are written repeatedly AFTER Start() — by identity.go's
	// onConnReady (the WS ready/reconnect callback's goroutine) and by
	// Hub.Publish's per-event delivery gate (the source's emit goroutine) —
	// and read by a future status command (Task 16), so they need real
	// synchronization rather than the zero-lock convention above.
	identityMu     sync.Mutex
	boundConnID    string
	staleIdentity  bool
	degradedReason string

	onClose         func(*Conn)
	checkLastForKey func(scope string) bool
	logger          *log.Logger
	closed          chan struct{}
	closeOnce       sync.Once
	received        atomic.Int64  // events fanned out to us (post-filter)
	seqCounter      atomic.Uint64 // per-conn monotonic seq assigned by Hub.Publish
	dropped         atomic.Int64  // events evicted via drop-oldest backpressure
}

// NewConn creates a Conn; pass a reader with pre-buffered bytes (handoff from Bus.handleConn) or nil for a fresh one.
func NewConn(conn net.Conn, reader *bufio.Reader, eventKey string, eventTypes []string, pid int, subID string) *Conn {
	if reader == nil {
		reader = bufio.NewReader(conn)
	}
	return &Conn{
		conn:       conn,
		reader:     reader,
		sendCh:     make(chan interface{}, sendChCap),
		eventKey:   eventKey,
		eventTypes: eventTypes,
		pid:        pid,
		subID:      subID,
		closed:     make(chan struct{}),
	}
}

// SubscriptionID returns the subscription identity. Falls back to EventKey
// when the stored subID is empty (legacy clients / no-SubscriptionKey EventKeys).
func (c *Conn) SubscriptionID() string {
	if c.subID == "" {
		return c.eventKey
	}
	return c.subID
}

func (c *Conn) SetOnClose(fn func(*Conn)) { c.onClose = fn }

// SetCheckLastForKey: returning true means "you are the last subscriber, run cleanup".
func (c *Conn) SetCheckLastForKey(fn func(string) bool) { c.checkLastForKey = fn }

// SetLogger attaches a logger (nil tolerated).
func (c *Conn) SetLogger(l *log.Logger) { c.logger = l }

// SetRemoteSubscriptionID records which remote Subscription (OpenAPI primary
// key, e.g. "sub_xxx") this consumer is bound to, read from
// Hello.RemoteSubscriptionID in handleHello. Call before Start() (same
// convention as SetLogger/SetOnClose/SetCheckLastForKey) — set once,
// read-only afterward, so no additional locking is needed.
func (c *Conn) SetRemoteSubscriptionID(id string) { c.remoteSubscriptionID = id }

func (c *Conn) EventKey() string     { return c.eventKey }
func (c *Conn) EventTypes() []string { return c.eventTypes }

// RemoteSubscriptionID satisfies Subscriber: "" means legacy (the default
// for every Conn until SetRemoteSubscriptionID is called).
func (c *Conn) RemoteSubscriptionID() string { return c.remoteSubscriptionID }

// SetOwnerIdentity records the owner identity fixed at registration (spec
// §4.4): identity is "user" or "bot" (Hello.Identity, verbatim — "" for a
// legacy/pre-Task-15 Hello); appID/userOpenID are the ONLY fields the
// identity gate compares against the freshly-resolved current identity
// (never UAT). Call before Start() (same convention as
// SetRemoteSubscriptionID) — set once, read-only afterward.
func (c *Conn) SetOwnerIdentity(identity, appID, userOpenID string) {
	c.ownerIdentity = identity
	c.ownerAppID = appID
	c.ownerUserOpenID = userOpenID
}

// OwnerIdentity returns the owner's Hello.Identity ("user"/"bot"/"" for a
// legacy/pre-Task-15 registration). Not part of Subscriber — only the
// comparison fields below are.
func (c *Conn) OwnerIdentity() string { return c.ownerIdentity }

// OwnerAppID satisfies Subscriber: "" for a never-set (legacy/test) Conn.
func (c *Conn) OwnerAppID() string { return c.ownerAppID }

// OwnerUserOpenID satisfies Subscriber: "" marks a bot or legacy/pre-Task-15
// consumer — the identity gate never gates or binds these (spec §4.4: bot
// consumers are NEVER identity-gated).
func (c *Conn) OwnerUserOpenID() string { return c.ownerUserOpenID }

// BoundConnID returns the WS connection_id this consumer's owner was last
// successfully BindUser'd on ("" = never bound). identity.go's onConnReady
// compares this against the CURRENT connID to skip a duplicate Bind for the
// same user on the same connection; a reconnect rotates connID, so the next
// onConnReady call always rebinds.
func (c *Conn) BoundConnID() string {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	return c.boundConnID
}

// SetBoundConnID records a successful BindUser for connID and clears any
// prior stale/degraded state — a fresh successful bind supersedes both.
func (c *Conn) SetBoundConnID(connID string) {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	c.boundConnID = connID
	c.staleIdentity = false
	c.degradedReason = ""
}

// StaleIdentity reports whether this consumer's owner last mismatched the
// current identity (spec §4.4): while true, the identity gate delivers NO
// events, attempts NO BindUser, and loads NO UAT for this consumer.
func (c *Conn) StaleIdentity() bool {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	return c.staleIdentity
}

// SetStaleIdentity marks owner != current. Does not touch boundConnID: a
// later owner == current success clears staleness via SetBoundConnID
// instead, keeping the two state transitions explicit and independent.
func (c *Conn) SetStaleIdentity() {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	c.staleIdentity = true
}

// DegradedReason returns why this consumer is degraded ("" = not degraded).
// Deliberately a short classified string, never a raw error or token — this
// may be surfaced by a future status command (Task 16).
func (c *Conn) DegradedReason() string {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	return c.degradedReason
}

// SetDegraded records a per-consumer failure reason (e.g. a bind/UAT error,
// or an unresolved current identity). A single consumer's failure must
// never affect any other consumer (spec §4.4) — callers only ever set this
// on the ONE Conn that failed.
func (c *Conn) SetDegraded(reason string) {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	c.degradedReason = reason
}

func (c *Conn) SendCh() chan interface{} { return c.sendCh }
func (c *Conn) PID() int                 { return c.pid }
func (c *Conn) IncrementReceived()       { c.received.Add(1) }
func (c *Conn) Received() int64          { return c.received.Load() }

// NextSeq returns the next monotonic seq for this conn (first call returns 1).
func (c *Conn) NextSeq() uint64 { return c.seqCounter.Add(1) }

func (c *Conn) DroppedCount() int64 { return c.dropped.Load() }
func (c *Conn) IncrementDropped()   { c.dropped.Add(1) }

// Start launches the sender and reader goroutines; call exactly once.
func (c *Conn) Start() {
	go c.SenderLoop()
	go c.ReaderLoop()
}

// writeFrame is the sole write path, serialised via writeMu.
func (c *Conn) writeFrame(msg interface{}) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return err
	}
	return protocol.Encode(c.conn, msg)
}

// SenderLoop exits on closed (not sendCh close) so Hub.Publish can send without panic risk.
func (c *Conn) SenderLoop() {
	for {
		select {
		case <-c.closed:
			return
		case msg := <-c.sendCh:
			if err := c.writeFrame(msg); err != nil {
				if c.logger != nil {
					c.logger.Printf("WARN: write to pid=%d failed: %v", c.pid, err)
				}
				c.shutdown()
				return
			}
		}
	}
}

// ReaderLoop reads control messages (Bye, PreShutdownCheck) until EOF.
func (c *Conn) ReaderLoop() {
	for {
		line, err := protocol.ReadFrame(c.reader)
		if err != nil {
			break
		}
		line = bytes.TrimRight(line, "\n")
		if len(line) == 0 {
			continue
		}
		msg, err := protocol.Decode(line)
		if err != nil {
			continue
		}
		c.handleControlMessage(msg)
	}
	c.shutdown()
}

func (c *Conn) handleControlMessage(msg interface{}) {
	switch msg.(type) {
	case *protocol.Bye:
		c.shutdown()
	case *protocol.PreShutdownCheck:
		// Use the connection's own authoritative subscription identity rather
		// than recomputing from the incoming message: a stale or mismatched
		// PreShutdownCheck must not ask about the wrong scope (which would
		// suppress or mistrigger per-subscription cleanup). Conn.SubscriptionID()
		// already falls back to EventKey when its stored subID is empty.
		scope := c.SubscriptionID()
		lastForKey := true
		if c.checkLastForKey != nil {
			lastForKey = c.checkLastForKey(scope)
		}
		ack := protocol.NewPreShutdownAck(lastForKey)
		if err := c.writeFrame(ack); err != nil && c.logger != nil {
			c.logger.Printf("WARN: pre_shutdown_ack to pid=%d failed: %v", c.pid, err)
		}
	}
}

func (c *Conn) shutdown() {
	c.closeOnce.Do(func() {
		close(c.closed)
		c.conn.Close()
		// sendCh is NOT closed: would race with Hub.Publish holding SendCh() after RUnlock.
		if c.onClose != nil {
			c.onClose(c)
		}
	})
}

// TrySend enqueues non-evictively under sendMu so it respects PushDropOldest's atomicity contract.
func (c *Conn) TrySend(msg interface{}) bool {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	select {
	case c.sendCh <- msg:
		return true
	default:
		return false
	}
}

// PushDropOldest enqueues msg; on full channel evicts one oldest and retries, atomically under sendMu.
// Returns (enqueued, dropped). A rare concurrent drain may make drop unnecessary — still succeeds with dropped=false.
func (c *Conn) PushDropOldest(msg interface{}) (enqueued, dropped bool) {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	select {
	case c.sendCh <- msg:
		return true, false
	default:
	}
	select {
	case <-c.sendCh:
		dropped = true
	default:
	}
	select {
	case c.sendCh <- msg:
		return true, dropped
	default:
		return false, dropped
	}
}

// Close is idempotent.
func (c *Conn) Close() {
	c.shutdown()
}
