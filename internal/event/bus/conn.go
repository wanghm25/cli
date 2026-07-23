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
	// from Hello.RemoteSubscriptionID in handleHello; only a refined CLIENT
	// sends a non-empty value, so this is "" for a client that does not.
	remoteSubscriptionID string

	// owner* fields fix this consumer's identity at registration
	// (handleHello) from Hello.Identity/Profile/UserOpenID + the
	// bus's own AppID — a bus is per-app, so there is no separate "hello
	// carries a different app_id" case to handle. Same zero-lock convention
	// as remoteSubscriptionID above: set once via SetOwnerIdentity before
	// Start(), read-only afterward.
	//
	// ownerUserOpenID == "" is the discriminator the identity gate (hub.go
	// Publish's delivery gate + identity.go's onConnReady bind gate) uses to
	// mean "bot or legacy consumer" — such consumers are NEVER
	// identity-gated or BindUser'd. A bot Hello (Identity=="bot")
	// always carries UserOpenID=="" (protocol/messages.go's Hello.UserOpenID
	// doc), so this falls out naturally with no extra bot-specific branch.
	ownerIdentity   string
	ownerAppID      string
	ownerUserOpenID string

	// listenTargetResource/listenIncludeResourceData record this consumer's
	// own local listening INTENT for its remote Subscription — target_resource
	// and payload_options.include_resource_data — populated from a refined
	// consumer's HelloV2 (Hello.TargetResource/Hello.IncludeResourceData) in
	// handleHello. Same zero-lock, write-once-before-Start()-then-read-only
	// convention as remoteSubscriptionID/owner* above. Together with
	// ownerIdentity/ownerAppID/ownerUserOpenID (this consumer's registered
	// AUTHORITY) these form the complete "local listening intent" the
	// updated_v1 lifecycle handler (lifecycle.go's classifyUpdateCompatibility)
	// compares an incoming event's After snapshot against field-by-field,
	// rather than comparing Authority alone. ""/false (the zero value) for a
	// legacy consumer that never calls SetListenIntent.
	listenTargetResource      string
	listenIncludeResourceData bool

	// identityMu guards the mutable post-registration identity-gate state
	// below. Unlike remoteSubscriptionID/owner* (write-once-then-read-only),
	// these are written repeatedly AFTER Start() — by identity.go's
	// onConnReady (the WS ready/reconnect callback's goroutine) and by
	// Hub.Publish's per-event delivery gate (the source's emit goroutine) —
	// and read by the status command, so they need real
	// synchronization rather than the zero-lock convention above.
	identityMu     sync.Mutex
	boundConnID    string
	staleIdentity  bool
	degradedReason string

	// --- lifecycle summary state ---
	// lastLifecycleEvent/lastLifecycleEventID/remoteState summarize the most
	// recent subscription lifecycle meta-event the lifecycle executor's
	// action (internal/event/bus/lifecycle) observed for this consumer's
	// remote Subscription. Guarded by the SAME identityMu as the rest of this
	// block: written by the executor's worker goroutines, read by
	// Hub.Consumers() (the `event status` surface).
	lastLifecycleEvent   string
	lastLifecycleEventID string
	remoteState          string

	// --- lifecycle ACTION state ---
	// suspensionReason/lastAction/lastActionError/nextAction record the real
	// per-event action lifecycle.SubscriptionAction took (or, per the security
	// red line, deliberately did NOT take) for this consumer's remote
	// Subscription. Same identityMu, same writers/readers as the summary
	// fields just above.
	//
	//   - suspensionReason: body.suspension.code carried VERBATIM from the
	//     most recent suspended_v1 (an open string, never a closed
	//     enum) — cleared to "" by a later activated_v1.
	//   - lastAction/lastActionError: which ONE remote SubscriptionClient
	//     call ("reactivate"/"renew"/"get" — at most one per
	//     event, never a retry) subscriptionLifecycleAction last attempted
	//     for this consumer, and its classified failure reason ("" on
	//     success, or no action attempted yet). lastActionError reuses typed
	//     error classification (errs.Problem's Category/Subtype) rather than
	//     inventing a new private error code — see
	//     classifyLifecycleActionError.
	//   - nextAction: a short, stable hint for what an operator/AI should do
	//     next while this consumer is degraded (e.g. nextActionReactivate —
	//     the recovery command is uniformly "reactivate", never "reactive"/"resume").
	//     "" means no outstanding recommendation.
	suspensionReason string
	lastAction       string
	lastActionError  string
	nextAction       string

	// --- decryption state (failure/observability) ---
	// decryptState is this consumer's most recent per-subscription decryption
	// status, produced by the bus-side EncryptKeyProvider (encrypt_key.go)
	// and the SDK-decrypt-failure path: "" (no encrypted delivery seen /
	// not applicable), "decrypted" (a usable key is available), or an error
	// class — "decrypt_key_unavailable" (no key: identity mismatch, missing
	// scope, GetEncryptKey failed) / "decrypt_failed" (key held but SDK
	// decrypt/parse failed). Same identityMu, same writers/readers as the
	// lifecycle block above. NEVER holds a key, ciphertext, or raw error — only
	// a short classification.
	decryptState string

	// lastDecryptError* summarize the most recent SDK decrypt FAILURE observed
	// for this consumer's subscription (surfaced by the status command as
	// last_decrypt_error {class,count,time}). decryptFailCount is the running
	// total; once it reaches decryptFailDegradeThreshold the consumer is also
	// marked degraded (single fail → count; persistent fail → degraded).
	// class is a fixed token ("decrypt_failed"), never a raw SDK error —
	// no padding/algorithm detail (no oracle).
	decryptFailCount      int64
	lastDecryptErrorClass string
	lastDecryptErrorTime  time.Time

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

// SetOwnerIdentity records the owner identity fixed at registration:
// identity is "user" or "bot" (Hello.Identity, verbatim — "" for a
// legacy Hello); appID/userOpenID are the ONLY fields the
// identity gate compares against the freshly-resolved current identity
// (never UAT). Call before Start() (same convention as
// SetRemoteSubscriptionID) — set once, read-only afterward.
func (c *Conn) SetOwnerIdentity(identity, appID, userOpenID string) {
	c.ownerIdentity = identity
	c.ownerAppID = appID
	c.ownerUserOpenID = userOpenID
}

// OwnerIdentity returns the owner's Hello.Identity ("user"/"bot"/"" for a
// legacy registration). Not part of Subscriber — only the
// comparison fields below are.
func (c *Conn) OwnerIdentity() string { return c.ownerIdentity }

// OwnerAppID satisfies Subscriber: "" for a never-set (legacy/test) Conn.
func (c *Conn) OwnerAppID() string { return c.ownerAppID }

// OwnerUserOpenID satisfies Subscriber: "" marks a bot or legacy
// consumer — the identity gate never gates or binds these (bot
// consumers are NEVER identity-gated).
func (c *Conn) OwnerUserOpenID() string { return c.ownerUserOpenID }

// SetListenIntent records this consumer's own local listening intent —
// target_resource + include_resource_data — from Hello.TargetResource/
// Hello.IncludeResourceData. Call before Start() (same convention as
// SetRemoteSubscriptionID/SetOwnerIdentity) — set once, read-only afterward.
func (c *Conn) SetListenIntent(targetResource string, includeResourceData bool) {
	c.listenTargetResource = targetResource
	c.listenIncludeResourceData = includeResourceData
}

// TargetResource returns this consumer's own local listening intent's
// target_resource ("" for a legacy consumer / never set).
func (c *Conn) TargetResource() string { return c.listenTargetResource }

// IncludeResourceDataIntent returns this consumer's own local listening
// intent's include_resource_data (false for a legacy consumer / never set,
// or a genuine plaintext subscription).
func (c *Conn) IncludeResourceDataIntent() bool { return c.listenIncludeResourceData }

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
// prior stale/degraded state — a fresh successful bind supersedes all three
// (staleIdentity, degradedReason, and nextAction).
func (c *Conn) SetBoundConnID(connID string) {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	c.boundConnID = connID
	c.staleIdentity = false
	c.degradedReason = ""
	c.nextAction = ""
}

// StaleIdentity reports whether this consumer's owner last mismatched the
// current identity: while true, the identity gate delivers NO
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
// may be surfaced by the status command.
func (c *Conn) DegradedReason() string {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	return c.degradedReason
}

// SetDegraded records a per-consumer failure reason (e.g. a bind/UAT error,
// or an unresolved current identity). A single consumer's failure must
// never affect any other consumer — callers only ever set this
// on the ONE Conn that failed.
func (c *Conn) SetDegraded(reason string) {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	c.degradedReason = reason
}

// LastLifecycleEvent returns the most recent subscription lifecycle event
// type observed for this consumer's remote Subscription ("" = none yet).
func (c *Conn) LastLifecycleEvent() string {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	return c.lastLifecycleEvent
}

// LastLifecycleEventID returns that event's event_id ("" = none yet).
func (c *Conn) LastLifecycleEventID() string {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	return c.lastLifecycleEventID
}

// RemoteState returns the last known remote Subscription state string
// reported by this consumer's lifecycle events ("" = unknown/none yet).
func (c *Conn) RemoteState() string {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	return c.remoteState
}

// SetLifecycleSummary records one lifecycle event's summary
// (LifecycleExecutor's summary-only action). eventType/eventID
// are recorded verbatim and always overwrite (both are always non-empty by
// the time Submit's own validation lets an event through). state is applied
// ONLY when non-empty: deleted_v1's SDK body carries no state field at all,
// and blanking a previously-known remoteState to "" on delete would destroy
// real information (e.g. the last known "active"/"suspended" snapshot) for
// no benefit — lastLifecycleEvent alone already records that a delete
// happened.
func (c *Conn) SetLifecycleSummary(eventType, eventID, state string) {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	c.lastLifecycleEvent = eventType
	c.lastLifecycleEventID = eventID
	if state != "" {
		c.remoteState = state
	}
}

// SuspensionReason returns the most recent suspended_v1's suspension.code,
// carried verbatim ("" = never suspended, or cleared by a later
// activated_v1).
func (c *Conn) SuspensionReason() string {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	return c.suspensionReason
}

// SetSuspensionReason records body.suspension.code VERBATIM (an
// open string, CLI never builds a closed enum for it) — pass "" to clear
// (activated_v1's hit path does this).
func (c *Conn) SetSuspensionReason(reason string) {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	c.suspensionReason = reason
}

// LastAction returns the most recent remote SubscriptionClient call
// subscriptionLifecycleAction attempted for this consumer ("reactivate" /
// "renew" / "get"; "" = none attempted yet).
func (c *Conn) LastAction() string {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	return c.lastAction
}

// SetLastAction records which action was attempted.
func (c *Conn) SetLastAction(action string) {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	c.lastAction = action
}

// LastActionError returns LastAction's classified failure reason ("" =
// succeeded, or no action attempted yet).
func (c *Conn) LastActionError() string {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	return c.lastActionError
}

// SetLastActionError records LastAction's classified outcome ("" = success).
func (c *Conn) SetLastActionError(reason string) {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	c.lastActionError = reason
}

// NextAction returns the current recommended recovery step ("" = none).
func (c *Conn) NextAction() string {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	return c.nextAction
}

// SetNextAction records the current recommended recovery step ("" = none
// outstanding). The recovery command is uniformly "reactivate"
// (never "reactive"/"resume") wherever that's what's being recommended.
func (c *Conn) SetNextAction(action string) {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	c.nextAction = action
}

// ClearActionDegraded clears BOTH degradedReason and nextAction together —
// used whenever a lifecycle action's outcome means "fully healthy again"
// (a bare successful Reactivate for a bot, or a successful
// Reactivate+bindConsumer pair for a user; likewise a successful Renew).
// Deliberately does NOT touch suspensionReason/lastAction/lastActionError —
// those are historical record-keeping, not "is this consumer currently
// degraded" state.
func (c *Conn) ClearActionDegraded() {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	c.degradedReason = ""
	c.nextAction = ""
}

// DecryptState returns this consumer's most recent decryption status ("" =
// none/not applicable). A short classification only — never a
// key/ciphertext.
func (c *Conn) DecryptState() string {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	return c.decryptState
}

// SetDecryptState records the decryption status. state is one of
// the small stable tokens documented on the decryptState field — never a raw
// SDK error or any key material.
func (c *Conn) SetDecryptState(state string) {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	c.decryptState = state
}

// decryptFailDegradeThreshold is how many decrypt failures a consumer tolerates
// before it is additionally marked degraded (single-event fail →
// drop+count; persistent fail → degraded). A small fixed value — this is
// control-plane observability, not a tuned data path.
const decryptFailDegradeThreshold = 3

// RecordDecryptFailure records one fail-closed SDK decrypt failure for this
// consumer. It increments the counter, stamps the last error
// class/time, and sets decrypt_state=decrypt_failed; once failures persist
// (>= decryptFailDegradeThreshold) it also marks the consumer degraded. It
// records ONLY a fixed classification — never a key, ciphertext, decrypted
// plaintext, or raw SDK error. The undecryptable event
// itself is dropped by the SDK before ever reaching delivery.
func (c *Conn) RecordDecryptFailure() {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	c.decryptFailCount++
	c.lastDecryptErrorClass = decryptStateFailed
	c.lastDecryptErrorTime = time.Now()
	c.decryptState = decryptStateFailed
	if c.decryptFailCount >= decryptFailDegradeThreshold {
		c.degradedReason = decryptStateFailed
	}
}

// DecryptFailCount returns the running total of decrypt failures observed for
// this consumer (0 = none).
func (c *Conn) DecryptFailCount() int64 {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	return c.decryptFailCount
}

// LastDecryptErrorClass returns the most recent decrypt-failure classification
// ("" = none yet). A fixed token, never a raw error.
func (c *Conn) LastDecryptErrorClass() string {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	return c.lastDecryptErrorClass
}

// LastDecryptErrorTime returns when the most recent decrypt failure was
// recorded (zero = none yet).
func (c *Conn) LastDecryptErrorTime() time.Time {
	c.identityMu.Lock()
	defer c.identityMu.Unlock()
	return c.lastDecryptErrorTime
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
