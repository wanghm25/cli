// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/larksuite/cli/internal/event/model"
	"github.com/larksuite/cli/internal/event/session"
)

// reasonCurrentIdentityUnresolved is the shared identity-dimension reason used
// by both bus-side gates (hub.go Publish's delivery gate and onConnReady's bind
// gate) when resolveCurrent itself fails — distinct from stale_identity (which means
// current WAS resolved but didn't match this consumer's owner). It aliases the
// single session-owned token so every gate records one consistent value.
const reasonCurrentIdentityUnresolved = session.ReasonCurrentIdentityUnresolved

// defaultWSReadyWait bounds how long handleHello waits for the WS source to
// become ready before it can bind a user consumer and ack it truly-ready. Kept
// below the consumer's hello_ack deadline (consume.helloAckTimeout = 5s) so a
// fail-closed reject still reaches the consumer as a clean bind failure rather
// than a handshake timeout. A field on the gate (not a const) so tests can
// shrink it.
const defaultWSReadyWait = 4 * time.Second

// bindAdmitRetryLimit bounds handleHello's inline-bind retries when a WS
// reconnect advances the source epoch mid-bind (errStaleEpoch): the retry binds
// on the now-current generation. A tiny fixed cap — epoch advances are rare and
// finite, so this converges immediately and never spins.
const bindAdmitRetryLimit = 3

// identityGate implements the bus-side owner/current identity gate +
// BindUser wiring. resolveCurrent and resolveUAT are injected so
// tests exercise every branch (match/mismatch/error/changing-across-calls)
// without touching disk or keychain; production wires resolveCurrent to
// session.ResolveCurrentIdentity (the ONE current-identity read) and resolveUAT
// to the credential chain (cmd/event/bus.go, via Bus.SetIdentityProviders).
type identityGate struct {
	hub            *Hub
	resolveCurrent func() (session.CurrentIdentity, error)
	resolveUAT     func(ctx context.Context, appID, userOpenID string) (string, error)
	logger         *log.Logger

	// wsReadyWait bounds awaitWSReady (handleHello's pre-ack wait for the first
	// WS-ready). Defaults to defaultWSReadyWait; tests shrink it.
	wsReadyWait time.Duration

	// connMu guards connID/bindUser/epoch/readyCh below: the LATEST values
	// FeishuSource's ready closure passed to onConnReady, memoized so
	// bindConsumer can be invoked independently of a fresh WS ready/reconnect
	// callback — specifically, by subscriptionLifecycleAction
	// (internal/event/bus/lifecycle.go) reacting to an activated_v1/
	// suspended_v1 event, which has no connID/bindUser of its own to pass
	// in. onConnReady itself still writes these on EVERY invocation (never
	// once) — source/feishu.go's ready closure deliberately keeps NOT
	// memoizing on ITS side (cli.Connection().ConnectionID/cli.BindUser are
	// read fresh from the live *larkws.Client every time); this is the ONE
	// place downstream that now remembers the latest pair.
	connMu sync.Mutex
	connID string
	// epoch tracks the WS source generation. It advances whenever onConnReady
	// memoizes a connID different from the previous one (first ready, or a
	// reconnect that rotated it), so a slow BindUser from an old generation can
	// be dropped before it overwrites a newer generation's binding.
	epoch    session.SourceEpoch
	bindUser func(context.Context, string) error
	// readyCh is closed once, the first time the WS becomes ready, so
	// awaitWSReady can block a pre-ack user consumer until the source is up
	// (closing a channel is a persistent broadcast — a late waiter sees it
	// immediately). readyClosed guards the close-once under connMu.
	readyCh     chan struct{}
	readyClosed bool
}

func newIdentityGate(
	hub *Hub,
	resolveCurrent func() (session.CurrentIdentity, error),
	resolveUAT func(ctx context.Context, appID, userOpenID string) (string, error),
	logger *log.Logger,
) *identityGate {
	return &identityGate{
		hub:            hub,
		resolveCurrent: resolveCurrent,
		resolveUAT:     resolveUAT,
		logger:         logger,
		wsReadyWait:    defaultWSReadyWait,
		readyCh:        make(chan struct{}),
	}
}

func (g *identityGate) logf(format string, args ...interface{}) {
	if g.logger != nil {
		g.logger.Printf(format, args...)
	}
}

// errOwnerMismatch/errConnectionNotReady/errStaleEpoch are bindConsumer's own
// sentinel errors — callers only need "did this fail" (Conn.SetStaleIdentity/
// SetIdentityDegraded already recorded WHY), but a plain non-nil error is still
// clearer at call sites than a bare bool. errStaleEpoch is NOT a failure of the
// consumer — it means a newer WS generation superseded this bind mid-flight, so
// the result was dropped; a caller that must confirm a bind (handleHello)
// retries on the now-current generation.
var (
	errOwnerMismatch      = errors.New("identity gate: owner does not match current identity")
	errConnectionNotReady = errors.New("identity gate: no WS connection ready yet to bind on")
	errStaleEpoch         = errors.New("identity gate: bind superseded by a newer WS generation")
)

// onConnReady is FeishuSource.OnConnReady's implementation: invoked from the
// WS client's OnReady (first usable connection of a run) and OnReconnected
// (every successful reconnect — connID rotates each dial, so a stale
// binding from before is never assumed valid) callbacks. It memoizes
// (connID, bindUser) for bindConsumer's later, independent use, advances the
// source epoch when the connID actually changed, signals any pre-ack waiter
// (awaitWSReady), and then runs bindConsumer for every registered USER consumer
// (bot consumers are invisible here — Hub.userConns already filters them out,
// bot consumers are NEVER identity-gated or BindUser'd). Each consumer's
// outcome (bound / stale / degraded) is entirely bindConsumer's concern — a
// single consumer's failure never affects any other, and a resolveCurrent
// failure degrades only the consumers actually evaluated rather than
// crashing/killing the WS connection.
func (g *identityGate) onConnReady(ctx context.Context, connID string, bindUser func(context.Context, string) error) {
	if g == nil || g.hub == nil {
		return
	}
	g.connMu.Lock()
	if connID != g.connID {
		// A new WS generation: advance the epoch so any in-flight bind from the
		// previous generation is dropped rather than allowed to overwrite this one.
		g.epoch.Advance()
	}
	g.connID = connID
	g.bindUser = bindUser
	if !g.readyClosed {
		g.readyClosed = true
		close(g.readyCh)
	}
	g.connMu.Unlock()

	for _, c := range g.hub.userConns() {
		_ = g.bindConsumer(ctx, c)
	}
}

// ready reports whether a WS connection has already become ready — i.e. a
// (connID, bindUser) pair has been memoized by a prior onConnReady. A consumer
// that registers AFTER the WS is ready is not covered by onConnReady's own loop
// (that loop only iterates the consumers present when the WS became ready), so
// handleHello uses this to decide whether to bind such a consumer immediately.
// When no connection has ever been ready this returns false, so handleHello
// waits (awaitWSReady) rather than eagerly binding a brand-new consumer.
func (g *identityGate) ready() bool {
	if g == nil {
		return false
	}
	g.connMu.Lock()
	defer g.connMu.Unlock()
	return g.connID != ""
}

// awaitWSReady blocks until the WS source has become ready at least once (a
// connID has been memoized), the context is cancelled, or timeout elapses.
// Returns true only if the source became ready. handleHello uses it so a user
// consumer that Hello's on a FRESH bus (WS not up yet) waits to be bound before
// it can ack truly-ready — a user consumer must never ack "ready" while
// BindUser has not yet succeeded. Bounded so a source that never comes up
// fails closed (reject) rather than parking the handshake forever.
func (g *identityGate) awaitWSReady(ctx context.Context, timeout time.Duration) bool {
	if g == nil {
		return false
	}
	g.connMu.Lock()
	ch := g.readyCh
	already := g.connID != ""
	g.connMu.Unlock()
	if already {
		return true
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ch:
		return true
	case <-ctx.Done():
		return false
	case <-timer.C:
		return false
	}
}

// bindConsumer performs the per-consumer bind sequence: the shared owner==
// current gate -> resolveUAT -> bindUser -> SetBoundConnID (epoch-guarded).
// Factored out of onConnReady's own loop body so a lifecycle action — e.g.
// subscriptionLifecycleAction reacting to activated_v1 (resume running) or a
// successful suspended_v1 Reactivate — can (re)bind ONE specific consumer
// independently of a fresh WS ready/reconnect event, using the LATEST
// (connID, bindUser) onConnReady last memoized.
//
// Every failure mode degrades/marks ONLY c, exactly like onConnReady's own
// original loop did, and returns a non-nil error so a caller can tell
// success from failure without re-deriving it from Conn state:
//   - resolveCurrent fails -> SetIdentityDegraded(reasonCurrentIdentityUnresolved).
//   - owner != current -> SetStaleIdentity() — NEVER loads a UAT for that
//     (historical) owner, NEVER binds (the security red line, via session.Gate).
//   - already bound on the memoized connID -> no-op success (no duplicate Bind).
//   - no WS connection has ever become ready (bindUser still nil) ->
//     SetIdentityDegraded("bind_failed: connection_not_ready") rather than panic.
//   - resolveUAT fails -> SetIdentityDegraded("bind_failed: uat_unavailable").
//   - bindUser fails -> SetIdentityDegraded("bind_failed: bind_api_error").
//   - the source epoch advanced while binding (a reconnect superseded this
//     generation) -> errStaleEpoch, result dropped, NOT written to c (the newer
//     generation's onConnReady will bind it).
//   - success -> SetBoundConnID(connID) (which itself clears stale/degraded/
//     nextAction).
func (g *identityGate) bindConsumer(ctx context.Context, c *Conn) error {
	if g == nil {
		return errors.New("identity gate: not configured")
	}

	cur, err := g.resolveCurrent()
	owner := model.OwnerRef{AppID: c.OwnerAppID(), UserOpenID: c.OwnerUserOpenID()}
	switch session.Gate(owner, cur, err) {
	case session.AdmitUnresolved:
		g.logf("WARN: identity gate: resolveCurrent failed for pid=%d: %v", c.PID(), err)
		c.SetIdentityDegraded(reasonCurrentIdentityUnresolved)
		return err
	case session.AdmitStale:
		c.SetStaleIdentity()
		return errOwnerMismatch
	}

	g.connMu.Lock()
	connID, bindUser, epoch := g.connID, g.bindUser, g.epoch.Current()
	g.connMu.Unlock()

	if connID != "" && c.BoundConnID() == connID {
		return nil // same user already bound on this exact connection — no duplicate Bind
	}
	if bindUser == nil {
		g.logf("WARN: identity gate: no WS connection ready yet to bind pid=%d", c.PID())
		c.SetIdentityDegraded("bind_failed: connection_not_ready")
		return errConnectionNotReady
	}

	uat, err := g.resolveUAT(ctx, cur.AppID, cur.UserOpenID)
	if err != nil {
		g.logf("WARN: identity gate: UAT resolution failed for pid=%d: %v", c.PID(), err)
		c.SetIdentityDegraded("bind_failed: uat_unavailable")
		return err
	}
	if err := bindUser(ctx, uat); err != nil {
		g.logf("WARN: identity gate: BindUser failed for pid=%d: %v", c.PID(), err)
		c.SetIdentityDegraded("bind_failed: bind_api_error")
		return err
	}
	// Epoch guard: if a newer WS generation superseded this one while the
	// (possibly slow) bind was in flight, drop the stale result rather than let
	// it overwrite the newer generation's binding on a now-dead connection. The
	// newer generation's onConnReady already (re)binds c on the live connection.
	if !g.epoch.IsCurrent(epoch) {
		g.logf("identity gate: dropping stale bind for pid=%d (WS generation %d superseded)", c.PID(), epoch)
		return errStaleEpoch
	}
	c.SetBoundConnID(connID)
	return nil
}
