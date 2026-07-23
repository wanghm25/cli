// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/larksuite/cli/internal/core"
)

// currentIdentity is the CLI's active identity resolved AT ACTION TIME
// — never a value cached from bus startup. The ONLY authoritative
// comparison key against a consumer's owner identity is appID+userOpenID;
// UAT/tokens are never part of this comparison and never flow through this
// type.
type currentIdentity struct {
	appID      string
	userOpenID string
}

// ownerMatchesCurrent is the owner comparison: owner_app_id +
// owner_user_open_id, exactly. Never compares tokens.
func ownerMatchesCurrent(ownerAppID, ownerUserOpenID string, cur currentIdentity) bool {
	return ownerAppID == cur.appID && ownerUserOpenID == cur.userOpenID
}

// reasonCurrentIdentityUnresolved is the shared SetDegraded reason used by
// both gates (hub.go Publish's delivery gate and onConnReady's bind gate
// below) when resolveCurrent itself fails — distinct from stale_identity
// (which means current WAS resolved but didn't match this consumer's owner).
const reasonCurrentIdentityUnresolved = "current_identity_unresolved"

// resolveCurrentIdentity is the PRODUCTION resolveCurrent:
// LoadMultiAppConfig reads config.json fresh on every call — no in-process
// caching — so this is never a bus-startup-cached value, unlike
// Factory.Config()/ResolveAccount(). CurrentAppConfig("") + Users[0] mirrors
// how the rest of the CLI picks the active profile/user.
func resolveCurrentIdentity() (currentIdentity, error) {
	multi, err := core.LoadMultiAppConfig()
	if err != nil {
		return currentIdentity{}, err
	}
	app := multi.CurrentAppConfig("")
	if app == nil {
		return currentIdentity{}, fmt.Errorf("identity gate: no current app config")
	}
	if len(app.Users) == 0 {
		return currentIdentity{}, fmt.Errorf("identity gate: current app %q has no logged-in user", app.AppId)
	}
	return currentIdentity{appID: app.AppId, userOpenID: app.Users[0].UserOpenId}, nil
}

// identityGate implements the bus-side owner/current identity gate +
// BindUser wiring. resolveCurrent and resolveUAT are injected so
// tests exercise every branch (match/mismatch/error/changing-across-calls)
// without touching disk or keychain; production wires resolveCurrent to
// resolveCurrentIdentity above and resolveUAT to the credential chain
// (cmd/event/bus.go, via Bus.SetIdentityProviders).
type identityGate struct {
	hub            *Hub
	resolveCurrent func() (currentIdentity, error)
	resolveUAT     func(ctx context.Context, appID, userOpenID string) (string, error)
	logger         *log.Logger

	// connMu guards connID/bindUser below: the LATEST values FeishuSource's
	// ready closure passed to onConnReady, memoized so
	// bindConsumer can be invoked independently of a fresh WS ready/reconnect
	// callback — specifically, by subscriptionLifecycleAction
	// (internal/event/bus/lifecycle.go) reacting to an activated_v1/
	// suspended_v1 event, which has no connID/bindUser of its own to pass
	// in. onConnReady itself still writes these on EVERY invocation (never
	// once) — source/feishu.go's ready closure deliberately keeps NOT
	// memoizing on ITS side (cli.Connection().ConnectionID/cli.BindUser are
	// read fresh from the live *larkws.Client every time); this is the ONE
	// place downstream that now remembers the latest pair.
	connMu   sync.Mutex
	connID   string
	bindUser func(context.Context, string) error
}

func newIdentityGate(
	hub *Hub,
	resolveCurrent func() (currentIdentity, error),
	resolveUAT func(ctx context.Context, appID, userOpenID string) (string, error),
	logger *log.Logger,
) *identityGate {
	return &identityGate{hub: hub, resolveCurrent: resolveCurrent, resolveUAT: resolveUAT, logger: logger}
}

func (g *identityGate) logf(format string, args ...interface{}) {
	if g.logger != nil {
		g.logger.Printf(format, args...)
	}
}

// errOwnerMismatch/errConnectionNotReady are bindConsumer's own sentinel
// errors — callers only need "did this fail" (Conn.SetStaleIdentity/
// SetDegraded already recorded WHY), but a plain non-nil error is still
// clearer at call sites than a bare bool.
var (
	errOwnerMismatch      = errors.New("identity gate: owner does not match current identity")
	errConnectionNotReady = errors.New("identity gate: no WS connection ready yet to bind on")
)

// onConnReady is FeishuSource.OnConnReady's implementation: invoked from the
// WS client's OnReady (first usable connection of a run) and OnReconnected
// (every successful reconnect — connID rotates each dial, so a stale
// binding from before is never assumed valid) callbacks. It memoizes
// (connID, bindUser) for bindConsumer's later, independent use
// and then runs bindConsumer for every registered USER consumer (bot
// consumers are invisible here — Hub.userConns already filters them out,
// bot consumers are NEVER identity-gated or BindUser'd). Each
// consumer's outcome (bound / stale / degraded) is entirely bindConsumer's
// concern — a single consumer's failure never affects any other,
// and a resolveCurrent failure degrades only the consumers actually
// evaluated rather than crashing/killing the WS connection.
func (g *identityGate) onConnReady(ctx context.Context, connID string, bindUser func(context.Context, string) error) {
	if g == nil || g.hub == nil {
		return
	}
	g.connMu.Lock()
	g.connID = connID
	g.bindUser = bindUser
	g.connMu.Unlock()

	for _, c := range g.hub.userConns() {
		_ = g.bindConsumer(ctx, c)
	}
}

// bindConsumer performs the per-consumer bind sequence: owner==
// current check -> resolveUAT -> bindUser -> SetBoundConnID. Factored out of
// onConnReady's own loop body so a lifecycle action — e.g.
// subscriptionLifecycleAction reacting to activated_v1 (resume running) or a
// successful suspended_v1 Reactivate — can (re)bind ONE
// specific consumer independently of a fresh WS ready/reconnect event, using
// the LATEST (connID, bindUser) onConnReady last memoized.
//
// Every failure mode degrades/marks ONLY c, exactly like onConnReady's own
// original loop did, and returns a non-nil error so a caller can tell
// success from failure without re-deriving it from Conn state:
//   - resolveCurrent fails -> SetDegraded(reasonCurrentIdentityUnresolved).
//   - owner != current -> SetStaleIdentity() — NEVER loads a UAT for that
//     (historical) owner, NEVER binds (the security red line, reused here).
//   - already bound on the memoized connID -> no-op success (no duplicate
//     Bind).
//   - no WS connection has ever become ready (bindUser still nil) ->
//     SetDegraded("bind_failed: connection_not_ready") rather than panic.
//   - resolveUAT fails -> SetDegraded("bind_failed: uat_unavailable").
//   - bindUser fails -> SetDegraded("bind_failed: bind_api_error").
//   - success -> SetBoundConnID(connID) (which itself clears stale/degraded/
//     nextAction).
func (g *identityGate) bindConsumer(ctx context.Context, c *Conn) error {
	if g == nil {
		return errors.New("identity gate: not configured")
	}

	cur, err := g.resolveCurrent()
	if err != nil {
		g.logf("WARN: identity gate: resolveCurrent failed for pid=%d: %v", c.PID(), err)
		c.SetDegraded(reasonCurrentIdentityUnresolved)
		return err
	}
	if !ownerMatchesCurrent(c.OwnerAppID(), c.OwnerUserOpenID(), cur) {
		c.SetStaleIdentity()
		return errOwnerMismatch
	}

	g.connMu.Lock()
	connID, bindUser := g.connID, g.bindUser
	g.connMu.Unlock()

	if connID != "" && c.BoundConnID() == connID {
		return nil // same user already bound on this exact connection — no duplicate Bind
	}
	if bindUser == nil {
		g.logf("WARN: identity gate: no WS connection ready yet to bind pid=%d", c.PID())
		c.SetDegraded("bind_failed: connection_not_ready")
		return errConnectionNotReady
	}

	uat, err := g.resolveUAT(ctx, cur.appID, cur.userOpenID)
	if err != nil {
		g.logf("WARN: identity gate: UAT resolution failed for pid=%d: %v", c.PID(), err)
		c.SetDegraded("bind_failed: uat_unavailable")
		return err
	}
	if err := bindUser(ctx, uat); err != nil {
		g.logf("WARN: identity gate: BindUser failed for pid=%d: %v", c.PID(), err)
		c.SetDegraded("bind_failed: bind_api_error")
		return err
	}
	c.SetBoundConnID(connID)
	return nil
}
