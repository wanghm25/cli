// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"context"
	"fmt"
	"log"

	"github.com/larksuite/cli/internal/core"
)

// currentIdentity is the CLI's active identity resolved AT ACTION TIME (spec
// §4.4) — never a value cached from bus startup. The ONLY authoritative
// comparison key against a consumer's owner identity is appID+userOpenID;
// UAT/tokens are never part of this comparison and never flow through this
// type.
type currentIdentity struct {
	appID      string
	userOpenID string
}

// ownerMatchesCurrent is the spec §4.4 comparison: owner_app_id +
// owner_user_open_id, exactly. Never compares tokens.
func ownerMatchesCurrent(ownerAppID, ownerUserOpenID string, cur currentIdentity) bool {
	return ownerAppID == cur.appID && ownerUserOpenID == cur.userOpenID
}

// reasonCurrentIdentityUnresolved is the shared SetDegraded reason used by
// both gates (hub.go Publish's delivery gate and onConnReady's bind gate
// below) when resolveCurrent itself fails — distinct from stale_identity
// (which means current WAS resolved but didn't match this consumer's owner).
const reasonCurrentIdentityUnresolved = "current_identity_unresolved"

// resolveCurrentIdentity is the PRODUCTION resolveCurrent (spec §4.4):
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
// BindUser wiring (spec §4.4). resolveCurrent and resolveUAT are injected so
// tests exercise every branch (match/mismatch/error/changing-across-calls)
// without touching disk or keychain; production wires resolveCurrent to
// resolveCurrentIdentity above and resolveUAT to the credential chain
// (cmd/event/bus.go, via Bus.SetIdentityProviders).
type identityGate struct {
	hub            *Hub
	resolveCurrent func() (currentIdentity, error)
	resolveUAT     func(ctx context.Context, appID, userOpenID string) (string, error)
	logger         *log.Logger
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

// onConnReady is FeishuSource.OnConnReady's implementation: invoked from the
// WS client's OnReady (first usable connection of a run) and OnReconnected
// (every successful reconnect — connID rotates each dial, so a stale
// binding from before is never assumed valid) callbacks.
//
// For every registered USER consumer (bot consumers are invisible here —
// Hub.userConns already filters them out, spec §4.4: bot consumers are
// NEVER identity-gated or BindUser'd):
//   - owner == current (resolved FRESH right now, never cached) and not yet
//     bound on this exact connID -> resolveUAT(current) then bindUser; a
//     per-consumer failure (either resolveUAT or bindUser) degrades ONLY
//     that consumer (Conn.SetDegraded) — every other consumer is untouched.
//   - owner == current and ALREADY bound on this connID -> no-op (no
//     duplicate Bind).
//   - owner != current -> Conn.SetStaleIdentity(); NEVER bound, NEVER loads
//     a UAT for that (historical) owner, NEVER touches the remote
//     connection for it.
//
// A resolveCurrent failure degrades every user consumer (fail-closed) and
// returns without touching the WS — it must never crash/kill the
// connection just because config.json was transiently unreadable.
func (g *identityGate) onConnReady(ctx context.Context, connID string, bindUser func(context.Context, string) error) {
	if g == nil || g.hub == nil {
		return
	}
	conns := g.hub.userConns()
	if len(conns) == 0 {
		return
	}

	cur, err := g.resolveCurrent()
	if err != nil {
		g.logf("WARN: identity gate: resolveCurrent failed at connect time, degrading %d user consumer(s): %v", len(conns), err)
		for _, c := range conns {
			c.SetDegraded(reasonCurrentIdentityUnresolved)
		}
		return
	}

	for _, c := range conns {
		if !ownerMatchesCurrent(c.OwnerAppID(), c.OwnerUserOpenID(), cur) {
			c.SetStaleIdentity()
			continue
		}
		if c.BoundConnID() == connID {
			continue // same user already bound on this exact connection — no duplicate Bind
		}
		uat, err := g.resolveUAT(ctx, cur.appID, cur.userOpenID)
		if err != nil {
			g.logf("WARN: identity gate: UAT resolution failed for pid=%d: %v", c.PID(), err)
			c.SetDegraded("bind_failed: uat_unavailable")
			continue
		}
		if err := bindUser(ctx, uat); err != nil {
			g.logf("WARN: identity gate: BindUser failed for pid=%d: %v", c.PID(), err)
			c.SetDegraded("bind_failed: bind_api_error")
			continue
		}
		c.SetBoundConnID(connID)
	}
}
