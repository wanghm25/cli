// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"context"

	"github.com/larksuite/cli/internal/event/bus/lifecycle"
	"github.com/larksuite/cli/internal/event/session"
)

// This file bridges the bus's concrete types to the interfaces the lifecycle
// control plane (internal/event/bus/lifecycle) needs, so the dependency only
// ever points bus -> lifecycle (lifecycle never imports bus). The compile-time
// assertions below fail loudly if a signature ever drifts out of alignment.
var (
	_ lifecycle.Conn         = (*Conn)(nil)
	_ lifecycle.IdentityGate = (*identityGate)(nil)
	_ lifecycle.Registry     = lifecycleRegistry{}
)

// lifecycleRegistry adapts *Hub to lifecycle.Registry, performing the
// []*Conn -> []lifecycle.Conn conversion exactly once per lookup so no handler
// downstream repeats it.
type lifecycleRegistry struct{ hub *Hub }

func (r lifecycleRegistry) ConnsByRemoteSubscriptionID(remoteSubID string) []lifecycle.Conn {
	conns := r.hub.connsByRemoteSubscriptionID(remoteSubID)
	out := make([]lifecycle.Conn, len(conns))
	for i, c := range conns {
		out[i] = c
	}
	return out
}

// lifecycleRegistry returns this Hub as a lifecycle.Registry.
func (h *Hub) lifecycleRegistry() lifecycle.Registry { return lifecycleRegistry{hub: h} }

// ResolveCurrent/ResolveUAT/BindConsumer make *identityGate satisfy
// lifecycle.IdentityGate. resolveCurrent/resolveUAT are the gate's injected
// funcs; the wrappers exist because interface satisfaction needs methods. Both
// the gate and the lifecycle interface now speak the shared
// session.CurrentIdentity, so ResolveCurrent is a straight pass-through — no
// value-type bridging remains.
func (g *identityGate) ResolveCurrent() (session.CurrentIdentity, error) {
	return g.resolveCurrent()
}

func (g *identityGate) ResolveUAT(ctx context.Context, appID, userOpenID string) (string, error) {
	return g.resolveUAT(ctx, appID, userOpenID)
}

// BindConsumer recovers the concrete *Conn from the lifecycle.Conn the action
// hands back. Every lifecycle.Conn originates from lifecycleRegistry above,
// which only ever wraps a *Conn, so the assertion always holds.
func (g *identityGate) BindConsumer(ctx context.Context, c lifecycle.Conn) error {
	return g.bindConsumer(ctx, c.(*Conn))
}
