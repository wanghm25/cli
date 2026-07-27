// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package app

import (
	"context"

	"github.com/larksuite/cli/internal/core"
	event "github.com/larksuite/cli/internal/event"
)

// SetupKind classifies the remote setup one `event consume` invocation entails.
// It is the explicit form of the isRefined/--dry-run branching the command used
// to do inline, so the risk policy and the dispatch both read one value.
type SetupKind int

const (
	// SetupRemoteSubscription: a materialized refined EventKey. Consuming it
	// provisions (creates / reuses / reactivates) a remote Subscription through
	// the subscription Controller before the bus starts — a real write-level
	// side effect. This is the only setup with write risk.
	SetupRemoteSubscription SetupKind = iota
	// SetupLegacy: an ordinary (legacy) EventKey, real run. It has no remote
	// Subscription resource; the bus's own PreConsume provisions delivery. Read
	// risk — it never writes remote management state.
	SetupLegacy
	// SetupNone: an ordinary (legacy) EventKey under --dry-run. A legacy key has
	// no remote-Subscription plan to preview, so this is a zero-side-effect
	// notice: no identity resolution, no bus, no write. Read risk.
	SetupNone
)

// InvocationDescriptor is the explicit classification of one consume
// invocation, derived purely from the resolved EventKey + the --dry-run flag
// before any identity or remote work. The command builds it once, consults its
// risk policy (Risk), and hands it to ConsumeUseCase.Run for dispatch — instead
// of re-deriving isRefined/dry-run branches inline.
type InvocationDescriptor struct {
	Kind     SetupKind
	Resolved event.ResolvedEventKey
}

// DescribeConsume classifies a resolved EventKey + dry-run flag into an
// InvocationDescriptor:
//
//   - a refined key               -> SetupRemoteSubscription (provisions a
//     remote Subscription; write risk). A refined --dry-run is STILL a
//     remote-subscription setup — it runs Probe+Plan and stops before the write
//     — so it stays SetupRemoteSubscription, not SetupNone.
//   - a legacy key with --dry-run -> SetupNone (nothing to preview).
//   - a legacy key, real run      -> SetupLegacy (the bus PreConsume provisions).
func DescribeConsume(resolved event.ResolvedEventKey, dryRun bool) InvocationDescriptor {
	switch {
	case resolved.IsRefined:
		return InvocationDescriptor{Kind: SetupRemoteSubscription, Resolved: resolved}
	case dryRun:
		return InvocationDescriptor{Kind: SetupNone, Resolved: resolved}
	default:
		return InvocationDescriptor{Kind: SetupLegacy, Resolved: resolved}
	}
}

// Risk is the argument-aware risk policy the command consults INSTEAD of
// mutating its Cobra risk annotation inside RunE. A remote-subscription setup is
// core.RiskWrite (it may create/reuse/reactivate a remote Subscription); every
// other setup is core.RiskRead (a legacy consume never writes remote management
// state, and a legacy dry-run does nothing at all). The command's STATIC
// annotation stays the conservative worst case (write) for anything inspecting
// risk before the argument is known; Risk is the precise per-invocation value
// once the EventKey is resolved.
func (d InvocationDescriptor) Risk() string {
	if d.Kind == SetupRemoteSubscription {
		return core.RiskWrite
	}
	return core.RiskRead
}

// ConsumeSetup is the set of leaf execution strategies ConsumeUseCase.Run
// dispatches to, one per SetupKind. The command supplies the implementation,
// wiring each to the Factory-resolved identity, clients, and the
// consume.Run / consume.RunRefined start it needs — so the branch DECISION
// lives in the use case, not inline in the command's RunE.
type ConsumeSetup interface {
	// RunRemoteSubscription drives a refined key: provision the remote
	// Subscription (Observe->Plan->[dry-run exit]->Apply) then start the bus.
	RunRemoteSubscription(ctx context.Context) error
	// RunLegacy drives an ordinary key's real run: console/scope preflight then
	// the bus consume start.
	RunLegacy(ctx context.Context) error
	// RunNoSetup drives an ordinary key under --dry-run: emit the "nothing to
	// preview" notice with zero side effects.
	RunNoSetup(ctx context.Context) error
}

// ConsumeUseCase orchestrates the `event consume` flow: it maps an
// InvocationDescriptor to the matching ConsumeSetup strategy and runs it. It
// owns the descriptor->execution dispatch so the command's RunE stays a thin
// flags->identity->use-case shell.
type ConsumeUseCase struct {
	setup ConsumeSetup
}

// NewConsumeUseCase builds a ConsumeUseCase over the command-supplied setup
// strategies.
func NewConsumeUseCase(setup ConsumeSetup) ConsumeUseCase {
	return ConsumeUseCase{setup: setup}
}

// Run dispatches d to the matching setup strategy.
func (uc ConsumeUseCase) Run(ctx context.Context, d InvocationDescriptor) error {
	switch d.Kind {
	case SetupRemoteSubscription:
		return uc.setup.RunRemoteSubscription(ctx)
	case SetupNone:
		return uc.setup.RunNoSetup(ctx)
	default: // SetupLegacy
		return uc.setup.RunLegacy(ctx)
	}
}
