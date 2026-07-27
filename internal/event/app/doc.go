// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

// Package app owns the event subsystem's use-case orchestration — the layer
// between the Cobra commands (cmd/event, cmd/event/subscription) and the
// owners built in PR1/PR2 (the catalog, the subscription Observe/Plan/Apply
// Controller, and the platform/lark gateway).
//
// A command's RunE resolves flags + identity, builds the identity-bound
// dependencies, then hands them to a use case here and renders the domain
// result it returns. The use cases hold the flow decisions — which remote
// setup a consume invocation entails, when to Plan, when to Apply, how to
// classify a non-writable plan — so no Reconcile/Plan/Apply logic lives in a
// command. Rendering (JSON shapes, text, typed presentation errors, Cobra
// flag reads) stays in the command layer; this package never writes output,
// builds a wire shape, or reads a flag.
//
// The three use cases mirror the three command surfaces:
//
//   - SubscriptionUseCase — the remote Subscription management plane's
//     Observe→Plan→Apply provisioning flow (subscription create).
//   - ConsumeUseCase — the `event consume` flow: it classifies an invocation
//     into an InvocationDescriptor (legacy / remote-subscription / no-setup)
//     and dispatches the bus/consume start accordingly.
//   - StatusUseCase — the read-only `event status` orchestration.
package app
