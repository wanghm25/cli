// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

// Package session is the single owner of event-consumer admission: the
// owner/current security gate, the live current-identity read, the source
// epoch, and the per-consumer readiness state machine.
//
// It is the shared low-level owner every other event package can import
// without a cycle: it depends only on internal/core (the config read) and
// internal/event/model (OwnerRef), plus stdlib — never on bus, consume,
// lifecycle, or cmd/event. Those packages, which historically each kept their
// own copy of "read the current identity" and "does owner match current",
// now all defer to this one implementation.
//
// The owner/current policy is FAIL-CLOSED and must not change: when the
// runtime current identity does not match a consumer's command-resolved
// owner, delivery stops, remote actions are forbidden, the consumer is marked
// stale_identity, and no historical owner's UAT is ever loaded. Gate is the
// one place that policy is decided; the four call sites (hub delivery, the
// bind gate, the lifecycle eligibility gate, and the encrypt-key fetch) each
// apply their own side effects to its decision.
package session
