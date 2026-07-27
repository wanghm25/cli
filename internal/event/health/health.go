// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

// Package health is the per-dimension health record for one event consumer.
//
// It replaces the single shared "degraded reason" slot that hub / identity /
// lifecycle / decrypt subsystems all used to overwrite last-writer-wins, with
// one independent Fact per DIMENSION. Each subsystem writes ONLY its own
// dimension, so a subscription-suspended fact and an identity-stale fact (and a
// decrypt-failed fact) can all be present at once and none clobbers another —
// and clearing one dimension (e.g. a successful BindUser clearing Identity)
// never wipes the others (Subscription / Decryption).
//
// The package is deliberately dependency-free: it imports no SDK, no bus, and
// nothing else from this repo — only the standard library — so it can be the
// shared vocabulary every subsystem writes to without creating an import cycle.
// Facts is concurrency-safe because a consumer's health is mutated from several
// goroutines (the WS ready/reconnect callback, the source emit path, the
// lifecycle executor workers) and read by the status query.
package health

import (
	"sync"
	"time"
)

// Severity ranks how serious a Fact is for a display.
type Severity int

const (
	// SeverityAdvisory: informational — the consumer is still functioning but
	// something warrants attention (e.g. the remote subscription is expiring
	// soon).
	SeverityAdvisory Severity = iota
	// SeverityDegraded: the consumer is functionally degraded on this dimension
	// (e.g. owner identity unresolved, remote subscription deleted, decrypt
	// failing).
	SeverityDegraded
)

// String renders a Severity as a short, stable token for status/JSON.
func (s Severity) String() string {
	switch s {
	case SeverityAdvisory:
		return "advisory"
	case SeverityDegraded:
		return "degraded"
	default:
		return "unknown"
	}
}

// Dimension identifies one independent axis of a consumer's health. Each is
// owned by exactly ONE subsystem (see the package doc): Identity by the
// identity gate, Subscription by the lifecycle control plane, Decryption by the
// decrypt-failure path, and Source/Delivery are reserved for the source and
// fan-out paths (no writer today — present for open-closed extension).
type Dimension int

const (
	Identity Dimension = iota
	Subscription
	Decryption
	Source
	Delivery
	// numDimensions is the count of dimensions; keep it last.
	numDimensions
)

// String renders a Dimension as a short, stable token for status/JSON.
func (d Dimension) String() string {
	switch d {
	case Identity:
		return "identity"
	case Subscription:
		return "subscription"
	case Decryption:
		return "decryption"
	case Source:
		return "source"
	case Delivery:
		return "delivery"
	default:
		return "unknown"
	}
}

// Fact is one dimension's health. Reason=="" is the healthy zero value; a
// non-empty Reason is a short, classified token (never a raw error or key
// material — this may be surfaced by the status command). When records when the
// fact was last set.
type Fact struct {
	Reason   string
	Severity Severity
	When     time.Time
}

// OK reports whether this dimension is healthy (no fact set).
func (f Fact) OK() bool { return f.Reason == "" }

// Facts holds one Fact per Dimension. The zero value is not usable; construct
// with New. All methods are safe for concurrent use.
type Facts struct {
	mu   sync.Mutex
	dims [numDimensions]Fact
}

// New returns an all-healthy Facts.
func New() *Facts { return &Facts{} }

// Set records f on dimension d, overwriting any prior fact for that dimension
// ONLY — never any other dimension. A zero When on a non-empty fact is stamped
// with the current time so callers don't each have to.
func (h *Facts) Set(d Dimension, f Fact) {
	if f.Reason != "" && f.When.IsZero() {
		f.When = time.Now()
	}
	h.mu.Lock()
	h.dims[d] = f
	h.mu.Unlock()
}

// Degrade is a convenience for Set(d, Fact{Reason, SeverityDegraded}).
func (h *Facts) Degrade(d Dimension, reason string) {
	h.Set(d, Fact{Reason: reason, Severity: SeverityDegraded})
}

// Clear resets dimension d to healthy, leaving every other dimension untouched.
func (h *Facts) Clear(d Dimension) {
	h.mu.Lock()
	h.dims[d] = Fact{}
	h.mu.Unlock()
}

// Get returns dimension d's current fact (the zero Fact when healthy).
func (h *Facts) Get(d Dimension) Fact {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.dims[d]
}

// Reason returns dimension d's current reason ("" when healthy) — a shorthand
// for Get(d).Reason.
func (h *Facts) Reason(d Dimension) string {
	return h.Get(d).Reason
}

// DimensionFact pairs a Dimension with its Fact for enumeration.
type DimensionFact struct {
	Dimension Dimension
	Fact
}

// Snapshot returns every non-healthy dimension's fact, in Dimension order. The
// status command uses this to surface MULTIPLE independent facts at once. An
// all-healthy Facts returns nil.
func (h *Facts) Snapshot() []DimensionFact {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []DimensionFact
	for d := Dimension(0); d < numDimensions; d++ {
		if h.dims[d].Reason != "" {
			out = append(out, DimensionFact{Dimension: d, Fact: h.dims[d]})
		}
	}
	return out
}

// Any reports whether at least one dimension is unhealthy.
func (h *Facts) Any() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for d := Dimension(0); d < numDimensions; d++ {
		if h.dims[d].Reason != "" {
			return true
		}
	}
	return false
}
