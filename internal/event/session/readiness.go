// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package session

import "sync"

// Readiness is one point on a consumer's admission path. The states are
// ordered, but "Ready" (the value that gates the stderr ready marker) is NOT
// simply the highest state reached — it is the real terminal readiness for that
// consumer's identity type, computed by Consumer.Ready:
//
//	Accepted     -> admitted to the hub (the IPC hello_ack milestone)
//	SourceReady  -> the WS source connection is up
//	Bound        -> BindUser succeeded (user identities only)
//	KeyReady     -> the subscription encrypt_key is cached (encrypted only)
//	RouteReady   -> terminal: everything this identity type requires is satisfied
//
// The point of separating these is that Accepted != Ready: a USER refined
// consumer is admitted (Accepted) the moment it registers, but it is only truly
// Ready — allowed to print "[event] ready" — once BindUser has succeeded, so it
// never claims readiness while it would silently receive nothing.
type Readiness int

const (
	StateAccepted Readiness = iota
	StateSourceReady
	StateBound
	StateKeyReady
	StateRouteReady
)

func (r Readiness) String() string {
	switch r {
	case StateAccepted:
		return "accepted"
	case StateSourceReady:
		return "source_ready"
	case StateBound:
		return "bound"
	case StateKeyReady:
		return "key_ready"
	case StateRouteReady:
		return "route_ready"
	default:
		return "unknown"
	}
}

// Consumer is one consumer's readiness tracker: it owns "what does Ready mean
// for this identity type" so the bus, rather than hard-coding the answer at the
// ack site, asks the session. A user consumer requires Bound; an encrypted
// consumer requires KeyReady; a bot/legacy plaintext consumer is Ready as soon
// as it is admitted. It is safe for concurrent use.
type Consumer struct {
	// requiresBind is fixed at construction from the owner's user_open_id: a
	// user owner (non-empty) must BindUser before it is Ready; a bot/legacy
	// owner ("") never binds.
	requiresBind bool
	// requiresKey is fixed from include_resource_data: an encrypted subscription
	// must have its encrypt_key cached before it is Ready.
	requiresKey bool

	mu          sync.Mutex
	accepted    bool
	sourceReady bool
	bound       bool
	keyReady    bool
}

// NewConsumer builds a readiness tracker for a consumer whose owner user_open_id
// and include_resource_data flag are known at Hello time. ownerUserOpenID == ""
// marks a bot/legacy consumer (never bound); includeResourceData marks an
// encrypted subscription (needs a key).
func NewConsumer(ownerUserOpenID string, includeResourceData bool) *Consumer {
	return &Consumer{
		requiresBind: ownerUserOpenID != "",
		requiresKey:  includeResourceData,
	}
}

// RequiresBind reports whether this consumer must BindUser before it is Ready
// (i.e. it is a user consumer). The bus uses this to decide whether to run the
// owner==current bind gate before acking.
func (c *Consumer) RequiresBind() bool { return c.requiresBind }

// RequiresKey reports whether this consumer must have its encrypt_key cached
// before it is Ready (i.e. include_resource_data was requested).
func (c *Consumer) RequiresKey() bool { return c.requiresKey }

// MarkAccepted records that the consumer has been admitted to the hub.
func (c *Consumer) MarkAccepted() { c.set(func() { c.accepted = true }) }

// MarkSourceReady records that the WS source connection is up.
func (c *Consumer) MarkSourceReady() { c.set(func() { c.sourceReady = true }) }

// MarkBound records a successful BindUser.
func (c *Consumer) MarkBound() { c.set(func() { c.bound = true }) }

// MarkKeyReady records that the subscription encrypt_key is cached.
func (c *Consumer) MarkKeyReady() { c.set(func() { c.keyReady = true }) }

func (c *Consumer) set(fn func()) {
	c.mu.Lock()
	fn()
	c.mu.Unlock()
}

// Ready reports the real terminal readiness for this consumer's identity type:
// it must be admitted, a user consumer must additionally be Bound, and an
// encrypted consumer must additionally be KeyReady. This — not Accepted — is
// what gates the stderr ready marker.
func (c *Consumer) Ready() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.accepted {
		return false
	}
	if c.requiresBind && !c.bound {
		return false
	}
	if c.requiresKey && !c.keyReady {
		return false
	}
	return true
}

// State returns the terminal-most Readiness reached (advisory/introspection).
// It reports RouteReady only once Ready() would be true, so it never claims a
// terminal state a user consumer has not actually earned.
func (c *Consumer) State() Readiness {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.accepted && (!c.requiresBind || c.bound) && (!c.requiresKey || c.keyReady):
		return StateRouteReady
	case c.keyReady:
		return StateKeyReady
	case c.bound:
		return StateBound
	case c.sourceReady:
		return StateSourceReady
	default:
		return StateAccepted
	}
}
