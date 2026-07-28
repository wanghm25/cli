// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/delivery"
	"github.com/larksuite/cli/internal/event/health"
	"github.com/larksuite/cli/internal/event/model"
	"github.com/larksuite/cli/internal/event/protocol"
	"github.com/larksuite/cli/internal/event/routing"
	"github.com/larksuite/cli/internal/event/session"
)

// exclusiveCleanupWaitTimeout bounds how long TryRegisterExclusive waits for an
// in-progress cleanup of the same subscription before rejecting, so a stuck
// cleanup can never wedge new consumers forever. Kept below the consumer's
// hello_ack deadline (consume.helloAckTimeout = 5s) so the reject still reaches
// the consumer as a clean failed_precondition instead of a handshake timeout.
// Override with LARKSUITE_CLI_EVENT_EXCLUSIVE_WAIT_TIMEOUT (a Go duration such as
// "2s"); values at or above the 5s handshake deadline are not recommended.
var exclusiveCleanupWaitTimeout = resolveExclusiveCleanupWaitTimeout()

func resolveExclusiveCleanupWaitTimeout() time.Duration {
	const def = 3 * time.Second
	if v := os.Getenv("LARKSUITE_CLI_EVENT_EXCLUSIVE_WAIT_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return def
}

// Subscriber is the interface a connection must satisfy for Hub registration.
type Subscriber interface {
	EventKey() string
	// SubscriptionID identifies the per-resource subscription for dedup purposes.
	// When no resource qualifier is needed it equals EventKey.
	SubscriptionID() string
	EventTypes() []string
	// RemoteSubscriptionID is the remote Subscription (OpenAPI primary key,
	// e.g. "sub_xxx") this consumer is bound to; "" means legacy (routed by
	// EventTypes() only, the dual-index routing matrix). Distinct
	// from SubscriptionID, which is the local per-resource fingerprint used
	// for the registration/cleanup machinery above and is unrelated to this.
	RemoteSubscriptionID() string
	// OwnerAppID and OwnerUserOpenID are the owner identity fixed at this
	// consumer's registration: owner_app_id + owner_user_open_id
	// is the ONLY comparison key against the freshly-resolved "current"
	// identity — UAT is never compared. OwnerUserOpenID()=="" marks a bot
	// consumer OR a legacy registration (Hello.Identity/
	// UserOpenID arrive "" until a refined client populates them); the routing
	// identity gate (and identity.go's bind gate) bypass these entirely —
	// bot consumers are NEVER identity-gated or BindUser'd.
	OwnerAppID() string
	OwnerUserOpenID() string
	SendCh() chan interface{}
	PID() int
	IncrementReceived()
	Received() int64
	// PushDropOldest enqueues atomically with drop-oldest backpressure.
	PushDropOldest(msg interface{}) (enqueued, dropped bool)
	// TrySend is non-evictive but shares PushDropOldest's mutex.
	TrySend(msg interface{}) bool
	DroppedCount() int64
	IncrementDropped()
	// NextSeq returns a monotonic per-subscriber seq; tests may return 0.
	NextSeq() uint64
}

type Hub struct {
	mu          sync.RWMutex
	subscribers map[Subscriber]struct{}
	// subCounts is keyed by SubscriptionID (not EventKey) so that different
	// per-resource subscriptions sharing the same EventKey are deduped independently.
	subCounts map[string]int
	// cleanupInProgress[subscriptionID] holds a channel closed on release;
	// presence means a cleanup lock is held for that subscription.
	cleanupInProgress map[string]chan struct{}
	logger            atomic.Pointer[log.Logger]

	// legacyDedup and refinedDedup are two separate dedup domains, each consulted
	// by Publish AFTER routing has chosen the eligible destinations for that
	// domain — never as one global gate at the source entry. Publish takes a
	// single atomic Claim on the domain's filter BEFORE delivering: only the
	// claim-winner delivers, so two concurrent same-key Publishes can never both
	// deliver (the check-then-commit TOCTOU). The claim is gated on the domain
	// having an eligible endpoint, so an event that reached no one claims nothing
	// and stays re-processable; a claim expires via the filter's TTL, so the
	// seen-set can't grow unbounded. legacyDedup keys by event_id alone (applied
	// to every legacy delivery regardless of remote_subscription_id — legacy
	// consumers receive refined events too). refinedDedup keys by
	// subscription_event_id, which is always present and globally unique for a
	// refined event, so it is the whole key (no remote_subscription_id scoping and
	// no fallback). Each *event.DedupFilter is self-locking; do not add locking
	// around them.
	legacyDedup  *event.DedupFilter
	refinedDedup *event.DedupFilter

	// currentResolver is the identity gate's fresh-current-identity resolver,
	// wired by Bus.SetIdentityProviders via SetCurrentResolver.
	// nil (the zero value — every non-gated NewHub()/NewBus() caller)
	// means NO identity gating: every consumer is delivered according to the
	// legacy ungated behavior. Guarded by mu (read together with the
	// subscribers snapshot when Publish builds the routing snapshot) rather than
	// a separate lock/atomic — it changes at most once in practice (bus
	// construction, before Run starts accepting events). Handed to the pure
	// router as the read-only identity port; the router owns no I/O of its own.
	currentResolver func() (session.CurrentIdentity, error)

	// crossCheckDropped counts events dropped by the refined cross-check
	// (routing's DropCrossCheck): a remote_subscription_id match that, on closer
	// inspection, disagreed with the matched consumer's own stored intent on
	// event_type/target_resource/authority. Distinct from per-consumer
	// DroppedCount (backpressure evictions) — this is a routing-integrity
	// signal, expected to stay at 0 in normal operation. The router DECIDES the
	// drop; Publish applies this counter and the WARN log.
	crossCheckDropped atomic.Int64

	// router and broker are the pure routing decision and the delivery
	// transport this Hub delegates to. Both are stateless (the zero value is
	// ready). Publish is the thin pipeline: build a snapshot -> router.Plan ->
	// atomic dedup claim -> broker.Deliver.
	router routing.Router
	broker delivery.Broker
}

func NewHub() *Hub {
	return &Hub{
		subscribers:       make(map[Subscriber]struct{}),
		subCounts:         make(map[string]int),
		cleanupInProgress: make(map[string]chan struct{}),
		legacyDedup:       event.NewDedupFilter(),
		refinedDedup:      event.NewDedupFilter(),
	}
}

// SetLogger attaches a logger (nil tolerated).
func (h *Hub) SetLogger(l *log.Logger) { h.logger.Store(l) }

// SetCurrentResolver wires the identity gate's fresh-current-identity
// resolver into the routing snapshot's identity port. nil disables gating
// entirely (the NewHub() default) — this is how every non-gated caller
// (and every test that never calls this) keeps exactly today's behavior.
func (h *Hub) SetCurrentResolver(fn func() (session.CurrentIdentity, error)) {
	h.mu.Lock()
	h.currentResolver = fn
	h.mu.Unlock()
}

// userConns returns every registered *Conn with a non-empty owner user
// (OwnerUserOpenID() != "") — i.e. every USER consumer whose owner was
// fixed at registration. Bot/legacy consumers (OwnerUserOpenID()
// == "") are excluded: they are never identity-gated or BindUser'd. Only
// *Conn is inspected (not the bare Subscriber interface) because the
// identity gate needs to mutate Conn-only state (BoundConnID/StaleIdentity/
// the per-dimension health facts) that intentionally isn't part of the
// Subscriber contract every mock must satisfy — mirrors how findSubscriberByPID
// (handle_hello_test.go) whitebox-iterates h.subscribers from within this
// same package.
func (h *Hub) userConns() []*Conn {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var out []*Conn
	for s := range h.subscribers {
		c, ok := s.(*Conn)
		if !ok || c.OwnerUserOpenID() == "" {
			continue
		}
		out = append(out, c)
	}
	return out
}

// connsByRemoteSubscriptionID returns every registered *Conn bound to the
// given remote Subscription id (mirrors userConns's shape/whitebox
// s.(*Conn) type-assert above — only *Conn carries the lifecycle-summary
// state LifecycleExecutor's summary action needs to mutate).
// remoteSubID=="" always returns nil: there is no "legacy" bucket
// here — callers only ever look this up with a concrete
// remote_subscription_id already read off a LifecycleEvent, which validates
// non-empty before reaching this point (internal/event/bus/lifecycle.go's
// Submit).
func (h *Hub) connsByRemoteSubscriptionID(remoteSubID string) []*Conn {
	if remoteSubID == "" {
		return nil
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	var out []*Conn
	for s := range h.subscribers {
		c, ok := s.(*Conn)
		if !ok || c.RemoteSubscriptionID() != remoteSubID {
			continue
		}
		out = append(out, c)
	}
	return out
}

// UnregisterAndIsLast removes s and reports whether it was last for its SubscriptionID; stale unregisters are no-ops.
func (h *Hub) UnregisterAndIsLast(s Subscriber) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, registered := h.subscribers[s]; !registered {
		return false
	}
	delete(h.subscribers, s)
	sid := s.SubscriptionID()
	h.subCounts[sid]--
	isLast := h.subCounts[sid] == 0
	if isLast {
		delete(h.subCounts, sid)
	}
	return isLast
}

// AcquireCleanupLock reserves cleanup rights iff exactly one subscriber exists for subscriptionID and no lock is held.
// Count==0 is rejected (would block future Register calls). On true return, caller MUST Release.
func (h *Hub) AcquireCleanupLock(subscriptionID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subCounts[subscriptionID] != 1 {
		return false
	}
	if _, alreadyLocked := h.cleanupInProgress[subscriptionID]; alreadyLocked {
		return false
	}
	h.cleanupInProgress[subscriptionID] = make(chan struct{})
	return true
}

// ReleaseCleanupLock is idempotent; OnClose calls unconditionally.
func (h *Hub) ReleaseCleanupLock(subscriptionID string) {
	h.mu.Lock()
	ch := h.cleanupInProgress[subscriptionID]
	delete(h.cleanupInProgress, subscriptionID)
	h.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

// RegisterAndIsFirst adds s to the hub and reports whether it's the first
// subscriber for its SubscriptionID. If a cleanup is in progress for
// s.SubscriptionID() (another conn holds the cleanup lock), this waits until
// cleanup releases before registering — closing the PreShutdownCheck ×
// Hello TOCTOU race. The wait releases h.mu before blocking on the
// channel, so concurrent operations on other subscriptions aren't stalled.
func (h *Hub) RegisterAndIsFirst(s Subscriber) bool {
	sid := s.SubscriptionID()
	for {
		h.mu.Lock()
		ch, locked := h.cleanupInProgress[sid]
		if locked {
			h.mu.Unlock()
			<-ch // wait for release, then re-check (defensive against races)
			continue
		}
		isFirst := h.subCounts[sid] == 0
		h.subscribers[s] = struct{}{}
		h.subCounts[sid]++
		h.mu.Unlock()
		return isFirst
	}
}

// TryRegisterExclusive registers s only when no subscriber holds s.SubscriptionID()
// and any in-progress cleanup for that subscription finishes within
// exclusiveCleanupWaitTimeout. On failure it returns (false, reason): either a
// duplicate consumer already holds the subscription, or the cleanup did not
// finish in time — the timeout guarantees a stuck cleanup can never wedge new
// consumers forever. reason is "" on success. Mirrors RegisterAndIsFirst's wait
// on in-progress cleanup, but bounded.
func (h *Hub) TryRegisterExclusive(s Subscriber) (bool, string) {
	sid := s.SubscriptionID()
	deadline := time.Now().Add(exclusiveCleanupWaitTimeout)
	for {
		h.mu.Lock()
		ch, locked := h.cleanupInProgress[sid]
		if locked {
			h.mu.Unlock()
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return false, "timed out waiting for the previous consumer's cleanup to finish; retry shortly"
			}
			timer := time.NewTimer(remaining)
			select {
			case <-ch:
				// Stop+drain so a timer that fired concurrently with Stop isn't left on .C.
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				continue
			case <-timer.C:
				return false, "timed out waiting for the previous consumer's cleanup to finish; retry shortly"
			}
		}
		if h.subCounts[sid] != 0 {
			pid := h.existingPIDForSubscriptionLocked(sid)
			h.mu.Unlock()
			return false, fmt.Sprintf("another consumer (pid %d) is already running for this subscription", pid)
		}
		h.subscribers[s] = struct{}{}
		h.subCounts[sid]++
		h.mu.Unlock()
		return true, ""
	}
}

// existingPIDForSubscriptionLocked returns the PID of one subscriber for sid.
// Caller must hold h.mu.
func (h *Hub) existingPIDForSubscriptionLocked(sid string) int {
	for sub := range h.subscribers {
		if sub.SubscriptionID() == sid {
			return sub.PID()
		}
	}
	return 0
}

// snapshot builds an immutable routing.Snapshot of the currently registered
// consumers, plus the identity port, under a single RLock. The identity
// resolver is captured alongside the subscribers under the SAME lock so a
// concurrent SetCurrentResolver can never interleave with a snapshot read;
// the router calls it later (outside the lock, at most once, lazily) exactly as
// the old inline gate did. TargetResource is *Conn-only listening intent — a
// bare Subscriber (a test fake, never a real registration) carries none, so it
// snapshots "" and the cross-check holds it only to the presence check.
func (h *Hub) snapshot() routing.Snapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()
	consumers := make([]routing.Consumer, 0, len(h.subscribers))
	for s := range h.subscribers {
		c := routing.Consumer{
			Ref:                  s,
			EventTypes:           s.EventTypes(),
			RemoteSubscriptionID: s.RemoteSubscriptionID(),
			Owner:                model.OwnerRef{AppID: s.OwnerAppID(), UserOpenID: s.OwnerUserOpenID()},
		}
		if conn, ok := s.(*Conn); ok {
			c.TargetResource = conn.TargetResource()
		}
		consumers = append(consumers, c)
	}
	return routing.Snapshot{Consumers: consumers, Identity: h.currentResolver}
}

// Publish is the thin bus pipeline: snapshot the consumers, ask the pure Router
// which are eligible, apply the side effects the Router decided (the identity
// marks and the cross-check counter/log), then fan the event out through the
// delivery Broker under a single atomic dedup claim per domain.
//
// Dual-index routing and the fail-closed gate now live in internal/event/routing;
// the fan-out/backpressure/counting in internal/event/delivery. Externally the
// behavior is identical: events reach the same consumers, with the same v2
// fields and the same per-consumer monotonic Seq.
//
// Dedup is per-domain and taken as one atomic claim BEFORE delivery: a legacy
// delivery is keyed by event_id, a refined delivery by the always-present,
// globally-unique subscription_event_id. Only the caller that wins the claim
// delivers, so two concurrent same-key Publishes can never both deliver. The
// claim is gated on the domain having an eligible endpoint, so an event that
// reached NO eligible destination (nothing matched, or everything was
// cross-checked/gated out) claims nothing and stays re-processable; each claim
// expires via the filter's TTL, bounding memory.
func (h *Hub) Publish(raw *event.RawEvent) {
	plan := h.router.Plan(h.snapshot(), raw)

	lg := h.logger.Load()

	// Apply the side effects the Router decided. Routing writes no state; the
	// Hub owns the cross-check counter/log and the per-consumer identity marks.
	for _, d := range plan.Drops {
		switch d.Reason {
		case routing.DropCrossCheck:
			h.crossCheckDropped.Add(1)
			if lg != nil {
				lg.Printf("WARN: refined event dropped: remote_subscription_id=%s cross_check_mismatch=%s",
					raw.RemoteSubscriptionID, d.Detail)
			}
		case routing.DropStaleIdentity:
			// owner != current: NO delivery, NO remote change, marked
			// stale_identity. The core identity-gate invariant.
			if c, ok := d.Ref.(*Conn); ok {
				c.SetStaleIdentity()
			}
		case routing.DropUnresolvedIdentity:
			// Fail CLOSED: never deliver to a user consumer under an unresolved
			// identity; mark it degraded on the identity dimension.
			if c, ok := d.Ref.(*Conn); ok {
				c.SetIdentityDegraded(reasonCurrentIdentityUnresolved)
			}
		}
	}

	if len(plan.Deliveries) == 0 {
		return
	}

	// Build the per-fan-out message template once. The Broker clones it per
	// endpoint, assigning that endpoint's own monotonically-increasing Seq —
	// aliasing a single Seq across subscribers would defeat gap-detection on
	// the consume side. Resolve source time once (not per subscriber): prefer
	// the upstream header create_time (raw.SourceTime) over the local arrival
	// timestamp so consumers see original publisher intent, falling back to
	// Timestamp when SourceTime wasn't populated.
	sourceTime := raw.SourceTime
	if sourceTime == "" && !raw.Timestamp.IsZero() {
		sourceTime = fmt.Sprintf("%d", raw.Timestamp.UnixMilli())
	}
	tmpl := protocol.NewEvent(raw.EventType, raw.EventID, sourceTime, 0, raw.Payload)
	// v2 fields: populated whenever the RAW event is refined, regardless of
	// which domain a given recipient matched in — a legacy consumer receiving a
	// refined-native event via event_type compat delivery gets them too
	// (harmless: omitempty on the wire). Resource -> TargetResource name
	// difference is intentional (see RawEvent/Event doc comments).
	if raw.RemoteSubscriptionID != "" {
		tmpl.RemoteSubscriptionID = raw.RemoteSubscriptionID
		tmpl.TargetResource = raw.Resource
		tmpl.Authority = raw.Authority
		tmpl.SubscriptionEventID = raw.SubscriptionEventID
	}
	build := func(seq uint64) interface{} {
		msg := *tmpl
		msg.Seq = seq
		return &msg
	}

	// Partition the eligible destinations by dedup domain (RouteKind).
	var legacyEps, refinedEps []delivery.Endpoint
	for _, dst := range plan.Deliveries {
		ep := dst.Ref.(delivery.Endpoint)
		if dst.Kind == routing.RouteRefined {
			refinedEps = append(refinedEps, ep)
		} else {
			legacyEps = append(legacyEps, ep)
		}
	}

	// LEGACY domain: dedup by event_id via a single atomic claim BEFORE delivery.
	// Only the claim-winner delivers, so concurrent same-event_id Publishes can't
	// double-deliver; the claim expires via the filter's TTL.
	if len(legacyEps) > 0 && h.legacyDedup.Claim(raw.EventID) {
		h.broker.Deliver(legacyEps, build, lg)
	}

	// REFINED domain: dedup by subscription_event_id — always present and
	// globally unique for a refined event, so it is the whole key (no
	// remote_subscription_id scoping, no fallback), claimed atomically BEFORE
	// delivery exactly like legacy. If it is somehow empty no key can be built:
	// deliver unconditionally (never silently drop) and log a warning.
	if len(refinedEps) > 0 {
		if key := raw.SubscriptionEventID; key == "" {
			h.broker.Deliver(refinedEps, build, lg)
			if lg != nil {
				lg.Printf("WARN: refined event undeduplicated: remote_subscription_id=%s event_id=%s has no subscription_event_id (cannot build a dedup key); delivering without dedup",
					raw.RemoteSubscriptionID, raw.EventID)
			}
		} else if h.refinedDedup.Claim(key) {
			h.broker.Deliver(refinedEps, build, lg)
		}
	}
}

// ConnCount returns the current number of registered subscribers.
func (h *Hub) ConnCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subscribers)
}

// CrossCheckDroppedCount returns how many events the refined cross-check
// (routing's DropCrossCheck) has dropped so far (0 in normal operation — see
// crossCheckDropped's own doc comment).
func (h *Hub) CrossCheckDroppedCount() int64 {
	return h.crossCheckDropped.Load()
}

// EventKeyCount returns total subscribers for the given EventKey, aggregating
// across all SubscriptionIDs. For per-subscription counts use SubCount.
func (h *Hub) EventKeyCount(eventKey string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	count := 0
	for s := range h.subscribers {
		if s.EventKey() == eventKey {
			count++
		}
	}
	return count
}

// SubCount returns the count of subscribers for the given SubscriptionID.
func (h *Hub) SubCount(subscriptionID string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.subCounts[subscriptionID]
}

// RegisteredEventTypes returns the deduplicated union of EventTypes() across
// every currently registered subscriber. Mirrors
// subscribedEventTypes's (bus.go) dedup-via-seen-set shape, but aggregates
// over LIVE registered consumers rather than the static event registry, and
// EventKeyCount's h.mu-guarded read pattern. handleStatusQuery (bus.go)
// calls this to populate StatusResponse.RegisteredEventTypes so a status
// probe can tell whether a given event type currently has any consumer on
// this bus — an old (pre-v2) bus never reported this at all.
func (h *Hub) RegisteredEventTypes() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	seen := make(map[string]struct{})
	var types []string
	for s := range h.subscribers {
		for _, et := range s.EventTypes() {
			if _, ok := seen[et]; ok {
				continue
			}
			seen[et] = struct{}{}
			types = append(types, et)
		}
	}
	return types
}

// BroadcastSourceStatus fans out a source-level status change to every
// subscriber. Best-effort: channel full → drop silently (status isn't
// worth applying back-pressure for). Routes through Subscriber.TrySend
// so the send shares PushDropOldest's sendMu — without this a status
// broadcast could slip into the tiny window between another
// goroutine's drop and its retry push and break the atomicity contract.
func (h *Hub) BroadcastSourceStatus(source, state, detail string) {
	msg := protocol.NewSourceStatus(source, state, detail)
	h.mu.RLock()
	defer h.mu.RUnlock()
	for s := range h.subscribers {
		s.TrySend(msg)
	}
}

// Consumers returns info about all connected consumers.
//
// Refined-status fields: RemoteSubscriptionID/
// OwnerAppID/OwnerUserOpenID are already on the Subscriber interface, so
// they're read directly for every subscriber, refined or not — OwnerAppID in
// particular is populated for EVERY consumer registered against a
// bus (it is just that bus's own AppID), not only refined ones.
// OwnerIdentity/StaleIdentity/per-dimension health are *Conn-only state (not
// part of Subscriber — see Conn's own doc comment on identityMu), so they need
// the same s.(*Conn) type-assert the Publish delivery gate already uses:
// this keeps the Subscriber interface untouched (no mock churn) while still
// surfacing them for every real registration, which is always a *Conn in
// production. A non-*Conn Subscriber (test fakes only) simply leaves those
// three fields at zero.
func (h *Hub) Consumers() []protocol.ConsumerInfo {
	h.mu.RLock()
	defer h.mu.RUnlock()
	result := make([]protocol.ConsumerInfo, 0, len(h.subscribers))
	for s := range h.subscribers {
		remoteSubID := s.RemoteSubscriptionID()
		info := protocol.ConsumerInfo{
			PID:                  s.PID(),
			EventKey:             s.EventKey(),
			SubscriptionID:       s.SubscriptionID(),
			Received:             s.Received(),
			Dropped:              s.DroppedCount(),
			RefinedSubscription:  remoteSubID != "",
			RemoteSubscriptionID: remoteSubID,
			OwnerAppID:           s.OwnerAppID(),
			OwnerUserOpenID:      s.OwnerUserOpenID(),
		}
		if c, ok := s.(*Conn); ok {
			info.OwnerIdentity = c.OwnerIdentity()
			info.StaleIdentity = c.StaleIdentity()
			// Per-dimension health: every currently-unhealthy dimension
			// (identity / subscription / decryption / ...) projected at once,
			// replacing the single degraded_reason slot — so a status display
			// can surface multiple independent facts simultaneously.
			info.Health = healthFactsToProtocol(c.HealthSnapshot())
			// Populate the summary fields from the Conn
			// getters the lifecycle executor's action writes to.
			info.LastLifecycleEvent = c.LastLifecycleEvent()
			info.RemoteState = c.RemoteState()
			// The real per-event action's own state —
			// suspension.code verbatim, the last attempted OAPI action +
			// its classified outcome, and the current recommended recovery
			// step.
			info.SuspensionReason = c.SuspensionReason()
			info.LastAction = c.LastAction()
			info.LastActionError = c.LastActionError()
			info.NextAction = c.NextAction()
			// Decryption observability. decrypt_state +
			// resource_data rollup + last_decrypt_error {class,count,time}.
			// Never a key/ciphertext/plaintext — only classifications.
			info.DecryptState = c.DecryptState()
			info.ResourceData = resourceDataStatus(c.DecryptState())
			if class, count := c.LastDecryptErrorClass(), c.DecryptFailCount(); class != "" || count > 0 {
				info.LastDecryptError = &protocol.DecryptError{
					Class: class,
					Count: count,
					Time:  formatDecryptErrorTime(c.LastDecryptErrorTime()),
				}
			}
		}
		result = append(result, info)
	}
	return result
}

// healthFactsToProtocol projects a Conn's per-dimension health snapshot onto
// the wire ConsumerInfo.Health shape — dimension/reason/severity tokens only,
// never a key/ciphertext/raw error. nil in, nil out (an all-healthy consumer
// carries no health entry, so omitempty keeps the old wire shape).
func healthFactsToProtocol(snap []health.DimensionFact) []protocol.HealthFact {
	if len(snap) == 0 {
		return nil
	}
	out := make([]protocol.HealthFact, 0, len(snap))
	for _, df := range snap {
		out = append(out, protocol.HealthFact{
			Dimension: df.Dimension.String(),
			Reason:    df.Reason,
			Severity:  df.Severity.String(),
		})
	}
	return out
}
