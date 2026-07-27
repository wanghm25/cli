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
	"github.com/larksuite/cli/internal/event/bus/lifecycle"
	"github.com/larksuite/cli/internal/event/model"
	"github.com/larksuite/cli/internal/event/protocol"
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
	// UserOpenID arrive "" until a refined client populates them); Hub.Publish's
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

	// legacyDedup and refinedDedup are separate dedup domains:
	// dedup runs AFTER
	// routing-domain identification, never as one global gate at the source
	// entry). legacyDedup keys by event_id alone (today's behavior, applied
	// to every event regardless of remote_subscription_id — legacy consumers
	// receive refined events too). refinedDedup keys by RefinedDedupKey
	// (remote_subscription_id-scoped), so the SAME event_id delivered under
	// two different remote_subscription_id contexts dedups independently in
	// each, rather than the second delivery being swallowed globally. Each
	// *event.DedupFilter is self-locking; do not add locking around them.
	legacyDedup  *event.DedupFilter
	refinedDedup *event.DedupFilter

	// currentResolver is the identity gate's fresh-current-identity resolver,
	// wired by Bus.SetIdentityProviders via SetCurrentResolver.
	// nil (the zero value — every non-gated NewHub()/NewBus() caller)
	// means NO identity gating: every consumer is delivered according to the
	// legacy ungated behavior. Guarded by mu (read together with the
	// subscribers snapshot at the top of Publish) rather than a separate
	// lock/atomic — it changes at most once in practice (bus construction,
	// before Run starts accepting events).
	currentResolver func() (session.CurrentIdentity, error)

	// crossCheckDropped counts events dropped by Publish's refined
	// cross-check (refinedCrossCheckMismatch): a remote_subscription_id match
	// that, on closer inspection, disagreed with the matched consumer's own
	// stored intent on event_type/target_resource/authority. Distinct from
	// per-consumer DroppedCount (backpressure evictions) — this is a
	// routing-integrity signal, expected to stay at 0 in normal operation.
	crossCheckDropped atomic.Int64
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
// resolver into Publish's delivery gate. nil disables gating
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
// DegradedReason) that intentionally isn't part of the Subscriber contract
// every mock must satisfy — mirrors how findSubscriberByPID
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

// publishMatch pairs a matched Subscriber with which routing domain matched it,
// so the dedup gate (computed once per domain per Publish, below) is applied
// per-recipient without re-deriving refined-vs-legacy from scratch.
type publishMatch struct {
	sub     Subscriber
	refined bool
}

// Publish fans out a RawEvent to all matching subscribers (non-blocking).
//
// Dual-index routing: a refined consumer (Subscriber.RemoteSubscriptionID()
// != "") is matched ONLY by remote_subscription_id equality — never by
// event_type alone, and never when raw has no remote_subscription_id (never
// guess which resource an unqualified event belongs to). A legacy
// consumer (RemoteSubscriptionID() == "") keeps today's event_type matching
// unconditionally — including for refined-native events, which legacy
// consumers still receive via event_type compat delivery.
//
// A matched refined consumer additionally passes refinedCrossCheckMismatch
// before delivery: fail-closed defense-in-depth on top of the
// remote_subscription_id match above, never a replacement for it — see that
// function's own doc comment for exactly what it checks and why an absent
// target_resource/authority is itself treated as a mismatch (drop), not
// skipped.
//
// A fresh *protocol.Event is allocated per subscriber so each consumer sees
// its own monotonically-increasing Seq (assigned via Conn.NextSeq) — sharing
// a single msg struct across subscribers would alias Seq and defeat the
// gap-detection at the consume side. The extra allocation per fan-out is
// cheap compared to the socket write that follows.
func (h *Hub) Publish(raw *event.RawEvent) {
	h.mu.RLock()
	matches := make([]publishMatch, 0, len(h.subscribers))
	for s := range h.subscribers {
		if remoteSubID := s.RemoteSubscriptionID(); remoteSubID != "" {
			if raw.RemoteSubscriptionID != "" && remoteSubID == raw.RemoteSubscriptionID {
				matches = append(matches, publishMatch{sub: s, refined: true})
			}
			continue
		}
		for _, et := range s.EventTypes() {
			if et == raw.EventType {
				matches = append(matches, publishMatch{sub: s, refined: false})
				break
			}
		}
	}
	// Snapshotted alongside subscribers under the same RLock:
	// nil means no identity gating configured (every non-gated caller).
	currentResolver := h.currentResolver
	h.mu.RUnlock()

	// Resolve source time once per Publish (not per subscriber) — same value
	// across the fan-out. Prefer the upstream header create_time
	// (raw.SourceTime) over the local arrival timestamp so consumers see
	// original publisher intent; fall back to Timestamp when SourceTime
	// wasn't populated (e.g. test-only sources, pre-4.4 RawEvent producers).
	sourceTime := raw.SourceTime
	if sourceTime == "" && !raw.Timestamp.IsZero() {
		sourceTime = fmt.Sprintf("%d", raw.Timestamp.UnixMilli())
	}

	// Split dedup: runs AFTER routing-domain identification, not
	// as one global event_id gate at the source entry — otherwise the same
	// physical event delivered under two remote_subscription_id contexts
	// would have its second delivery swallowed before Hub.Publish even got to
	// route it to the second refined consumer. Each IsDuplicate call is
	// self-locking and has side effects (marks the key seen); called exactly
	// once per domain per Publish, regardless of how many subscribers match.
	//
	// legacyDup is evaluated unconditionally: legacy consumers receive
	// refined events too (routed by event_type above), so the legacy domain
	// must dedup by event_id across BOTH kinds of raw events.
	legacyDup := h.legacyDedup.IsDuplicate(raw.EventID)

	// refinedDrop only applies when raw actually carries a
	// remote_subscription_id (that's the domain gate — there is no refined
	// domain otherwise). Priority ①②③ per RefinedDedupKey; ③ (ok=false) means
	// no key could be built — deliver unconditionally (never silently drop)
	// and log a warning instead.
	var refinedDrop bool
	if raw.RemoteSubscriptionID != "" {
		if key, ok := event.RefinedDedupKey(raw.RemoteSubscriptionID, raw.SubscriptionEventID, raw.EventID); ok {
			refinedDrop = h.refinedDedup.IsDuplicate(key)
		} else if lg := h.logger.Load(); lg != nil {
			lg.Printf("WARN: refined event undeduplicated: remote_subscription_id=%s event_id=%s has no subscription_event_id and no event_id (cannot build a dedup key); delivering without dedup",
				raw.RemoteSubscriptionID, raw.EventID)
		}
	}

	// Identity gate state, resolved AT MOST ONCE per Publish
	// call — lazily, only when a matched subscriber is actually a USER
	// consumer (OwnerUserOpenID() != ""); bot/legacy consumers never pay
	// this cost and are never gated, regardless of currentResolver.
	var (
		identityResolved bool
		identityCur      session.CurrentIdentity
		identityErr      error
	)

	for _, m := range matches {
		if m.refined {
			if refinedDrop {
				continue
			}
		} else if legacyDup {
			continue
		}
		s := m.sub

		if m.refined {
			if reason := refinedCrossCheckMismatch(raw, s); reason != "" {
				h.crossCheckDropped.Add(1)
				if lg := h.logger.Load(); lg != nil {
					lg.Printf("WARN: refined event dropped: remote_subscription_id=%s cross_check_mismatch=%s",
						raw.RemoteSubscriptionID, reason)
				}
				continue
			}
		}

		if currentResolver != nil && s.OwnerUserOpenID() != "" {
			if !identityResolved {
				identityCur, identityErr = currentResolver()
				identityResolved = true
			}
			// The shared owner/current gate decides the fail-closed policy; this
			// site applies the delivery-path side effects.
			owner := model.OwnerRef{AppID: s.OwnerAppID(), UserOpenID: s.OwnerUserOpenID()}
			switch session.Gate(owner, identityCur, identityErr) {
			case session.AdmitUnresolved:
				// Fail CLOSED: never deliver to a user consumer under an
				// unresolved identity. A single unresolved lookup degrades
				// every user consumer matched in THIS Publish call; bot
				// consumers never reach this branch at all.
				if c, ok := s.(*Conn); ok {
					c.SetDegraded(reasonCurrentIdentityUnresolved)
				}
				continue
			case session.AdmitStale:
				// owner != current: NO delivery, NO remote change, marked
				// stale_identity. This is the core identity-gate invariant.
				if c, ok := s.(*Conn); ok {
					c.SetStaleIdentity()
				}
				continue
			}
		}

		msg := protocol.NewEvent(
			raw.EventType,
			raw.EventID,
			sourceTime,
			s.NextSeq(),
			raw.Payload,
		)
		// v2 fields: populated whenever the RAW event is refined,
		// regardless of which domain THIS recipient matched in — a legacy
		// consumer receiving a refined-native event via event_type compat
		// delivery gets them too (harmless: omitempty on the wire, and this
		// recipient just ignores fields it doesn't look at). Resource ->
		// TargetResource name difference is intentional (see RawEvent/Event
		// doc comments) — do not rename either to match the other.
		if raw.RemoteSubscriptionID != "" {
			msg.RemoteSubscriptionID = raw.RemoteSubscriptionID
			msg.TargetResource = raw.Resource
			msg.Authority = raw.Authority
			msg.SubscriptionEventID = raw.SubscriptionEventID
		}

		enqueued, dropped := s.PushDropOldest(msg)
		if dropped {
			s.IncrementDropped()
			if lg := h.logger.Load(); lg != nil {
				lg.Printf("WARN: backpressure on conn pid=%d event_key=%s dropped_total=%d",
					s.PID(), s.EventKey(), s.DroppedCount())
			}
		}
		if enqueued {
			s.IncrementReceived()
		}
	}
}

// refinedCrossCheckMismatch implements the fail-closed defense-in-depth
// cross-check for a refined consumer already matched by remote_subscription_id
// equality: that match alone routed raw to s, but a legit refined event ALWAYS
// carries its full context (the platform envelope always includes
// target_resource and authority), so this treats a MISSING dimension as an
// anomaly to drop, not a reason to deliver blind.
// Returns "" (no mismatch — deliver) or a short fixed classification naming
// what went wrong, so a drop's log line and counter stay consistent. The
// tokens are distinct for "absent context" versus "present-but-disagrees":
// "event_type" / "target_resource" / "authority" (mismatch) and
// "target_resource_missing" / "authority_missing" (absent context).
//
// event_type is always checkable: raw.EventType and s.EventTypes() are both
// always populated (refined or not), and a real remote Subscription is
// permanently bound to one event_type at Create time — so a mismatch here
// under an already-matched remote_subscription_id would be a genuine
// anomaly, not a normal/expected state.
//
// target_resource and authority must be present on a refined-native event;
// an empty value drops (*_missing). When present, target_resource is compared
// against the consumer's own stored intent via event.TargetResourceEqual
// (normalized, so escaping/selector-ordering never false-drops) — and only
// when s is a *Conn exposing that intent (a bare Subscriber — a test fake,
// never a real registration — has no target_resource concept and is only held
// to the presence check). authority reuses lifecycle.AuthorityMatchesOwner,
// the SAME "user:<open_id>"/"app" compare the updated_v1 lifecycle
// compatibility check already establishes, so the two never drift out of sync.
func refinedCrossCheckMismatch(raw *event.RawEvent, s Subscriber) string {
	matched := false
	for _, et := range s.EventTypes() {
		if et == raw.EventType {
			matched = true
			break
		}
	}
	if !matched {
		return "event_type"
	}

	// target_resource: a legit refined event always carries its own
	// target_resource (the platform envelope includes it), so an absent one
	// under an already-matched remote_subscription_id is an anomaly — fail
	// closed rather than deliver blind. When present, hold a *Conn to its own
	// resolved listening intent, comparing NORMALIZED so escaping/selector-
	// ordering differences don't false-drop (a bare Subscriber — a test fake —
	// carries no target_resource intent and is never held to one).
	if raw.Resource == "" {
		return "target_resource_missing"
	}
	if c, ok := s.(*Conn); ok && c.TargetResource() != "" && !event.TargetResourceEqual(raw.Resource, c.TargetResource()) {
		return "target_resource"
	}

	// authority: likewise always present on a legit refined event, so an absent
	// one is an anomaly (fail closed). When present it must name THIS consumer's
	// own owner — "user:<open_id>" for a user, "app" for a bot.
	if raw.Authority == "" {
		return "authority_missing"
	}
	if !lifecycle.AuthorityMatchesOwner(raw.Authority, s.OwnerUserOpenID()) {
		return "authority"
	}

	return ""
}

// ConnCount returns the current number of registered subscribers.
func (h *Hub) ConnCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subscribers)
}

// CrossCheckDroppedCount returns how many events Publish's refined
// cross-check (refinedCrossCheckMismatch) has dropped so far (0 in normal
// operation — see crossCheckDropped's own doc comment).
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
// OwnerIdentity/StaleIdentity/DegradedReason are *Conn-only state (not part
// of Subscriber — see Conn's own doc comment on identityMu), so they need
// the same s.(*Conn) type-assert the Publish delivery gate already uses
// (hub.go's Publish, "if c, ok := s.(*Conn); ok"): this keeps the Subscriber
// interface untouched (no mock churn) while still surfacing them for every
// real registration, which is always a *Conn in production. A non-*Conn
// Subscriber (test fakes only) simply leaves those three fields at zero.
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
			info.DegradedReason = c.DegradedReason()
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
