// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"sync"
	"time"
)

const (
	defaultDedupTTL = 5 * time.Minute
	defaultRingSize = 10000
)

// DedupFilter: seen map is sole authority; ring only bounds map size via overflow eviction.
type DedupFilter struct {
	seen map[string]time.Time
	ring []string
	pos  int
	ttl  time.Duration
	mu   sync.Mutex
}

func NewDedupFilter() *DedupFilter {
	return NewDedupFilterWithSize(defaultRingSize, defaultDedupTTL)
}

func NewDedupFilterWithSize(ringSize int, ttl time.Duration) *DedupFilter {
	return &DedupFilter{
		seen: make(map[string]time.Time),
		ring: make([]string, ringSize),
		ttl:  ttl,
	}
}

// Claim atomically checks whether key was already seen within the TTL and, if
// not, marks it seen — the check and the mark happen under ONE lock, in one
// step. It returns true iff THIS caller is the first to claim key (it was NOT a
// live duplicate), so among any set of callers racing the same key exactly one
// wins. This is the single dedup decision point a delivery gate needs: gate the
// side effect on the claim and only the winner proceeds, which closes the
// check-then-commit TOCTOU where two concurrent same-key callers could both pass
// a separate read-only check before either committed and thus both proceed. An
// entry past its TTL is evicted and freshly re-claimable; the ring bounds the
// seen-set so memory stays bounded even under an unbounded stream of keys.
func (d *DedupFilter) Claim(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()

	if ts, ok := d.seen[key]; ok {
		if now.Sub(ts) < d.ttl {
			return false
		}
		delete(d.seen, key)
	}

	d.seen[key] = now

	if old := d.ring[d.pos]; old != "" && old != key {
		delete(d.seen, old)
	}
	d.ring[d.pos] = key
	d.pos = (d.pos + 1) % len(d.ring)

	if d.pos%1000 == 0 {
		d.cleanupExpired(now)
	}

	return true
}

// IsDuplicate reports whether eventID was already seen within the TTL, marking
// it seen as a side effect. It is the negative-sense complement of Claim —
// IsDuplicate(k) == !Claim(k) — kept for callers and tests that read more
// naturally as "is this a duplicate?" than "did I win the claim?".
func (d *DedupFilter) IsDuplicate(eventID string) bool {
	return !d.Claim(eventID)
}

// Seen reports whether key was recorded within the TTL, WITHOUT recording it —
// a read-only check. Pair with Record to implement "check duplicate BEFORE
// accepting, commit the key only AFTER a successful accept", so an event that
// is checked but then dropped (e.g. a full bounded queue) is NOT marked seen
// and a later retry of the same key can still be processed. An expired entry it
// encounters is lazily evicted (like IsDuplicate) and reported as not-seen.
func (d *DedupFilter) Seen(eventID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	ts, ok := d.seen[eventID]
	if !ok {
		return false
	}
	if time.Since(ts) < d.ttl {
		return true
	}
	delete(d.seen, eventID)
	return false
}

// Record marks key as seen now (idempotent — a re-record just refreshes the
// timestamp). It performs the same ring/overflow bookkeeping as Claim's mark
// half, so Seen()+Record() together are equivalent to a single Claim() except
// the commit is deferred to the caller's discretion. This split is for a caller
// that already holds its OWN lock across the Seen/Record pair AND must be able
// to abandon the check without committing (e.g. drop the key on a full queue so
// a retry stays processable); a plain delivery gate wants Claim instead.
func (d *DedupFilter) Record(eventID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	if _, ok := d.seen[eventID]; ok {
		d.seen[eventID] = now
		return
	}
	d.seen[eventID] = now
	if old := d.ring[d.pos]; old != "" && old != eventID {
		delete(d.seen, old)
	}
	d.ring[d.pos] = eventID
	d.pos = (d.pos + 1) % len(d.ring)
	if d.pos%1000 == 0 {
		d.cleanupExpired(now)
	}
}

func (d *DedupFilter) cleanupExpired(now time.Time) {
	for id, ts := range d.seen {
		if now.Sub(ts) >= d.ttl {
			delete(d.seen, id)
		}
	}
}

// RefinedDedupKey builds the dedup key for the refined-subscription domain,
// with a frozen fallback priority:
//
//	① remote_subscription_id + subscription_event_id (preferred: unique within
//	   the remote Subscription's own event enumeration).
//	② remote_subscription_id + event_id (fallback when the push envelope
//	   didn't carry a subscription_event_id).
//	③ neither subEventID nor eventID present: no key can be built. ok=false
//	   tells the caller dedup is impossible here — it MUST deliver the event
//	   unconditionally (never silently drop) and should log a warning.
//
// remoteSubID=="" always returns ok=false: there is no refined domain
// without a remote_subscription_id (that event isn't a refined-subscription
// event in the first place; it belongs only to the legacy event_id domain).
//
// The key mixes a NUL byte between components so a boundary shift (e.g.
// remoteSubID="R1"+subEventID="23" vs remoteSubID="R12"+subEventID="3")
// can't collide via naive string concatenation. The returned key is an
// opaque string handed straight to DedupFilter.IsDuplicate — this function
// does no locking or TTL/ring bookkeeping of its own.
func RefinedDedupKey(remoteSubID, subEventID, eventID string) (key string, ok bool) {
	if remoteSubID == "" {
		return "", false
	}
	if subEventID != "" {
		return remoteSubID + "\x00" + subEventID, true
	}
	if eventID != "" {
		return remoteSubID + "\x00" + eventID, true
	}
	return "", false
}
