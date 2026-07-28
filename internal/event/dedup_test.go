// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"sync"
	"testing"
	"time"
)

func TestDedupFilter_FirstSeen(t *testing.T) {
	d := NewDedupFilter()
	if d.IsDuplicate("evt-1") {
		t.Error("first occurrence should not be duplicate")
	}
}

func TestDedupFilter_SecondSeen(t *testing.T) {
	d := NewDedupFilter()
	d.IsDuplicate("evt-1")
	if !d.IsDuplicate("evt-1") {
		t.Error("second occurrence within TTL should be duplicate")
	}
}

func TestDedupFilter_TTLExpiry(t *testing.T) {
	d := NewDedupFilterWithSize(defaultRingSize, 10*time.Millisecond)
	d.IsDuplicate("evt-1")
	time.Sleep(20 * time.Millisecond)
	if d.IsDuplicate("evt-1") {
		t.Error("should not be duplicate after TTL expires")
	}
}

func TestDedupFilter_RingBuffer(t *testing.T) {
	d := NewDedupFilterWithSize(5, 10*time.Millisecond)
	for i := 0; i < 5; i++ {
		d.IsDuplicate("evt-" + string(rune('a'+i)))
	}
	for i := 0; i < 5; i++ {
		if !d.IsDuplicate("evt-" + string(rune('a'+i))) {
			t.Errorf("evt-%c should still be duplicate", rune('a'+i))
		}
	}
	time.Sleep(20 * time.Millisecond)
	for i := 5; i < 10; i++ {
		d.IsDuplicate("evt-" + string(rune('a'+i)))
	}
	for i := 0; i < 5; i++ {
		if d.IsDuplicate("evt-" + string(rune('a'+i))) {
			t.Errorf("evt-%c should not be duplicate after ring eviction + TTL expiry", rune('a'+i))
		}
	}
}

func TestDedupFilter_ConcurrentSafe(t *testing.T) {
	d := NewDedupFilter()
	done := make(chan struct{})
	for i := 0; i < 100; i++ {
		go func(id string) {
			d.IsDuplicate(id)
			done <- struct{}{}
		}("evt-" + string(rune(i)))
	}
	for i := 0; i < 100; i++ {
		<-done
	}
}

// Under N concurrent writers, exactly N IsDuplicate calls must observe first-seen.
func TestDedupFilter_ConcurrentFirstSeenExactlyOnce(t *testing.T) {
	const n = 200
	d := NewDedupFilter()

	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = "evt-unique-" + string(rune('A'+i%26)) + string(rune('a'+(i/26)%26)) + string(rune('0'+i%10))
	}

	results := make(chan bool, n)
	for i := 0; i < n; i++ {
		go func(id string) {
			results <- d.IsDuplicate(id)
		}(ids[i])
	}

	firstSeen := 0
	for i := 0; i < n; i++ {
		if !<-results {
			firstSeen++
		}
	}
	if firstSeen != n {
		t.Errorf("first-seen count = %d, want %d", firstSeen, n)
	}

	for _, id := range ids {
		if !d.IsDuplicate(id) {
			t.Errorf("ID %q not flagged as duplicate on second call", id)
			break
		}
	}
}

// Reinserting an ID that already occupies its own ring slot must not delete the fresh seen entry.
func TestDedupFilter_SelfEvictionPreservesFreshEntry(t *testing.T) {
	d := NewDedupFilterWithSize(2, time.Hour)
	d.ring[0] = "X"
	d.pos = 0

	if d.IsDuplicate("X") {
		t.Fatal("first call should not be duplicate (seen empty)")
	}
	if !d.IsDuplicate("X") {
		t.Error("self-slot reinsert wiped seen[X] — duplicate signal lost")
	}
}

// After cleanupExpired, an ID past its TTL must not be reported as duplicate even if still in the ring.
func TestDedupFilter_TTLExpiryAfterCleanupRunRespected(t *testing.T) {
	d := NewDedupFilterWithSize(10, 10*time.Millisecond)
	if d.IsDuplicate("A") {
		t.Fatal("first IsDuplicate(A) should be false")
	}
	time.Sleep(25 * time.Millisecond)
	for i := 0; i < 9; i++ {
		d.IsDuplicate("f" + string(rune('0'+i)))
	}
	if d.IsDuplicate("A") {
		t.Error("A is past TTL — must NOT be reported as duplicate")
	}
}

// RefinedDedupKey priority is frozen: ① remote_subscription_id +
// subscription_event_id; ② fallback to remote_subscription_id + event_id when
// subscription_event_id is absent; ③ neither available → ok=false (caller must
// deliver without dedup and log a warning). remoteSubID=="" always yields
// ok=false: there is no refined domain without a remote_subscription_id.
func TestRefinedDedupKey_Priority(t *testing.T) {
	tests := []struct {
		name        string
		remoteSubID string
		subEventID  string
		eventID     string
		wantOK      bool
	}{
		{"priority1_subEventID_preferred_over_eventID", "R1", "sub-evt-1", "evt-1", true},
		{"priority2_fallback_to_eventID", "R1", "", "evt-1", true},
		{"priority3_neither_present_not_dedupable", "R1", "", "", false},
		{"no_remoteSubID_not_refined_domain", "", "sub-evt-1", "evt-1", false},
		{"no_remoteSubID_and_nothing_else", "", "", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key, ok := RefinedDedupKey(tc.remoteSubID, tc.subEventID, tc.eventID)
			if ok != tc.wantOK {
				t.Fatalf("RefinedDedupKey(%q,%q,%q) ok=%v, want %v", tc.remoteSubID, tc.subEventID, tc.eventID, ok, tc.wantOK)
			}
			if !ok && key != "" {
				t.Errorf("RefinedDedupKey ok=false must return an empty key, got %q", key)
			}
		})
	}
}

// Priority ①: when both subscription_event_id and event_id are present, the
// key must be built from subscription_event_id (the preferred component),
// never a key that would collide with the ② fallback shape.
func TestRefinedDedupKey_PrefersSubscriptionEventIDOverEventID(t *testing.T) {
	keyWithBoth, ok := RefinedDedupKey("R1", "sub-evt-1", "evt-1")
	if !ok {
		t.Fatal("expected ok=true when subscription_event_id is present")
	}
	keyFallbackOnly, ok := RefinedDedupKey("R1", "", "evt-1")
	if !ok {
		t.Fatal("expected ok=true for the fallback (event_id only) case")
	}
	if keyWithBoth == keyFallbackOnly {
		t.Errorf("key built from subscription_event_id (%q) must differ from the event_id-only fallback key (%q) — priority ① must win, not silently degrade to ②", keyWithBoth, keyFallbackOnly)
	}
}

// The key builder must not let a naive concatenation collide across a
// component boundary (e.g. remoteSubID="R1"+subEventID="23" vs
// remoteSubID="R12"+subEventID="3"): a real separator is required.
func TestRefinedDedupKey_NoBoundaryCollision(t *testing.T) {
	keyA, okA := RefinedDedupKey("R1", "23", "")
	keyB, okB := RefinedDedupKey("R12", "3", "")
	if !okA || !okB {
		t.Fatal("both cases should be dedupable (ok=true)")
	}
	if keyA == keyB {
		t.Errorf("boundary collision: RefinedDedupKey(%q,%q,_) == RefinedDedupKey(%q,%q,_) == %q; key builder needs an unambiguous separator", "R1", "23", "R12", "3", keyA)
	}
}

// Same (remoteSubID, subEventID, eventID) inputs must always build the same
// key: RefinedDedupKey is a pure function, its output feeds DedupFilter.IsDuplicate.
func TestRefinedDedupKey_Deterministic(t *testing.T) {
	k1, ok1 := RefinedDedupKey("R1", "sub-1", "evt-1")
	k2, ok2 := RefinedDedupKey("R1", "sub-1", "evt-1")
	if !ok1 || !ok2 || k1 != k2 {
		t.Errorf("RefinedDedupKey must be deterministic: got (%q,%v) and (%q,%v)", k1, ok1, k2, ok2)
	}
}

func TestDedupFilter_ConcurrentRingEviction(t *testing.T) {
	const ringSize = 16
	const writers = 8
	const perWriter = 40
	d := NewDedupFilterWithSize(ringSize, 5*time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				d.IsDuplicate("evt-w" + string(rune('0'+w)) + "-" + string(rune('0'+i%10)) + string(rune('a'+i/10)))
			}
		}(w)
	}
	wg.Wait()

	time.Sleep(10 * time.Millisecond)
	for i := 0; i < ringSize*4; i++ {
		d.IsDuplicate("evt-fill-" + string(rune('0'+i%10)) + string(rune('a'+i/10)))
	}
	if d.IsDuplicate("evt-w0-0a") {
		t.Error("evicted ID should not be reported as duplicate")
	}
}

// Claim is the atomic check-and-mark: the first caller wins (true), a second
// within the TTL loses (false).
func TestDedupFilter_Claim_FirstWinsSecondLoses(t *testing.T) {
	d := NewDedupFilter()
	if !d.Claim("k") {
		t.Error("first Claim must win (key not yet seen)")
	}
	if d.Claim("k") {
		t.Error("second Claim within TTL must lose (key already claimed)")
	}
}

// IsDuplicate is the exact negative-sense complement of Claim.
func TestDedupFilter_Claim_IsComplementOfIsDuplicate(t *testing.T) {
	d := NewDedupFilter()
	if d.Claim("a") == d.IsDuplicate("b") {
		// Claim("a")=true (first), IsDuplicate("b")=false (first) — must differ.
		t.Error("first Claim and first IsDuplicate on fresh keys must have opposite sense")
	}
	// Same key: after a winning Claim, IsDuplicate must report true.
	d.Claim("c")
	if !d.IsDuplicate("c") {
		t.Error("IsDuplicate must report a previously-claimed key as duplicate")
	}
}

// A claim expires via the TTL, so the key becomes re-claimable — this is what
// bounds the seen-set in time (stale ids do not linger forever).
func TestDedupFilter_Claim_TTLExpiry(t *testing.T) {
	d := NewDedupFilterWithSize(defaultRingSize, 10*time.Millisecond)
	if !d.Claim("k") {
		t.Fatal("first Claim must win")
	}
	if d.Claim("k") {
		t.Fatal("second Claim within TTL must lose")
	}
	time.Sleep(20 * time.Millisecond)
	if !d.Claim("k") {
		t.Error("Claim after TTL expiry must win again — a claim must not be retained past its TTL")
	}
}

// The ring bounds the seen-set in size: an id pushed out of the ring (and past
// its TTL) becomes claimable again, so memory stays bounded under an unbounded
// stream of distinct keys.
func TestDedupFilter_Claim_BoundedByRingEviction(t *testing.T) {
	d := NewDedupFilterWithSize(4, 10*time.Millisecond)
	if !d.Claim("first") {
		t.Fatal("first Claim must win")
	}
	time.Sleep(20 * time.Millisecond)
	for i := 0; i < 8; i++ {
		d.Claim("filler-" + string(rune('a'+i)))
	}
	if !d.Claim("first") {
		t.Error("an evicted + expired id must be claimable again — claim memory must stay bounded")
	}
}

// Under N goroutines racing the SAME key, exactly one Claim wins. This is the
// atomicity the delivery gate relies on to never double-deliver.
func TestDedupFilter_Claim_ConcurrentExactlyOneWinner(t *testing.T) {
	const n = 256
	d := NewDedupFilter()

	results := make(chan bool, n)
	for i := 0; i < n; i++ {
		go func() { results <- d.Claim("same-key") }()
	}
	wins := 0
	for i := 0; i < n; i++ {
		if <-results {
			wins++
		}
	}
	if wins != 1 {
		t.Errorf("exactly one Claim must win among %d concurrent claimers, got %d", n, wins)
	}
}
