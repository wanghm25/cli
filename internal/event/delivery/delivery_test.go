// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package delivery

import (
	"sync"
	"sync/atomic"
	"testing"
)

// fakeEndpoint is a self-contained Endpoint for exercising the Broker in
// isolation: a bounded channel with the same atomic drop-oldest semantics as
// the real Conn, plus received/dropped counters and a per-endpoint seq. A
// zero-capacity fakeEndpoint models an endpoint that can never enqueue.
type fakeEndpoint struct {
	pid       int
	eventKey  string
	sendCh    chan interface{}
	seq       atomic.Uint64
	received  atomic.Int64
	dropped   atomic.Int64
	sawSeqs   []uint64
	blackhole bool // PushDropOldest always fails (enqueued=false)
	sendMu    sync.Mutex
}

func newFakeEndpoint(pid int, capacity int) *fakeEndpoint {
	return &fakeEndpoint{pid: pid, eventKey: "k", sendCh: make(chan interface{}, capacity)}
}

func (e *fakeEndpoint) NextSeq() uint64 {
	s := e.seq.Add(1)
	e.sawSeqs = append(e.sawSeqs, s)
	return s
}
func (e *fakeEndpoint) IncrementReceived()  { e.received.Add(1) }
func (e *fakeEndpoint) IncrementDropped()   { e.dropped.Add(1) }
func (e *fakeEndpoint) DroppedCount() int64 { return e.dropped.Load() }
func (e *fakeEndpoint) PID() int            { return e.pid }
func (e *fakeEndpoint) EventKey() string    { return e.eventKey }

func (e *fakeEndpoint) PushDropOldest(msg interface{}) (enqueued, dropped bool) {
	if e.blackhole {
		return false, false
	}
	e.sendMu.Lock()
	defer e.sendMu.Unlock()
	select {
	case e.sendCh <- msg:
		return true, false
	default:
	}
	select {
	case <-e.sendCh:
		dropped = true
	default:
	}
	select {
	case e.sendCh <- msg:
		return true, dropped
	default:
		return false, dropped
	}
}

func trivialBuild(seq uint64) interface{} { return seq }

// Fan-out: every endpoint receives the event, each is counted received, and the
// accepted count equals the number of endpoints that enqueued.
func TestBroker_Deliver_FansOutAndCountsAccepted(t *testing.T) {
	a := newFakeEndpoint(1, 8)
	b := newFakeEndpoint(2, 8)

	accepted := Broker{}.Deliver([]Endpoint{a, b}, trivialBuild, nil)
	if accepted != 2 {
		t.Errorf("accepted = %d, want 2", accepted)
	}
	if a.received.Load() != 1 || b.received.Load() != 1 {
		t.Errorf("received: a=%d b=%d, want 1 each", a.received.Load(), b.received.Load())
	}
	if len(a.sendCh) != 1 || len(b.sendCh) != 1 {
		t.Errorf("queued: a=%d b=%d, want 1 each", len(a.sendCh), len(b.sendCh))
	}
}

// Each endpoint is assigned its OWN seq via its own NextSeq — never aliased.
func TestBroker_Deliver_AssignsPerEndpointSeq(t *testing.T) {
	a := newFakeEndpoint(1, 4)
	b := newFakeEndpoint(2, 4)
	// Pre-advance b's counter so its seq differs from a's, proving the Broker
	// uses each endpoint's own NextSeq rather than a shared value.
	b.seq.Store(41)

	var gotA, gotB uint64
	Broker{}.Deliver([]Endpoint{a}, func(seq uint64) interface{} { gotA = seq; return seq }, nil)
	Broker{}.Deliver([]Endpoint{b}, func(seq uint64) interface{} { gotB = seq; return seq }, nil)

	if gotA != 1 {
		t.Errorf("endpoint a seq = %d, want 1", gotA)
	}
	if gotB != 42 {
		t.Errorf("endpoint b seq = %d, want 42 (its own counter, not aliased)", gotB)
	}
}

// Backpressure: a full queue evicts the oldest, counts the drop, still accepts
// the new event (enqueued), and logs (logger non-nil path exercised via nil-safe
// call — here we assert the drop bookkeeping).
func TestBroker_Deliver_BackpressureEvictsCountsAndStillAccepts(t *testing.T) {
	e := newFakeEndpoint(7, 1)
	// Pre-fill the single slot so the next delivery must evict.
	e.sendCh <- "stale"

	accepted := Broker{}.Deliver([]Endpoint{e}, trivialBuild, nil)
	if accepted != 1 {
		t.Errorf("accepted = %d, want 1 (drop-oldest still enqueues the new event)", accepted)
	}
	if e.dropped.Load() != 1 {
		t.Errorf("dropped count = %d, want 1", e.dropped.Load())
	}
	if e.received.Load() != 1 {
		t.Errorf("received count = %d, want 1 (an evicting enqueue is still accepted)", e.received.Load())
	}
	// The queued item must be the NEW event (seq 1), not the evicted "stale".
	got := <-e.sendCh
	if got != uint64(1) {
		t.Errorf("queued item = %v, want the new event (seq 1)", got)
	}
}

// An endpoint that can never enqueue contributes 0 accepted and 0 received, so a
// no-accept fan-out reports accepted==0 (the host then commits no dedup).
func TestBroker_Deliver_BlackholeEndpointNotAccepted(t *testing.T) {
	e := newFakeEndpoint(9, 0)
	e.blackhole = true

	accepted := Broker{}.Deliver([]Endpoint{e}, trivialBuild, nil)
	if accepted != 0 {
		t.Errorf("accepted = %d, want 0 for an endpoint that never enqueues", accepted)
	}
	if e.received.Load() != 0 {
		t.Errorf("received = %d, want 0", e.received.Load())
	}
	if e.dropped.Load() != 0 {
		t.Errorf("dropped = %d, want 0 (no eviction happened)", e.dropped.Load())
	}
}

// Mixed fan-out: an accepting endpoint and a blackhole endpoint — accepted
// reflects only the one that enqueued.
func TestBroker_Deliver_MixedAcceptance(t *testing.T) {
	ok := newFakeEndpoint(1, 4)
	bad := newFakeEndpoint(2, 0)
	bad.blackhole = true

	accepted := Broker{}.Deliver([]Endpoint{ok, bad}, trivialBuild, nil)
	if accepted != 1 {
		t.Errorf("accepted = %d, want 1 (only the accepting endpoint)", accepted)
	}
}

// Empty endpoint set: nothing to do, accepted 0.
func TestBroker_Deliver_NoEndpoints(t *testing.T) {
	if accepted := (Broker{}).Deliver(nil, trivialBuild, nil); accepted != 0 {
		t.Errorf("accepted = %d, want 0 for no endpoints", accepted)
	}
}
