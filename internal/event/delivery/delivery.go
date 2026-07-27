// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

// Package delivery owns the event bus's fan-out mechanics: pushing a routed
// event to each destination's bounded per-consumer queue with drop-oldest
// backpressure, counting received and dropped events, and logging backpressure.
// It makes NO identity, remote, or routing decisions — routing (internal/event/
// routing) has already chosen the destinations. Delivery does not even see the
// event: the host hands it a per-seq message factory, so the Broker is pure
// transport and is trivially testable in isolation with a fake Endpoint.
package delivery

import "log"

// Endpoint is the minimal per-destination contract delivery needs: a monotonic
// per-endpoint sequence, an atomic drop-oldest enqueue, and the received/dropped
// bookkeeping. The bus Subscriber (and every test fake) already satisfies this.
type Endpoint interface {
	// NextSeq returns this endpoint's next monotonic seq (assigned per fan-out
	// so each consumer sees a gap-free sequence of its own).
	NextSeq() uint64
	// PushDropOldest enqueues msg atomically; on a full queue it evicts one
	// oldest entry and retries. It returns (enqueued, dropped): enqueued=true
	// means msg is now queued (accepted), dropped=true means an older entry was
	// evicted to make room.
	PushDropOldest(msg interface{}) (enqueued, dropped bool)
	// IncrementReceived counts one accepted (enqueued) event.
	IncrementReceived()
	// IncrementDropped counts one backpressure eviction.
	IncrementDropped()
	// DroppedCount is the running eviction total (for the backpressure log).
	DroppedCount() int64
	// PID and EventKey identify the endpoint in the backpressure log.
	PID() int
	EventKey() string
}

// Broker fans a routed event out to endpoints. It is stateless; the zero value
// is ready to use. It is a struct rather than a bare function so the transport
// policy can be extended (e.g. metrics, a fan-out cap) without changing call
// sites.
type Broker struct{}

// Deliver fans one event out to endpoints and returns how many ACCEPTED it
// (enqueued == true). build produces the per-endpoint message given that
// endpoint's own NextSeq(): the host owns the message shape and the seq
// assignment, so the Broker never inspects event content — it only enqueues and
// counts. A backpressure eviction (dropped) is counted and logged but still
// counts as accepted, because the NEW event is queued (an older one was
// evicted). logger may be nil (no backpressure logging).
//
// The accepted count is what lets the host commit dedup only AFTER an eligible
// destination actually queued the event: 0 accepted means nothing was queued,
// so the event must stay re-processable.
func (Broker) Deliver(endpoints []Endpoint, build func(seq uint64) interface{}, logger *log.Logger) (accepted int) {
	for _, ep := range endpoints {
		msg := build(ep.NextSeq())
		enqueued, dropped := ep.PushDropOldest(msg)
		if dropped {
			ep.IncrementDropped()
			if logger != nil {
				logger.Printf("WARN: backpressure on conn pid=%d event_key=%s dropped_total=%d",
					ep.PID(), ep.EventKey(), ep.DroppedCount())
			}
		}
		if enqueued {
			ep.IncrementReceived()
			accepted++
		}
	}
	return accepted
}
