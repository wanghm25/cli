// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/protocol"
)

func TestHubDroppedCountIncrements(t *testing.T) {
	h := NewHub()
	server, client := testNetPipe(t)
	defer server.Close()
	defer client.Close()
	c := NewConn(server, nil, "k", []string{"t"}, 1, "")
	c.sendCh = make(chan interface{}, 1)
	h.RegisterAndIsFirst(c)

	// Distinct EventIDs: Hub.Publish now dedups by event_id for the legacy
	// domain (spec §4.3 relocated dedup INSIDE Publish, previously an
	// upstream bus.go gate this direct-Publish test never went through).
	// This test's subject is drop-oldest backpressure, not dedup, so each
	// call must look like a genuinely distinct event.
	h.Publish(&event.RawEvent{EventID: "evt-1", EventType: "t"})
	h.Publish(&event.RawEvent{EventID: "evt-2", EventType: "t"})
	h.Publish(&event.RawEvent{EventID: "evt-3", EventType: "t"})

	if got := c.DroppedCount(); got != 2 {
		t.Errorf("expected 2 drops, got %d", got)
	}
}

func TestPublishAssignsIncrementalSeq(t *testing.T) {
	h := NewHub()
	server, client := testNetPipe(t)
	defer server.Close()
	defer client.Close()
	c := NewConn(server, nil, "k", []string{"t"}, 1, "")
	c.sendCh = make(chan interface{}, 10)
	h.RegisterAndIsFirst(c)

	// Distinct EventIDs per call: Hub.Publish now dedups by event_id for the
	// legacy domain (spec §4.3), so 5 identical/empty event_ids would
	// collapse to a single delivery. This test's subject is seq assignment,
	// not dedup.
	for i := 0; i < 5; i++ {
		h.Publish(&event.RawEvent{EventID: fmt.Sprintf("evt-%d", i), EventType: "t"})
	}

	for i := uint64(1); i <= 5; i++ {
		msg := <-c.SendCh()
		ev, ok := msg.(*protocol.Event)
		if !ok {
			t.Fatalf("iter %d: expected *protocol.Event, got %T", i, msg)
		}
		if ev.Seq != i {
			t.Errorf("iter %d: expected seq %d, got %d", i, i, ev.Seq)
		}
	}
}

func TestPublishPopulatesEventIDAndSourceTime(t *testing.T) {
	h := NewHub()
	server, client := testNetPipe(t)
	defer server.Close()
	defer client.Close()
	c := NewConn(server, nil, "k", []string{"t"}, 1, "")
	c.sendCh = make(chan interface{}, 1)
	h.RegisterAndIsFirst(c)

	const eid = "test-event-id-123"
	h.Publish(&event.RawEvent{
		EventID:   eid,
		EventType: "t",
		Timestamp: time.UnixMilli(1234567890123),
	})

	msg := <-c.SendCh()
	ev := msg.(*protocol.Event)
	if ev.EventID != eid {
		t.Errorf("expected EventID %q, got %q", eid, ev.EventID)
	}
	if ev.SourceTime != "1234567890123" {
		t.Errorf("expected SourceTime \"1234567890123\", got %q", ev.SourceTime)
	}
}

// Explicit SourceTime (upstream header.create_time) must win over local Timestamp.
func TestPublishSourceTimeTakesPrecedence(t *testing.T) {
	h := NewHub()
	server, client := testNetPipe(t)
	defer server.Close()
	defer client.Close()
	c := NewConn(server, nil, "k", []string{"t"}, 1, "")
	c.sendCh = make(chan interface{}, 1)
	h.RegisterAndIsFirst(c)

	const upstreamTs = "1700000000000"
	h.Publish(&event.RawEvent{
		EventID:    "evt-1",
		EventType:  "t",
		SourceTime: upstreamTs,
		Timestamp:  time.UnixMilli(1999999999999),
	})

	msg := <-c.SendCh()
	ev := msg.(*protocol.Event)
	if ev.SourceTime != upstreamTs {
		t.Errorf("SourceTime: got %q, want %q", ev.SourceTime, upstreamTs)
	}
}

func TestPublishSourceTimeFallback(t *testing.T) {
	h := NewHub()
	server, client := testNetPipe(t)
	defer server.Close()
	defer client.Close()
	c := NewConn(server, nil, "k", []string{"t"}, 1, "")
	c.sendCh = make(chan interface{}, 1)
	h.RegisterAndIsFirst(c)

	h.Publish(&event.RawEvent{
		EventID:   "evt-2",
		EventType: "t",
		Timestamp: time.UnixMilli(42),
	})

	msg := <-c.SendCh()
	ev := msg.(*protocol.Event)
	if ev.SourceTime != "42" {
		t.Errorf("SourceTime fallback: got %q, want %q", ev.SourceTime, "42")
	}
}

func testNetPipe(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	return net.Pipe()
}

// TestHubRegisteredEventTypes_DedupsAcrossSubscribers locks the aggregator
// Task 15a adds so handleStatusQuery (bus.go) can populate
// StatusResponse.RegisteredEventTypes (spec §4.2): the union of every
// registered subscriber's EventTypes(), deduplicated — mirrors
// subscribedEventTypes's dedup-via-seen-set shape, but over LIVE registered
// consumers rather than the static event registry.
func TestHubRegisteredEventTypes_DedupsAcrossSubscribers(t *testing.T) {
	h := NewHub()
	server1, client1 := testNetPipe(t)
	defer server1.Close()
	defer client1.Close()
	server2, client2 := testNetPipe(t)
	defer server2.Close()
	defer client2.Close()

	c1 := NewConn(server1, nil, "im.msg", []string{"im.message.receive_v1", "im.message.created_v1"}, 1, "")
	c2 := NewConn(server2, nil, "vc.meeting", []string{"vc.meeting.started_v1", "im.message.created_v1"}, 2, "")
	h.RegisterAndIsFirst(c1)
	h.RegisterAndIsFirst(c2)

	got := h.RegisteredEventTypes()
	want := map[string]bool{
		"im.message.receive_v1": true,
		"im.message.created_v1": true,
		"vc.meeting.started_v1": true,
	}
	if len(got) != len(want) {
		t.Fatalf("RegisteredEventTypes() = %v, want %d deduped entries matching %v", got, len(want), want)
	}
	for _, et := range got {
		if !want[et] {
			t.Errorf("unexpected event type %q in RegisteredEventTypes()", et)
		}
	}
}

// TestHubRegisteredEventTypes_EmptyWhenNoSubscribers locks the no-consumers
// baseline: an idle bus with nobody registered yet must report an empty
// (not nil-panicking, not stale) list.
func TestHubRegisteredEventTypes_EmptyWhenNoSubscribers(t *testing.T) {
	h := NewHub()
	if got := h.RegisteredEventTypes(); len(got) != 0 {
		t.Errorf("RegisteredEventTypes() = %v, want empty", got)
	}
}
