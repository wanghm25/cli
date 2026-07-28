// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/larksuite/cli/internal/event"
)

// N concurrent Publishes of the SAME subscription_event_id must produce exactly
// ONE refined delivery. The dedup claim is atomic (check-and-mark under one
// lock), so exactly one racing Publish wins and delivers — closing the
// check-then-commit TOCTOU where two callers could both pass a read-only check
// before either committed and thus both deliver. Run under -race.
func TestHub_Publish_ConcurrentSameSubscriptionEventID_ExactlyOneDelivery(t *testing.T) {
	h := NewHub()
	refined := newRefinedTestConn("im.msg/chat-id/oc_1", []string{"im.message.receive_v1"}, "R1")
	h.RegisterAndIsFirst(refined)

	const n = 64 // < the testConn sendCh capacity (100), so no drop can mask a double-deliver
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			h.Publish(&event.RawEvent{
				EventID:              "evt-1",
				EventType:            "im.message.receive_v1",
				RemoteSubscriptionID: "R1",
				SubscriptionEventID:  "sub-evt-shared",
				Resource:             "im.message?chat_id=oc_1",
				Authority:            "app",
				Payload:              json.RawMessage(`{}`),
			})
		}()
	}
	wg.Wait()

	if got := refined.Received(); got != 1 {
		t.Errorf("exactly one refined delivery expected for %d concurrent Publishes of the same subscription_event_id, got %d", n, got)
	}
}

// The legacy half of the same guarantee: N concurrent Publishes of the same
// event_id must produce exactly ONE legacy delivery. Run under -race.
func TestHub_Publish_ConcurrentSameEventID_Legacy_ExactlyOneDelivery(t *testing.T) {
	h := NewHub()
	legacy := newTestConn("im.msg", []string{"im.message.receive_v1"})
	h.RegisterAndIsFirst(legacy)

	const n = 64
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			h.Publish(&event.RawEvent{
				EventID:   "evt-legacy-shared",
				EventType: "im.message.receive_v1",
				Payload:   json.RawMessage(`{}`),
			})
		}()
	}
	wg.Wait()

	if got := legacy.Received(); got != 1 {
		t.Errorf("legacy: exactly one delivery expected for %d concurrent Publishes of the same event_id, got %d", n, got)
	}
}

// The refined dedup domain is keyed on subscription_event_id ALONE (globally
// unique), not the old remote_subscription_id-scoped composite: the SAME
// subscription_event_id arriving under a DIFFERENT remote_subscription_id AND a
// different event_id is still deduped. (This is the inverse of
// TestHub_Publish_SplitDedupAcrossRemoteSubscriptions, where DISTINCT
// subscription_event_ids each deliver.)
func TestHub_Publish_RefinedDedupKeyedOnSubscriptionEventID(t *testing.T) {
	h := NewHub()
	r1 := newRefinedTestConn("im.msg/chat-id/oc_1", []string{"im.message.receive_v1"}, "R1")
	r2 := newRefinedTestConn("im.msg/chat-id/oc_2", []string{"im.message.receive_v1"}, "R2")
	h.RegisterAndIsFirst(r1)
	h.RegisterAndIsFirst(r2)

	h.Publish(&event.RawEvent{
		EventID:              "evt-A",
		EventType:            "im.message.receive_v1",
		RemoteSubscriptionID: "R1",
		SubscriptionEventID:  "sub-evt-shared",
		Resource:             "im.message?chat_id=oc_1",
		Authority:            "app",
		Payload:              json.RawMessage(`{}`),
	})
	// Same subscription_event_id, but a different remote_subscription_id AND a
	// different event_id — under subscription_event_id keying this is the SAME
	// key, so it is deduped globally.
	h.Publish(&event.RawEvent{
		EventID:              "evt-B",
		EventType:            "im.message.receive_v1",
		RemoteSubscriptionID: "R2",
		SubscriptionEventID:  "sub-evt-shared",
		Resource:             "im.message?chat_id=oc_2",
		Authority:            "app",
		Payload:              json.RawMessage(`{}`),
	})

	mustReceiveEvent(t, r1.sendCh, "R1 receives the first event carrying sub-evt-shared")
	mustNotReceive(t, r2.sendCh, "R2 must be deduped: the same subscription_event_id was already claimed globally (keyed on subscription_event_id, not remote_subscription_id)")
}
