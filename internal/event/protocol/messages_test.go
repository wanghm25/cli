// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package protocol

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"testing"
	"time"
)

// Every NewXxx helper must set the Type discriminator (Decode rejects messages without it).
func TestConstructors_PinTypeField(t *testing.T) {
	if got := NewHello(1, "k", []string{"t"}, "v1", ""); got.Type != MsgTypeHello {
		t.Errorf("NewHello.Type = %q, want %q", got.Type, MsgTypeHello)
	}
	if got := NewHelloAck("v1", true); got.Type != MsgTypeHelloAck || !got.FirstForKey {
		t.Errorf("NewHelloAck mismatch: %+v", got)
	}
	if got := NewEvent("im.msg", "e1", "", 7, json.RawMessage(`{}`)); got.Type != MsgTypeEvent || got.Seq != 7 {
		t.Errorf("NewEvent mismatch: %+v", got)
	}
	if got := NewPreShutdownCheck("k", ""); got.Type != MsgTypePreShutdownCheck || got.EventKey != "k" {
		t.Errorf("NewPreShutdownCheck mismatch: %+v", got)
	}
	if got := NewPreShutdownAck(true); got.Type != MsgTypePreShutdownAck || !got.LastForKey {
		t.Errorf("NewPreShutdownAck mismatch: %+v", got)
	}
	if got := NewStatusQuery(); got.Type != MsgTypeStatusQuery {
		t.Errorf("NewStatusQuery.Type = %q", got.Type)
	}
	if got := NewStatusResponse(42, 10, 2, []ConsumerInfo{{PID: 1}, {PID: 2}}); got.Type != MsgTypeStatusResponse || got.PID != 42 || len(got.Consumers) != 2 {
		t.Errorf("NewStatusResponse mismatch: %+v", got)
	}
	if got := NewShutdown(); got.Type != MsgTypeShutdown {
		t.Errorf("NewShutdown.Type = %q", got.Type)
	}
	if got := NewSourceStatus("feishu-ws", SourceStateConnected, "ok"); got.Type != MsgTypeSourceStatus || got.Detail != "ok" {
		t.Errorf("NewSourceStatus mismatch: %+v", got)
	}
}

func TestEncode_DecodeRoundtripAllTypes(t *testing.T) {
	roundtrip := func(t *testing.T, msg interface{}, want interface{}) {
		t.Helper()
		var buf bytes.Buffer
		if err := Encode(&buf, msg); err != nil {
			t.Fatalf("encode: %v", err)
		}
		line := bytes.TrimRight(buf.Bytes(), "\n")
		got, err := Decode(line)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if gotT, wantT := fmt.Sprintf("%T", got), fmt.Sprintf("%T", want); gotT != wantT {
			t.Errorf("decoded type = %s, want %s", gotT, wantT)
		}
	}
	roundtrip(t, NewHelloAck("v1", true), &HelloAck{})
	roundtrip(t, NewPreShutdownCheck("im.msg", ""), &PreShutdownCheck{})
	roundtrip(t, NewPreShutdownAck(false), &PreShutdownAck{})
	roundtrip(t, NewStatusQuery(), &StatusQuery{})
	roundtrip(t, NewStatusResponse(7, 120, 1, []ConsumerInfo{{PID: 99, EventKey: "k"}}), &StatusResponse{})
	roundtrip(t, NewShutdown(), &Shutdown{})
	roundtrip(t, NewSourceStatus("feishu", SourceStateReconnecting, "attempt 2"), &SourceStatus{})
	roundtrip(t, &Bye{Type: MsgTypeBye}, &Bye{})
}

// EncodeWithDeadline must apply a write deadline so a wedged peer can't stall the writer forever.
func TestEncodeWithDeadline_AppliesDeadline(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	start := time.Now()
	err := EncodeWithDeadline(client, NewShutdown(), 100*time.Millisecond)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected deadline error, got nil")
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("EncodeWithDeadline didn't honour deadline: took %v (want ~100ms)", elapsed)
	}
}

func TestReadFrame_RejectsOversized(t *testing.T) {
	big := bytes.Repeat([]byte("a"), MaxFrameBytes+1)
	big = append(big, '\n')
	br := bufio.NewReader(bytes.NewReader(big))
	_, err := ReadFrame(br)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("ReadFrame on oversized input: err = %v, want ErrFrameTooLarge", err)
	}
}

func TestReadFrame_PropagatesEOF(t *testing.T) {
	br := bufio.NewReader(bytes.NewReader(nil))
	_, err := ReadFrame(br)
	if err != io.EOF {
		t.Errorf("err = %v, want io.EOF", err)
	}
}

func TestHelloAckRejected_RoundTrip(t *testing.T) {
	ack := NewHelloAckRejected("v1", "another consumer (pid 42) is already running for this subscription")
	if !ack.Rejected || ack.RejectReason == "" {
		t.Fatalf("NewHelloAckRejected fields: %+v", ack)
	}
	var buf bytes.Buffer
	if err := Encode(&buf, ack); err != nil {
		t.Fatalf("encode: %v", err)
	}
	msg, err := Decode(bytes.TrimRight(buf.Bytes(), "\n"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, ok := msg.(*HelloAck)
	if !ok {
		t.Fatalf("decoded type = %T, want *HelloAck", msg)
	}
	if !got.Rejected || got.RejectReason != ack.RejectReason {
		t.Errorf("roundtrip = %+v, want Rejected with reason", got)
	}
}

// --- IPC protocol v2 additive fields (spec §4.2) ---
//
// These tests cover three things per frame: (1) a v2-populated frame
// round-trips through Encode/Decode preserving every new field, (2) the
// legacy `subscription_id` field's meaning (local parameter fingerprint) is
// untouched by the new `remote_subscription_id` field, and (3) an OLD frame
// (marshaled as if by a pre-v2 build, i.e. missing the new keys entirely)
// still decodes cleanly with the new fields left at their zero value and the
// old fields intact — this is the backward-compat contract §4.2 requires.

func TestHello_V2FieldsRoundTrip(t *testing.T) {
	h := &Hello{
		Type:                 MsgTypeHello,
		PID:                  123,
		EventKey:             "im.message.created_v1/chat-id/oc_xxx",
		EventTypes:           []string{"im.message.created_v1"},
		Version:              "v1",
		SubscriptionID:       "im.message.created_v1:chat-id:oc_xxx", // local fingerprint (frozen meaning)
		ConsumerScopeID:      "scope-abc123",
		RemoteSubscriptionID: "sub_123", // remote OpenAPI id — must NOT alias SubscriptionID
		Identity:             "user",
		Profile:              "work",
		UserOpenID:           "ou_xxx",
		Capabilities:         []string{"hello_v2"},
	}
	var buf bytes.Buffer
	if err := Encode(&buf, h); err != nil {
		t.Fatalf("encode: %v", err)
	}
	msg, err := Decode(bytes.TrimRight(buf.Bytes(), "\n"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, ok := msg.(*Hello)
	if !ok {
		t.Fatalf("decoded type = %T, want *Hello", msg)
	}
	if got.SubscriptionID != h.SubscriptionID {
		t.Errorf("SubscriptionID (local fingerprint) corrupted by roundtrip: got %q, want %q", got.SubscriptionID, h.SubscriptionID)
	}
	if got.RemoteSubscriptionID == got.SubscriptionID {
		t.Errorf("RemoteSubscriptionID must be distinct from SubscriptionID, both = %q", got.SubscriptionID)
	}
	if got.ConsumerScopeID != h.ConsumerScopeID {
		t.Errorf("ConsumerScopeID = %q, want %q", got.ConsumerScopeID, h.ConsumerScopeID)
	}
	if got.RemoteSubscriptionID != h.RemoteSubscriptionID {
		t.Errorf("RemoteSubscriptionID = %q, want %q", got.RemoteSubscriptionID, h.RemoteSubscriptionID)
	}
	if got.Identity != h.Identity {
		t.Errorf("Identity = %q, want %q", got.Identity, h.Identity)
	}
	if got.Profile != h.Profile {
		t.Errorf("Profile = %q, want %q", got.Profile, h.Profile)
	}
	if got.UserOpenID != h.UserOpenID {
		t.Errorf("UserOpenID = %q, want %q", got.UserOpenID, h.UserOpenID)
	}
	if !reflect.DeepEqual(got.Capabilities, h.Capabilities) {
		t.Errorf("Capabilities = %v, want %v", got.Capabilities, h.Capabilities)
	}
}

func TestEvent_V2FieldsRoundTrip(t *testing.T) {
	e := &Event{
		Type:                 MsgTypeEvent,
		EventType:            "im.message.created_v1",
		EventID:              "ev_1",
		SourceTime:           "1234567890",
		Seq:                  3,
		Payload:              json.RawMessage(`{"a":1}`),
		RemoteSubscriptionID: "sub_123",
		TargetResource:       "im.message?chat_id=oc_xxx",
		Authority:            "user:ou_xxx",
		SubscriptionEventID:  "sub_evt_1",
	}
	var buf bytes.Buffer
	if err := Encode(&buf, e); err != nil {
		t.Fatalf("encode: %v", err)
	}
	msg, err := Decode(bytes.TrimRight(buf.Bytes(), "\n"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, ok := msg.(*Event)
	if !ok {
		t.Fatalf("decoded type = %T, want *Event", msg)
	}
	if got.RemoteSubscriptionID != e.RemoteSubscriptionID {
		t.Errorf("RemoteSubscriptionID = %q, want %q", got.RemoteSubscriptionID, e.RemoteSubscriptionID)
	}
	if got.TargetResource != e.TargetResource {
		t.Errorf("TargetResource = %q, want %q", got.TargetResource, e.TargetResource)
	}
	if got.Authority != e.Authority {
		t.Errorf("Authority = %q, want %q", got.Authority, e.Authority)
	}
	if got.SubscriptionEventID != e.SubscriptionEventID {
		t.Errorf("SubscriptionEventID = %q, want %q", got.SubscriptionEventID, e.SubscriptionEventID)
	}
	// Pre-existing fields must survive untouched alongside the new ones.
	if got.EventType != e.EventType || got.EventID != e.EventID || got.Seq != e.Seq {
		t.Errorf("legacy Event fields corrupted: got %+v", got)
	}
}

func TestStatusResponse_V2CapabilityFieldsRoundTrip(t *testing.T) {
	sr := &StatusResponse{
		Type:                 MsgTypeStatusResponse,
		PID:                  1,
		UptimeSec:            10,
		ActiveConns:          2,
		Consumers:            []ConsumerInfo{{PID: 2, EventKey: "im.message.created_v1", SubscriptionID: "fp"}},
		ProtocolVersion:      "v2",
		Capabilities:         []string{"refined_routing", "hello_v2"},
		RegisteredEventTypes: []string{"im.message.created_v1", "im.message.receive_v1"},
	}
	var buf bytes.Buffer
	if err := Encode(&buf, sr); err != nil {
		t.Fatalf("encode: %v", err)
	}
	msg, err := Decode(bytes.TrimRight(buf.Bytes(), "\n"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, ok := msg.(*StatusResponse)
	if !ok {
		t.Fatalf("decoded type = %T, want *StatusResponse", msg)
	}
	if got.ProtocolVersion != sr.ProtocolVersion {
		t.Errorf("ProtocolVersion = %q, want %q", got.ProtocolVersion, sr.ProtocolVersion)
	}
	if !reflect.DeepEqual(got.Capabilities, sr.Capabilities) {
		t.Errorf("Capabilities = %v, want %v", got.Capabilities, sr.Capabilities)
	}
	if !reflect.DeepEqual(got.RegisteredEventTypes, sr.RegisteredEventTypes) {
		t.Errorf("RegisteredEventTypes = %v, want %v", got.RegisteredEventTypes, sr.RegisteredEventTypes)
	}
	if len(got.Consumers) != 1 || got.Consumers[0].SubscriptionID != "fp" {
		t.Errorf("legacy Consumers field corrupted: %+v", got.Consumers)
	}
}

// TestHello_V2FieldsOmittedWhenZero pins the `omitempty` contract: a Hello
// built the old way (no v2 fields set) must marshal to exactly the old wire
// shape, byte for byte indistinguishable from a pre-v2 build. This is what
// lets a v1 bus/consumer on the other end ignore v2 entirely instead of
// choking on unexpected keys.
func TestHello_V2FieldsOmittedWhenZero(t *testing.T) {
	h := NewHello(1, "im.message.receive_v1", []string{"im.message.receive_v1"}, "v1", "")
	data, err := json.Marshal(h)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, key := range []string{
		`"consumer_scope_id"`, `"remote_subscription_id"`, `"identity"`,
		`"profile"`, `"user_open_id"`, `"capabilities"`,
	} {
		if bytes.Contains(data, []byte(key)) {
			t.Errorf("zero-valued v2 field leaked onto wire: %s in %s", key, data)
		}
	}
}

// TestDecode_OldHello_BackwardCompat feeds Decode a hand-written JSON line
// shaped exactly like a message a pre-v2 build would have produced (no v2
// keys at all) and checks it still decodes, with the new fields at zero
// value and the old `subscription_id` — the local parameter fingerprint —
// untouched.
func TestDecode_OldHello_BackwardCompat(t *testing.T) {
	old := `{"type":"hello","pid":55,"event_key":"im.message.receive_v1","event_types":["im.message.receive_v1"],"version":"v1","subscription_id":"im.message.receive_v1:legacy-fp"}`
	msg, err := Decode([]byte(old))
	if err != nil {
		t.Fatalf("decode old hello frame: %v", err)
	}
	h, ok := msg.(*Hello)
	if !ok {
		t.Fatalf("decoded type = %T, want *Hello", msg)
	}
	if h.SubscriptionID != "im.message.receive_v1:legacy-fp" {
		t.Errorf("old subscription_id (local fingerprint) lost: got %q", h.SubscriptionID)
	}
	if h.ConsumerScopeID != "" || h.RemoteSubscriptionID != "" || h.Identity != "" ||
		h.Profile != "" || h.UserOpenID != "" || h.Capabilities != nil {
		t.Errorf("v2 fields should be zero-valued decoding an old frame, got %+v", h)
	}
}

func TestDecode_OldEvent_BackwardCompat(t *testing.T) {
	old := `{"type":"event","event_type":"im.message.receive_v1","event_id":"e1","source_time":"111","seq":5,"payload":{"x":1}}`
	msg, err := Decode([]byte(old))
	if err != nil {
		t.Fatalf("decode old event frame: %v", err)
	}
	e, ok := msg.(*Event)
	if !ok {
		t.Fatalf("decoded type = %T, want *Event", msg)
	}
	if e.EventType != "im.message.receive_v1" || e.EventID != "e1" || e.Seq != 5 {
		t.Errorf("legacy Event fields not preserved: %+v", e)
	}
	if e.RemoteSubscriptionID != "" || e.TargetResource != "" || e.Authority != "" || e.SubscriptionEventID != "" {
		t.Errorf("v2 fields should be zero-valued decoding an old frame, got %+v", e)
	}
}

func TestDecode_OldStatusResponse_BackwardCompat(t *testing.T) {
	old := `{"type":"status_response","pid":1,"uptime_sec":5,"active_conns":1,"consumers":[{"pid":2,"event_key":"k","subscription_id":"k:fp","received":1,"dropped":0}]}`
	msg, err := Decode([]byte(old))
	if err != nil {
		t.Fatalf("decode old status_response frame: %v", err)
	}
	sr, ok := msg.(*StatusResponse)
	if !ok {
		t.Fatalf("decoded type = %T, want *StatusResponse", msg)
	}
	if len(sr.Consumers) != 1 || sr.Consumers[0].SubscriptionID != "k:fp" {
		t.Errorf("legacy Consumers not preserved: %+v", sr.Consumers)
	}
	if sr.ProtocolVersion != "" || sr.Capabilities != nil || sr.RegisteredEventTypes != nil {
		t.Errorf("v2 fields should be zero-valued decoding an old frame, got %+v", sr)
	}
	// Task 16 (spec §4.6): the old consumer entry above carries none of the
	// refined-status fields at all — every one of them must decode
	// zero-valued, exactly like the StatusResponse-level v2 fields above.
	c := sr.Consumers[0]
	if c.RefinedSubscription || c.RemoteSubscriptionID != "" || c.OwnerIdentity != "" ||
		c.OwnerAppID != "" || c.OwnerUserOpenID != "" || c.StaleIdentity || c.DegradedReason != "" ||
		c.RemoteState != "" || c.RemoteSubscription != nil || c.LastLifecycleEvent != "" {
		t.Errorf("Task 16 ConsumerInfo fields should be zero-valued decoding an old frame, got %+v", c)
	}
}

// --- Task 16 additive ConsumerInfo fields (spec §4.6) ---
//
// Mirrors the v2-additive-field test shape already established above for
// Hello/Event/StatusResponse: (1) a fully-populated ConsumerInfo round-trips
// through Encode/Decode inside a StatusResponse, and (2) omitempty means a
// ConsumerInfo built the old way (only the pre-Task-16 fields set) marshals
// to exactly the old wire shape, byte for byte indistinguishable from a
// pre-Task-16 build.

func TestConsumerInfo_V2FieldsRoundTrip(t *testing.T) {
	ci := ConsumerInfo{
		PID:                  7,
		EventKey:             "im.message.created_v1/chat-id/oc_xxx",
		SubscriptionID:       "im.message.created_v1:chat-id:oc_xxx",
		Received:             42,
		Dropped:              1,
		RefinedSubscription:  true,
		RemoteSubscriptionID: "sub_abc123",
		OwnerIdentity:        "user",
		OwnerAppID:           "cli_app123",
		OwnerUserOpenID:      "ou_xxx",
		StaleIdentity:        true,
		DegradedReason:       "bind_failed: uat_unavailable",
		RemoteState:          "enabled",
		RemoteSubscription: &RemoteSubscriptionInfo{
			State:               "enabled",
			ExpireTime:          1732000000,
			IncludeResourceData: true,
		},
		LastLifecycleEvent: "subscription.activated",
	}
	sr := NewStatusResponse(1, 10, 1, []ConsumerInfo{ci})

	var buf bytes.Buffer
	if err := Encode(&buf, sr); err != nil {
		t.Fatalf("encode: %v", err)
	}
	msg, err := Decode(bytes.TrimRight(buf.Bytes(), "\n"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, ok := msg.(*StatusResponse)
	if !ok {
		t.Fatalf("decoded type = %T, want *StatusResponse", msg)
	}
	if len(got.Consumers) != 1 {
		t.Fatalf("Consumers len = %d, want 1", len(got.Consumers))
	}
	gc := got.Consumers[0]
	if !reflect.DeepEqual(gc, ci) {
		t.Errorf("ConsumerInfo roundtrip mismatch:\ngot:  %+v\nwant: %+v", gc, ci)
	}
}

func TestConsumerInfo_V2FieldsOmittedWhenZero(t *testing.T) {
	sr := NewStatusResponse(1, 10, 1, []ConsumerInfo{{
		PID:            2,
		EventKey:       "k",
		SubscriptionID: "k:fp",
		Received:       1,
		Dropped:        0,
	}})
	data, err := json.Marshal(sr)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, key := range []string{
		`"refined_subscription"`, `"remote_subscription_id"`, `"owner_identity"`,
		`"owner_app_id"`, `"owner_user_open_id"`, `"stale_identity"`,
		`"degraded_reason"`, `"remote_state"`, `"remote_subscription"`,
		`"last_lifecycle_event"`,
	} {
		if bytes.Contains(data, []byte(key)) {
			t.Errorf("zero-valued Task 16 field leaked onto wire: %s in %s", key, data)
		}
	}
}
