// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package source

import (
	"context"
	"testing"

	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"

	"github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/protocol"
)

// "disconnected to <url>" contains "connected to ws" — must use HasPrefix to avoid misclassifying as connect.
func TestTryNotify_Classify(t *testing.T) {
	cases := []struct {
		name       string
		msg        string
		errDetail  string
		wantState  string
		wantDetail string
		wantCalled bool
	}{
		{
			name:       "connected (SDK connect success)",
			msg:        "connected to wss://example.com/gw [conn_id=abc]",
			wantState:  protocol.SourceStateConnected,
			wantCalled: true,
		},
		{
			name:       "disconnected must not be misclassified as connected",
			msg:        "disconnected to wss://example.com/gw [conn_id=abc]",
			wantState:  protocol.SourceStateDisconnected,
			wantCalled: true,
		},
		{
			name:       "disconnected carries errDetail through",
			msg:        "disconnected to wss://example.com/gw [conn_id=abc]",
			errDetail:  "read tcp: broken pipe",
			wantState:  protocol.SourceStateDisconnected,
			wantDetail: "read tcp: broken pipe",
			wantCalled: true,
		},
		{
			name:       "reconnecting with attempt 1",
			msg:        "trying to reconnect: 1 [conn_id=abc]",
			wantState:  protocol.SourceStateReconnecting,
			wantDetail: "attempt 1",
			wantCalled: true,
		},
		{
			name:       "reconnecting with attempt 12",
			msg:        "trying to reconnect: 12",
			wantState:  protocol.SourceStateReconnecting,
			wantDetail: "attempt 12",
			wantCalled: true,
		},
		{
			name:       "case-insensitive connected",
			msg:        "CONNECTED TO WSS://example.com",
			wantState:  protocol.SourceStateConnected,
			wantCalled: true,
		},
		{
			name:       "ignore generic connect-failed error",
			msg:        "connect failed, err: dial tcp: i/o timeout",
			errDetail:  "connect failed, err: dial tcp: i/o timeout",
			wantCalled: false,
		},
		{
			name:       "ignore read-loop failure",
			msg:        "receive message failed, err: websocket: close 1006",
			errDetail:  "receive message failed, err: websocket: close 1006",
			wantCalled: false,
		},
		{
			name:       "ignore heartbeat noise",
			msg:        "receive pong",
			wantCalled: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotState, gotDetail string
			called := false
			lg := &sdkLogger{notify: func(state, detail string) {
				called = true
				gotState = state
				gotDetail = detail
			}}
			lg.tryNotify(tc.msg, tc.errDetail)

			if called != tc.wantCalled {
				t.Fatalf("called=%v, want %v (msg=%q)", called, tc.wantCalled, tc.msg)
			}
			if !called {
				return
			}
			if gotState != tc.wantState {
				t.Errorf("state = %q, want %q", gotState, tc.wantState)
			}
			if gotDetail != tc.wantDetail {
				t.Errorf("detail = %q, want %q", gotDetail, tc.wantDetail)
			}
		})
	}
}

func TestTryNotify_NilNotifySafe(t *testing.T) {
	lg := &sdkLogger{notify: nil}
	lg.tryNotify("disconnected to wss://example.com", "")
	lg.tryNotify("connected to wss://example.com", "")
	lg.tryNotify("trying to reconnect: 1", "")
}

// TestRawHandlerSubscriptionEnvelope_UserAuthority: a refined-subscription V2
// event carries header.subscription{subscription_id,resource,authority{type,
// principal_id},subscription_event_id} — buildRawHandler must normalize all
// four into RawEvent, with a "user" authority rendering as "user:<open_id>"
// (spec §4.3, matching cmd/event/subscription/subscription.go's formatAuthority
// vocabulary).
func TestRawHandlerSubscriptionEnvelope_UserAuthority(t *testing.T) {
	s := &FeishuSource{}
	var captured *event.RawEvent
	handler := s.buildRawHandler(func(e *event.RawEvent) { captured = e })

	body := []byte(`{"header":{"event_id":"evt-100","event_type":"im.message.receive_v1","create_time":"1700000000000","subscription":{"subscription_id":"sub_123","resource":"im.message?chat_id=oc_xxx","authority":{"type":"user","principal_id":"ou_abc123"},"subscription_event_id":"sub_evt_456"}}}`)
	if err := handler(context.Background(), &larkevent.EventReq{Body: body}); err != nil {
		t.Fatalf("handler returned err: %v", err)
	}

	if captured == nil {
		t.Fatal("expected emit to fire")
	}
	if captured.RemoteSubscriptionID != "sub_123" {
		t.Errorf("RemoteSubscriptionID: got %q, want %q", captured.RemoteSubscriptionID, "sub_123")
	}
	if captured.Resource != "im.message?chat_id=oc_xxx" {
		t.Errorf("Resource: got %q, want %q", captured.Resource, "im.message?chat_id=oc_xxx")
	}
	if captured.Authority != "user:ou_abc123" {
		t.Errorf("Authority: got %q, want %q", captured.Authority, "user:ou_abc123")
	}
	if captured.SubscriptionEventID != "sub_evt_456" {
		t.Errorf("SubscriptionEventID: got %q, want %q", captured.SubscriptionEventID, "sub_evt_456")
	}
	if string(captured.Payload) != string(body) {
		t.Errorf("Payload should be raw bytes, unchanged")
	}
}

// TestRawHandlerSubscriptionEnvelope_AppAuthority: same envelope shape but an
// "app" authority, which formats as the bare string "app" (no principal_id
// suffix) per the shared vocabulary.
func TestRawHandlerSubscriptionEnvelope_AppAuthority(t *testing.T) {
	s := &FeishuSource{}
	var captured *event.RawEvent
	handler := s.buildRawHandler(func(e *event.RawEvent) { captured = e })

	body := []byte(`{"header":{"event_id":"evt-101","event_type":"im.message.receive_v1","create_time":"1700000000001","subscription":{"subscription_id":"sub_789","resource":"im.message?chat_id=oc_yyy","authority":{"type":"app","principal_id":"cli_xxx"},"subscription_event_id":"sub_evt_999"}}}`)
	if err := handler(context.Background(), &larkevent.EventReq{Body: body}); err != nil {
		t.Fatalf("handler returned err: %v", err)
	}

	if captured == nil {
		t.Fatal("expected emit to fire")
	}
	if captured.RemoteSubscriptionID != "sub_789" {
		t.Errorf("RemoteSubscriptionID: got %q, want %q", captured.RemoteSubscriptionID, "sub_789")
	}
	if captured.Resource != "im.message?chat_id=oc_yyy" {
		t.Errorf("Resource: got %q, want %q", captured.Resource, "im.message?chat_id=oc_yyy")
	}
	if captured.Authority != "app" {
		t.Errorf("Authority: got %q, want %q", captured.Authority, "app")
	}
	if captured.SubscriptionEventID != "sub_evt_999" {
		t.Errorf("SubscriptionEventID: got %q, want %q", captured.SubscriptionEventID, "sub_evt_999")
	}
}

// TestRawHandlerNoSubscription_NewFieldsEmpty: an old-shaped event with no
// header.subscription block must leave all four new RawEvent fields empty,
// and Payload must stay byte-identical to the input body (no envelope
// normalization side effects on the raw bytes).
func TestRawHandlerNoSubscription_NewFieldsEmpty(t *testing.T) {
	s := &FeishuSource{}
	var captured *event.RawEvent
	handler := s.buildRawHandler(func(e *event.RawEvent) { captured = e })

	body := []byte(`{"header":{"event_id":"evt-42","event_type":"im.message.receive_v1","create_time":"1700000000000"}}`)
	if err := handler(context.Background(), &larkevent.EventReq{Body: body}); err != nil {
		t.Fatalf("handler returned err: %v", err)
	}

	if captured == nil {
		t.Fatal("expected emit to fire")
	}
	if captured.RemoteSubscriptionID != "" {
		t.Errorf("RemoteSubscriptionID: got %q, want empty", captured.RemoteSubscriptionID)
	}
	if captured.Resource != "" {
		t.Errorf("Resource: got %q, want empty", captured.Resource)
	}
	if captured.Authority != "" {
		t.Errorf("Authority: got %q, want empty", captured.Authority)
	}
	if captured.SubscriptionEventID != "" {
		t.Errorf("SubscriptionEventID: got %q, want empty", captured.SubscriptionEventID)
	}
	if string(captured.Payload) != string(body) {
		t.Errorf("Payload should be byte-identical to input body: got %s, want %s", captured.Payload, body)
	}
}

// TestFormatSubscriptionAuthority covers the full vocabulary the helper must
// mirror from cmd/event/subscription/subscription.go's formatAuthority:
// "user"+principal -> "user:<principal_id>"; "user" alone -> "user"; "app"
// -> "app" (principal_id ignored); unrecognized type -> passthrough verbatim;
// empty type -> "".
func TestFormatSubscriptionAuthority(t *testing.T) {
	cases := []struct {
		name        string
		authType    string
		principalID string
		want        string
	}{
		{"user with principal", "user", "ou_abc123", "user:ou_abc123"},
		{"user without principal", "user", "", "user"},
		{"app ignores principal", "app", "cli_xxx", "app"},
		{"unrecognized type passthrough", "robot", "id_xxx", "robot"},
		{"empty type", "", "ou_abc123", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := formatSubscriptionAuthority(tc.authType, tc.principalID)
			if got != tc.want {
				t.Errorf("formatSubscriptionAuthority(%q, %q) = %q, want %q", tc.authType, tc.principalID, got, tc.want)
			}
		})
	}
}
