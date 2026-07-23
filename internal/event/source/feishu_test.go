// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package source

import (
	"context"
	"fmt"
	"sync"
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
// (matching cmd/event/subscription/subscription.go's formatAuthority
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

// --- subscription lifecycle handler registration ---

// lifecyclePayload synthesizes a raw WS push payload for one of the SDK's 6
// subscription lifecycle meta-events, mirroring the SDK's own
// event/dispatcher/subscription_dispatch_test.go fixture shape (that is what
// dispatcher.Do(ctx, payload) parses in production — the same entry point
// ws/client.go's real WS read loop calls).
func lifecyclePayload(eventType, eventBody string) []byte {
	return []byte(fmt.Sprintf(`{
		"schema":"2.0",
		"header":{
			"event_id":"evt_lifecycle_test",
			"event_type":%q
		},
		"event":%s
	}`, eventType, eventBody))
}

// TestBuildDispatcher_RegistersAllSixLifecycleHandlers_NoPanic locks that
// all 6 typed handlers must register without the SDK dispatcher's
// duplicate-registration panic, and each must reach OnLifecycleEvent.
func TestBuildDispatcher_RegistersAllSixLifecycleHandlers_NoPanic(t *testing.T) {
	s := &FeishuSource{}
	var captured []LifecycleEvent
	s.OnLifecycleEvent = func(_ context.Context, le LifecycleEvent) { captured = append(captured, le) }

	d := s.buildDispatcher([]string{"im.message.receive_v1"}, func(*event.RawEvent) {})

	types := []string{
		lifecycleEventTypeActivated,
		lifecycleEventTypeUpdated,
		lifecycleEventTypeSuspended,
		lifecycleEventTypeExpirationReminder,
		lifecycleEventTypeExpired,
		lifecycleEventTypeDeleted,
	}
	const richBody = `{"subscription_id":"sub_test","target_resource":"im.message?chat_id=oc_x","state":"active"}`
	const updatedBody = `{"before":{"subscription_id":"sub_test","state":"active"},"after":{"subscription_id":"sub_test","state":"suspended"}}`
	const deletedBody = `{"subscription_id":"sub_test"}`

	for _, et := range types {
		body := richBody
		switch et {
		case lifecycleEventTypeUpdated:
			body = updatedBody
		case lifecycleEventTypeDeleted:
			body = deletedBody
		}
		if _, err := d.Do(context.Background(), lifecyclePayload(et, body)); err != nil {
			t.Fatalf("Do(%s) failed: %v", et, err)
		}
	}

	if len(captured) != len(types) {
		t.Fatalf("OnLifecycleEvent called %d times, want %d (captured=%+v)", len(captured), len(types), captured)
	}
}

// recordingEncryptKeyProvider is a structural larkevent.EncryptKeyProvider that
// records which subscription_ids it was asked for (decryption wiring tests).
type recordingEncryptKeyProvider struct {
	mu    sync.Mutex
	asked []string
	key   string
	ok    bool
}

func (r *recordingEncryptKeyProvider) EncryptKey(_ context.Context, subID string) (string, bool) {
	r.mu.Lock()
	r.asked = append(r.asked, subID)
	r.mu.Unlock()
	return r.key, r.ok
}

// encryptedEnvelope is a whole-envelope encrypted subscription push body:
// top-level plaintext encrypt_info (routing) + encrypt (ciphertext).
const encryptedEnvelope = `{"encrypt_info":{"subscription":{"subscription_id":"sub_enc_1"}},"encrypt":"not-real-ciphertext"}`

// TestBuildDispatcher_EncryptKeyProviderConsultedAndFailClosed proves the
// wiring: with an EncryptKeyProvider set, buildDispatcher calls
// WithEncryptKeyProvider, so the SDK consults it (by subscription_id) for an
// encrypted envelope. On a miss the envelope stays encrypted and fails the
// downstream parse fail-closed — it NEVER reaches emit (no ciphertext delivery).
func TestBuildDispatcher_EncryptKeyProviderConsultedAndFailClosed(t *testing.T) {
	prov := &recordingEncryptKeyProvider{ok: false} // miss
	s := &FeishuSource{EncryptKeyProvider: prov}
	var emitted int
	d := s.buildDispatcher([]string{"im.message.receive_v1"}, func(*event.RawEvent) { emitted++ })

	// A miss leaves the body encrypted -> downstream parse fails -> Do errors.
	// That error is the fail-closed behavior; we assert on the side effects.
	_, _ = d.Do(context.Background(), []byte(encryptedEnvelope))

	prov.mu.Lock()
	asked := append([]string(nil), prov.asked...)
	prov.mu.Unlock()
	if len(asked) != 1 || asked[0] != "sub_enc_1" {
		t.Fatalf("provider consulted for %v, want [sub_enc_1] (WithEncryptKeyProvider not wired?)", asked)
	}
	if emitted != 0 {
		t.Errorf("emit called %d times, want 0: an undecryptable envelope must never be delivered", emitted)
	}
}

// TestBuildDispatcher_NilEncryptKeyProvider_Unchanged proves the opt-in
// contract: with no provider, WithEncryptKeyProvider is never called, so an
// encrypted envelope is handled exactly as before decryption support (parse-fails, no
// emit) — and no provider is consulted because there is none.
func TestBuildDispatcher_NilEncryptKeyProvider_Unchanged(t *testing.T) {
	s := &FeishuSource{} // no EncryptKeyProvider
	var emitted int
	d := s.buildDispatcher([]string{"im.message.receive_v1"}, func(*event.RawEvent) { emitted++ })
	_, _ = d.Do(context.Background(), []byte(encryptedEnvelope))
	if emitted != 0 {
		t.Errorf("emit called %d times, want 0", emitted)
	}
}

// TestBuildDispatcher_KeyPresentButDecryptFails_FailClosed_NoEmit:
// with a key present but the ciphertext undecryptable, the SDK dispatcher
// fail-closes — Do returns an error and emit is NEVER called (no ciphertext is
// ever delivered as a plaintext event).
func TestBuildDispatcher_KeyPresentButDecryptFails_FailClosed_NoEmit(t *testing.T) {
	// A key IS available for the subscription, but encryptedEnvelope's "encrypt"
	// field is not valid ciphertext for it, so EventDecrypt fails.
	prov := &recordingEncryptKeyProvider{key: "some-key", ok: true}
	s := &FeishuSource{EncryptKeyProvider: prov}
	var emitted int
	d := s.buildDispatcher([]string{"im.message.receive_v1"}, func(*event.RawEvent) { emitted++ })

	_, err := d.Do(context.Background(), []byte(encryptedEnvelope))
	if err == nil {
		t.Error("Do must return a fail-closed error when decryption fails")
	}
	if emitted != 0 {
		t.Errorf("emit called %d times, want 0: an undecryptable envelope is never delivered", emitted)
	}
}

// TestSdkLogger_DecryptFailure_ExtractsSubscriptionID: a fail-closed
// decrypt-failure SDK Error line (the ws client wraps the dispatcher Do error)
// fires OnDecryptFailure with the extracted subscription_id; unrelated error
// lines never fire it. The callback only ever receives a subscription_id —
// never a key/ciphertext/plaintext.
func TestSdkLogger_DecryptFailure_ExtractsSubscriptionID(t *testing.T) {
	var got []string
	lg := &sdkLogger{onDecryptFailure: func(subID string) { got = append(got, subID) }}

	// Exactly how ws/client.go wraps a dispatcher Do error for the WS path.
	lg.Error(context.Background(), "handle message failed, message_type: event, message_id: m1, trace_id: t1, err: subscription event decryption failed (subscription_id=sub_abc123): illegal base64 data")
	if len(got) != 1 || got[0] != "sub_abc123" {
		t.Fatalf("OnDecryptFailure got %v, want [sub_abc123]", got)
	}

	got = nil
	lg.Error(context.Background(), "handle message failed, message_type: event, err: some unrelated transport problem")
	if len(got) != 0 {
		t.Errorf("OnDecryptFailure fired on a non-decrypt error: %v", got)
	}
}

// TestSdkLogger_DecryptFailure_NilCallback_NoPanic: detection is best-effort.
func TestSdkLogger_DecryptFailure_NilCallback_NoPanic(t *testing.T) {
	lg := &sdkLogger{} // no onDecryptFailure
	lg.Error(context.Background(), "err: subscription event decryption failed (subscription_id=sub_x): boom")
}

// TestBuildDispatcher_OnLifecycleEventNil_NoPanic: a FeishuSource with no
// lifecycle executor configured (OnLifecycleEvent left nil, matching
// OnConnReady's own nil-tolerant contract) must not panic when a lifecycle
// event arrives.
func TestBuildDispatcher_OnLifecycleEventNil_NoPanic(t *testing.T) {
	s := &FeishuSource{}
	d := s.buildDispatcher(nil, func(*event.RawEvent) {})
	body := `{"subscription_id":"sub_1","state":"active"}`
	if _, err := d.Do(context.Background(), lifecyclePayload(lifecycleEventTypeActivated, body)); err != nil {
		t.Fatalf("Do failed: %v", err)
	}
}

// TestBuildDispatcher_LifecycleEventNeverReachesEmit is the direct proof that
// lifecycle meta-events are diverted away from business-consumer delivery
// (lifecycle handlers are bus-internal control plane, not delivered to
// ordinary consumers): registering the 6 typed handlers makes the SDK route by
// event_type to them instead of the customized-event handler that calls
// emit/Hub.Publish. emit fires t.Fatal if ever invoked for any of the 6.
func TestBuildDispatcher_LifecycleEventNeverReachesEmit(t *testing.T) {
	s := &FeishuSource{}
	s.OnLifecycleEvent = func(context.Context, LifecycleEvent) {}
	d := s.buildDispatcher([]string{"im.message.receive_v1"}, func(e *event.RawEvent) {
		t.Fatalf("emit must never be called for a subscription lifecycle event, got %+v", e)
	})

	types := []string{
		lifecycleEventTypeActivated,
		lifecycleEventTypeUpdated,
		lifecycleEventTypeSuspended,
		lifecycleEventTypeExpirationReminder,
		lifecycleEventTypeExpired,
		lifecycleEventTypeDeleted,
	}
	for _, et := range types {
		body := `{"subscription_id":"sub_test","state":"active"}`
		if et == lifecycleEventTypeUpdated {
			body = `{"before":{"subscription_id":"sub_test"},"after":{"subscription_id":"sub_test","state":"active"}}`
		} else if et == lifecycleEventTypeDeleted {
			body = `{"subscription_id":"sub_test"}`
		}
		if _, err := d.Do(context.Background(), lifecyclePayload(et, body)); err != nil {
			t.Fatalf("Do(%s) failed: %v", et, err)
		}
	}
}

// TestBuildDispatcher_LifecycleEvents_NormalizeFields covers every one of
// the 6 handlers' field normalization: body
// SubscriptionId preferred (After.SubscriptionId for updated, body-only for
// deleted), event_id from header, suspension.code carried verbatim
// (including an unrecognized/future value — an open vocabulary),
// authority formatted via the shared formatSubscriptionAuthority vocabulary.
func TestBuildDispatcher_LifecycleEvents_NormalizeFields(t *testing.T) {
	tests := []struct {
		name      string
		eventType string
		body      string
		want      LifecycleEvent
	}{
		{
			name:      "activated",
			eventType: lifecycleEventTypeActivated,
			body:      `{"subscription_id":"sub_1","target_resource":"im.message?chat_id=oc_1","authority":{"type":"user","open_id":"ou_1"},"state":"active","expire_time":1700000000}`,
			want: LifecycleEvent{
				EventType:            lifecycleEventTypeActivated,
				EventID:              "evt_lifecycle_test",
				RemoteSubscriptionID: "sub_1",
				TargetResource:       "im.message?chat_id=oc_1",
				Authority:            "user:ou_1",
				State:                "active",
				ExpireTime:           1700000000,
			},
		},
		{
			name:      "suspended carries a KNOWN suspension code verbatim",
			eventType: lifecycleEventTypeSuspended,
			body:      `{"subscription_id":"sub_2","state":"suspended","suspension":{"code":"authority_revoked"}}`,
			want: LifecycleEvent{
				EventType:            lifecycleEventTypeSuspended,
				EventID:              "evt_lifecycle_test",
				RemoteSubscriptionID: "sub_2",
				State:                "suspended",
				SuspensionCode:       "authority_revoked",
			},
		},
		{
			name:      "suspended carries an UNKNOWN suspension code verbatim (open vocabulary)",
			eventType: lifecycleEventTypeSuspended,
			body:      `{"subscription_id":"sub_2b","state":"suspended","suspension":{"code":"some_future_reason_cli_has_never_seen"}}`,
			want: LifecycleEvent{
				EventType:            lifecycleEventTypeSuspended,
				EventID:              "evt_lifecycle_test",
				RemoteSubscriptionID: "sub_2b",
				State:                "suspended",
				SuspensionCode:       "some_future_reason_cli_has_never_seen",
			},
		},
		{
			name:      "expiration_reminder",
			eventType: lifecycleEventTypeExpirationReminder,
			body:      `{"subscription_id":"sub_3","state":"active","expire_time":42}`,
			want: LifecycleEvent{
				EventType:            lifecycleEventTypeExpirationReminder,
				EventID:              "evt_lifecycle_test",
				RemoteSubscriptionID: "sub_3",
				State:                "active",
				ExpireTime:           42,
			},
		},
		{
			name:      "expired",
			eventType: lifecycleEventTypeExpired,
			body:      `{"subscription_id":"sub_4","state":"expired"}`,
			want: LifecycleEvent{
				EventType:            lifecycleEventTypeExpired,
				EventID:              "evt_lifecycle_test",
				RemoteSubscriptionID: "sub_4",
				State:                "expired",
			},
		},
		{
			name:      "deleted is body-only (no state/target_resource/authority in the SDK's own body shape)",
			eventType: lifecycleEventTypeDeleted,
			body:      `{"subscription_id":"sub_5"}`,
			want: LifecycleEvent{
				EventType:            lifecycleEventTypeDeleted,
				EventID:              "evt_lifecycle_test",
				RemoteSubscriptionID: "sub_5",
			},
		},
		{
			name:      "updated normalizes the AFTER snapshot, not before",
			eventType: lifecycleEventTypeUpdated,
			body:      `{"before":{"subscription_id":"sub_6","state":"active"},"after":{"subscription_id":"sub_6","state":"suspended","suspension":{"code":"authority_revoked"}}}`,
			want: LifecycleEvent{
				EventType:            lifecycleEventTypeUpdated,
				EventID:              "evt_lifecycle_test",
				RemoteSubscriptionID: "sub_6",
				State:                "suspended",
				SuspensionCode:       "authority_revoked",
			},
		},
		{
			name:      "updated with no after snapshot normalizes to an empty remote_subscription_id (dropped later by Submit's own validation)",
			eventType: lifecycleEventTypeUpdated,
			body:      `{"before":{"subscription_id":"sub_7","state":"active"}}`,
			want: LifecycleEvent{
				EventType: lifecycleEventTypeUpdated,
				EventID:   "evt_lifecycle_test",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &FeishuSource{}
			var got LifecycleEvent
			var calls int
			s.OnLifecycleEvent = func(_ context.Context, le LifecycleEvent) { got = le; calls++ }

			d := s.buildDispatcher(nil, func(e *event.RawEvent) {
				t.Fatalf("emit must never be called for a lifecycle event, got %+v", e)
			})

			if _, err := d.Do(context.Background(), lifecyclePayload(tc.eventType, tc.body)); err != nil {
				t.Fatalf("Do failed: %v", err)
			}
			if calls != 1 {
				t.Fatalf("OnLifecycleEvent called %d times, want 1", calls)
			}
			if got != tc.want {
				t.Errorf("normalized LifecycleEvent =\n  %+v\nwant\n  %+v", got, tc.want)
			}
		})
	}
}

// TestBuildDispatcher_LifecycleCollisionWithBusinessType_SkipsLifecycleHandler
// is the panic-safety guard (registering a custom handler twice for the same
// event type would panic the SDK Dispatcher): IF a business EventKey's event_type
// ever collided with one of the 6 lifecycle types (none do today), buildDispatcher
// must not panic, and the business path (already registered first) keeps
// serving that event_type instead.
func TestBuildDispatcher_LifecycleCollisionWithBusinessType_SkipsLifecycleHandler(t *testing.T) {
	s := &FeishuSource{}
	var lifecycleCalls int
	s.OnLifecycleEvent = func(context.Context, LifecycleEvent) { lifecycleCalls++ }

	var emitted *event.RawEvent
	d := s.buildDispatcher([]string{lifecycleEventTypeActivated}, func(e *event.RawEvent) { emitted = e })

	body := `{"subscription_id":"sub_1","state":"active"}`
	if _, err := d.Do(context.Background(), lifecyclePayload(lifecycleEventTypeActivated, body)); err != nil {
		t.Fatalf("Do failed: %v", err)
	}

	if emitted == nil {
		t.Fatal("expected the business OnCustomizedEvent handler (emit) to fire when a business type collides with a lifecycle type")
	}
	if lifecycleCalls != 0 {
		t.Errorf("OnLifecycleEvent called %d times, want 0 (lifecycle handler must be skipped on collision)", lifecycleCalls)
	}
}
