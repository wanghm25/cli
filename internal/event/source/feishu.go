// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package source

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"

	"github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/protocol"
)

const maxEventBodyBytes = 1 << 20 // bound per-subscriber sendCh memory under runaway payloads

type FeishuSource struct {
	AppID     string
	AppSecret string
	Domain    string
	Logger    *log.Logger

	// OnConnReady is invoked on the WS client's OnReady (first usable
	// connection of a run) and OnReconnected (every successful reconnect)
	// callbacks with the fresh connection ID and a bindUser func bound to
	// THIS client instance. A plain func type (not larkws.Client) so this
	// package stays SDK-typed only here — the bus-side identity gate (which
	// needs internal/core config resolution) stays out of this core-free
	// package. nil is tolerated (no identity gating configured).
	OnConnReady func(ctx context.Context, connID string, bindUser func(ctx context.Context, uat string) error)
}

func (s *FeishuSource) Name() string { return "feishu-websocket" }

func (s *FeishuSource) Start(ctx context.Context, eventTypes []string, emit func(*event.RawEvent), notify StatusNotifier) error {
	d := dispatcher.NewEventDispatcher("", "")

	rawHandler := s.buildRawHandler(emit)

	for _, et := range eventTypes {
		d.OnCustomizedEvent(et, rawHandler)
	}

	opts := []larkws.ClientOption{larkws.WithEventHandler(d)}
	if s.Domain != "" {
		opts = append(opts, larkws.WithDomain(s.Domain))
	}
	if s.Logger != nil || notify != nil {
		opts = append(opts, larkws.WithLogLevel(larkcore.LogLevelInfo))
		opts = append(opts, larkws.WithLogger(&sdkLogger{l: s.Logger, notify: notify}))
	}

	// var cli up-front so the ready closure can capture it before NewClient
	// returns (WithOnReady/WithOnReconnected only ever fire AFTER Start(),
	// by which time cli is assigned) — cli.Connection().ConnectionID and
	// cli.BindUser are read fresh on every invocation, never memoized here.
	var cli *larkws.Client
	ready := func(ctx context.Context) {
		if s.OnConnReady != nil {
			s.OnConnReady(ctx, cli.Connection().ConnectionID, cli.BindUser)
		}
	}
	opts = append(opts,
		larkws.WithOnReady(ready),
		// Reconnect rotates connection_id, so any prior BindUser is no
		// longer assumed valid — always re-run the same ready logic.
		larkws.WithOnReconnected(func(ctx context.Context) { ready(ctx) }),
	)

	if notify != nil {
		notify(protocol.SourceStateConnecting, "")
	}
	cli = larkws.NewClient(s.AppID, s.AppSecret, opts...)

	errCh := make(chan error, 1)
	go func() { errCh <- cli.Start(ctx) }()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

// buildRawHandler is extracted from Start so unit tests can exercise it without a WS client.
func (s *FeishuSource) buildRawHandler(emit func(*event.RawEvent)) func(context.Context, *larkevent.EventReq) error {
	return func(_ context.Context, e *larkevent.EventReq) error {
		if e.Body == nil {
			return nil
		}
		if len(e.Body) > maxEventBodyBytes {
			if s.Logger != nil {
				s.Logger.Printf("[feishu] drop oversized event: %d bytes > cap %d", len(e.Body), maxEventBodyBytes)
			}
			return nil
		}
		var envelope struct {
			Header struct {
				EventID    string `json:"event_id"`
				EventType  string `json:"event_type"`
				CreateTime string `json:"create_time"`
				// Subscription is the refined-subscription push envelope
				// (spec §4.3/§0.4): SDK event/model.go's EventHeader.Subscription
				// shape (resource/authority{type,principal_id}), NOT the OpenAPI
				// management-side Subscription (target_resource/authority{open_id,
				// union_id,app_id}) — do not conflate the two. Absent on
				// non-refined events, in which case every subfield below
				// zero-values to "".
				Subscription struct {
					SubscriptionID string `json:"subscription_id"`
					Resource       string `json:"resource"`
					Authority      struct {
						Type        string `json:"type"`
						PrincipalID string `json:"principal_id"`
					} `json:"authority"`
					SubscriptionEventID string `json:"subscription_event_id"`
				} `json:"subscription"`
			} `json:"header"`
		}
		if err := json.Unmarshal(e.Body, &envelope); err != nil {
			if s.Logger != nil {
				preview := string(e.Body)
				if len(preview) > 200 {
					preview = preview[:200] + "...(truncated)"
				}
				s.Logger.Printf("[feishu] drop malformed event: unmarshal error: %v body=%s", err, preview)
			}
			return nil
		}
		if envelope.Header.EventID == "" || envelope.Header.EventType == "" {
			if s.Logger != nil {
				s.Logger.Printf("[feishu] drop event missing header fields: event_id=%q event_type=%q",
					envelope.Header.EventID, envelope.Header.EventType)
			}
			return nil
		}
		emit(&event.RawEvent{
			EventID:    envelope.Header.EventID,
			EventType:  envelope.Header.EventType,
			SourceTime: envelope.Header.CreateTime,
			Payload:    json.RawMessage(e.Body),
			Timestamp:  time.Now(),

			RemoteSubscriptionID: envelope.Header.Subscription.SubscriptionID,
			Resource:             envelope.Header.Subscription.Resource,
			Authority: formatSubscriptionAuthority(
				envelope.Header.Subscription.Authority.Type,
				envelope.Header.Subscription.Authority.PrincipalID,
			),
			SubscriptionEventID: envelope.Header.Subscription.SubscriptionEventID,
		})
		return nil
	}
}

// formatSubscriptionAuthority normalizes the push-envelope's
// header.subscription.authority{type,principal_id} into this spec's compact
// identity vocabulary. Mirrors cmd/event/subscription/subscription.go's
// formatAuthority (same "user:<id>" / "user" / "app" / passthrough rules),
// but takes the push-envelope's {type, principal_id} shape rather than the
// management-side larkeventv1.Authority{Type,OpenId,...} — principal_id IS
// the open_id for a "user" authority, so no separate OpenId field is needed.
// authType is an open string, not a closed enum, so an unrecognized value is
// passed through verbatim rather than dropped.
func formatSubscriptionAuthority(authType, principalID string) string {
	switch authType {
	case "":
		return ""
	case "user":
		if principalID != "" {
			return "user:" + principalID
		}
		return "user"
	case "app":
		return "app"
	default:
		return authType
	}
}

// sdkLogger forwards every SDK line to bus.log; lifecycle lines also fire notify.
type sdkLogger struct {
	l      *log.Logger
	notify StatusNotifier
}

func (a *sdkLogger) Debug(_ context.Context, _ ...interface{}) {}
func (a *sdkLogger) Info(_ context.Context, args ...interface{}) {
	msg := fmt.Sprint(args...)
	if a.l != nil {
		a.l.Output(2, "[SDK] "+msg)
	}
	a.tryNotify(msg, "")
}
func (a *sdkLogger) Warn(_ context.Context, args ...interface{}) {
	msg := fmt.Sprint(args...)
	if a.l != nil {
		a.l.Output(2, "[SDK WARN] "+msg)
	}
	a.tryNotify(msg, "")
}
func (a *sdkLogger) Error(_ context.Context, args ...interface{}) {
	msg := fmt.Sprint(args...)
	if a.l != nil {
		a.l.Output(2, "[SDK ERROR] "+msg)
	}
	// Errors usually manifest as disconnects; pass msg as detail.
	a.tryNotify(msg, msg)
}

var reconnectAttemptRe = regexp.MustCompile(`reconnect:?\s*(\d+)`)

// tryNotify uses HasPrefix (not Contains): "connected to" matches inside "disconnected to" otherwise.
func (a *sdkLogger) tryNotify(msg, errDetail string) {
	if a.notify == nil {
		return
	}
	lower := strings.ToLower(msg)
	switch {
	case strings.HasPrefix(lower, sdkLogReconnecting):
		detail := ""
		if m := reconnectAttemptRe.FindStringSubmatch(lower); len(m) == 2 {
			detail = "attempt " + m[1]
		}
		a.notify(protocol.SourceStateReconnecting, detail)
	case strings.HasPrefix(lower, sdkLogDisconnected):
		a.notify(protocol.SourceStateDisconnected, errDetail)
	case strings.HasPrefix(lower, sdkLogConnected):
		a.notify(protocol.SourceStateConnected, "")
	}
}

var _ larkcore.Logger = (*sdkLogger)(nil)
