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
	// larkeventv1 alias is REQUIRED (mirrors internal/event/subscription_client.go):
	// this package's declared name is ALSO "larkevent" (spec §0.4's documented
	// collision with the core event package aliased above), so it must be
	// aliased distinctly.
	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"
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

	// OnLifecycleEvent is invoked from the 6 typed subscription lifecycle
	// handlers registered in buildDispatcher (spec §5.1) whenever the SDK
	// routes one of the event.subscription.*_v1 meta-events to this source —
	// le is already normalized (see LifecycleEvent's own doc). Same
	// plain-func-over-plain-value reasoning as OnConnReady above: the bus-side
	// bounded lifecycle executor (internal/event/bus/lifecycle.go) this is
	// wired to needs internal/core-adjacent state this SDK-typed-only package
	// must stay out of. nil is tolerated (no lifecycle executor configured).
	OnLifecycleEvent func(ctx context.Context, le LifecycleEvent)
}

// LifecycleEvent is FeishuSource's normalized shape for one of the SDK's 6
// subscription lifecycle meta-events (spec §5.1: activated/updated/
// suspended/expiration_reminder/expired/deleted). Every field is a plain
// primitive (never an SDK pointer type) since this crosses the
// OnLifecycleEvent boundary into internal/event/bus — bus.go already imports
// this package (source.FeishuSource/source.Source/source.All()), so this
// package must never import bus back; defining LifecycleEvent here (rather
// than in bus, which is where the design note originally located it) is what
// keeps that one-way dependency intact. internal/event/bus/lifecycle.go
// re-exports this exact type under its own package via a type alias so bus
// code never has to spell out the "source." package qualifier.
type LifecycleEvent struct {
	EventType string // header.event_type, e.g. "event.subscription.suspended_v1"
	EventID   string // header.event_id

	// RemoteSubscriptionID prefers the event BODY's subscription_id (spec
	// §5.1's payload facts): After.SubscriptionId for updated_v1 (Before is
	// never used), the top-level SubscriptionId for the other 5 (deleted_v1's
	// body carries ONLY this field). "" means the body didn't carry one (e.g.
	// a malformed updated_v1 with no after snapshot) — callers must treat
	// that as undeliverable, never guess a resource.
	RemoteSubscriptionID string

	// State is the body's subscription state (open vocabulary, e.g.
	// "active"/"suspended"/"expired"). deleted_v1's SDK body has no state
	// field at all, so this stays "" for that event type — not a fabricated
	// "deleted" value.
	State string

	// SuspensionCode is body.suspension.code carried VERBATIM (spec §5.4: an
	// open string, never a closed enum — an unrecognized future value must
	// round-trip unchanged, never fail normalization). "" when absent.
	SuspensionCode string

	TargetResource string // body.target_resource; "" when absent (e.g. deleted_v1)

	// Authority is formatted through the SAME vocabulary as
	// formatSubscriptionAuthority (RawEvent.Authority) — "user:<id>" / "app" /
	// passthrough / "" — even though the SOURCE shape differs (this is the
	// OpenAPI management-side Authority{Type,OpenId,UnionId,AppId}, not the
	// push-envelope's Authority{Type,PrincipalID}).
	Authority string

	ExpireTime int64 // body.expire_time, unix seconds; 0 when absent
}

func (s *FeishuSource) Name() string { return "feishu-websocket" }

func (s *FeishuSource) Start(ctx context.Context, eventTypes []string, emit func(*event.RawEvent), notify StatusNotifier) error {
	d := s.buildDispatcher(eventTypes, emit)

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
	// cli.BindUser are read fresh on every invocation and passed straight
	// through: THIS package/closure never memoizes them across calls. (The
	// bus-side identityGate DOES memoize the latest (connID, bindUser) pair
	// it receives via OnConnReady, Task 18 — so its own bindConsumer can
	// (re)bind a specific consumer independently of a fresh ready/reconnect
	// event, e.g. reacting to an activated_v1/suspended_v1 lifecycle event.
	// That memoization lives entirely downstream of this closure, which
	// keeps doing exactly what this comment always said: read fresh, pass
	// through, remember nothing.)
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

// buildDispatcher constructs the EventDispatcher and registers every
// handler: business OnCustomizedEvent handlers first, then the 6
// subscription lifecycle handlers (spec §5.1) — extracted from Start so unit
// tests can exercise registration and routing without a live WS client
// (mirrors buildRawHandler's own extraction below).
func (s *FeishuSource) buildDispatcher(eventTypes []string, emit func(*event.RawEvent)) *dispatcher.EventDispatcher {
	d := dispatcher.NewEventDispatcher("", "")

	rawHandler := s.buildRawHandler(emit)

	businessEventTypes := make(map[string]struct{}, len(eventTypes))
	for _, et := range eventTypes {
		d.OnCustomizedEvent(et, rawHandler)
		businessEventTypes[et] = struct{}{}
	}

	// Lifecycle handlers are bus-internal control plane only (spec §5.1):
	// registering them here makes the SDK route these 6 event types (by
	// header.event_type) to dispatchLifecycleEvent instead of
	// rawHandler/emit — they must NEVER reach Hub.Publish/business consumers.
	s.registerLifecycleHandlers(d, businessEventTypes)

	return d
}

// --- subscription lifecycle handlers (spec §5.1) --------------------------

// The 6 subscription lifecycle meta-event types (SDK facts, spec §0.4/§5.1).
// NO "deactivated" — suspend is "suspended_v1", carrying suspension.code.
const (
	lifecycleEventTypeActivated          = "event.subscription.activated_v1"
	lifecycleEventTypeUpdated            = "event.subscription.updated_v1"
	lifecycleEventTypeSuspended          = "event.subscription.suspended_v1"
	lifecycleEventTypeExpirationReminder = "event.subscription.expiration_reminder_v1"
	lifecycleEventTypeExpired            = "event.subscription.expired_v1"
	lifecycleEventTypeDeleted            = "event.subscription.deleted_v1"
)

// registerLifecycleHandlers registers the 6 typed subscription lifecycle
// handlers on d, BEFORE cli.Start (called from buildDispatcher/Start).
// businessEventTypes is the set already registered via OnCustomizedEvent
// (buildDispatcher's loop just above) — the SDK dispatcher PANICS on any
// duplicate event_type registration (event/dispatcher's generated
// On*/OnCustomizedEvent functions all guard the same
// eventType2EventHandler map), so a collision here (none exist among today's
// business EventKeys) skips ONLY that one lifecycle handler — leaving the
// already-registered business handler in place — rather than crashing the
// whole bus.
func (s *FeishuSource) registerLifecycleHandlers(d *dispatcher.EventDispatcher, businessEventTypes map[string]struct{}) {
	guard := func(eventType string, register func()) {
		if _, collide := businessEventTypes[eventType]; collide {
			if s.Logger != nil {
				s.Logger.Printf("[feishu] WARN: business event type %q collides with a subscription lifecycle event type; skipping lifecycle handler registration (the SDK dispatcher panics on duplicate registration)", eventType)
			}
			return
		}
		register()
	}
	guard(lifecycleEventTypeActivated, func() { d.OnP2SubscriptionActivatedV1(s.handleSubscriptionActivated) })
	guard(lifecycleEventTypeUpdated, func() { d.OnP2SubscriptionUpdatedV1(s.handleSubscriptionUpdated) })
	guard(lifecycleEventTypeSuspended, func() { d.OnP2SubscriptionSuspendedV1(s.handleSubscriptionSuspended) })
	guard(lifecycleEventTypeExpirationReminder, func() { d.OnP2SubscriptionExpirationReminderV1(s.handleSubscriptionExpirationReminder) })
	guard(lifecycleEventTypeExpired, func() { d.OnP2SubscriptionExpiredV1(s.handleSubscriptionExpired) })
	guard(lifecycleEventTypeDeleted, func() { d.OnP2SubscriptionDeletedV1(s.handleSubscriptionDeleted) })
}

// dispatchLifecycleEvent is every handler's common tail: forward le to
// OnLifecycleEvent if configured (nil-tolerant, mirrors the ready closure's
// "if s.OnConnReady != nil" pattern in Start).
func (s *FeishuSource) dispatchLifecycleEvent(ctx context.Context, le LifecycleEvent) {
	if s.OnLifecycleEvent != nil {
		s.OnLifecycleEvent(ctx, le)
	}
}

// headerEventID extracts header.event_id, nil-safe. base is the SDK's
// EventV2Base (package larkevent, i.e. "github.com/.../oapi-sdk-go/v3/event"
// — the SAME package this file aliases larkevent for buildRawHandler above);
// EVERY P2Subscription*V1 struct embeds it. It must be read through this
// EXPLICIT field name (never the promoted e.Header shorthand): each
// P2Subscription*V1 ALSO embeds *larkevent.EventReq, whose OWN Header field
// (an http header map) is a same-depth, same-name, genuinely ambiguous
// selector clash with EventV2Base.Header — e.Header does not compile.
func headerEventID(base *larkevent.EventV2Base) string {
	if base == nil || base.Header == nil {
		return ""
	}
	return base.Header.EventID
}

// formatLifecycleAuthority normalizes the OpenAPI management-side
// Authority{Type,OpenId,UnionId,AppId} (all pointers) through the SAME
// vocabulary formatSubscriptionAuthority already implements for the
// push-envelope's Authority{Type,PrincipalID} shape — reusing it here (rather
// than re-deriving the user/app/passthrough rules) is why that helper takes
// an already-resolved principalID string rather than the push-envelope type
// directly. OpenId is preferred over UnionId when a "user" authority carries
// both (OpenId is this CLI's canonical user identifier elsewhere, e.g.
// cmd/event/subscription's own formatAuthority).
func formatLifecycleAuthority(a *larkeventv1.Authority) string {
	if a == nil || a.Type == nil {
		return ""
	}
	principalID := ""
	switch {
	case a.OpenId != nil && *a.OpenId != "":
		principalID = *a.OpenId
	case a.UnionId != nil && *a.UnionId != "":
		principalID = *a.UnionId
	}
	return formatSubscriptionAuthority(*a.Type, principalID)
}

// normalizeLifecycleEvent builds a LifecycleEvent from the 4 lifecycle event
// types that share an identical (but distinctly-named, SDK-generated) body
// shape: activated/suspended/expiration_reminder/expired — all
// {SubscriptionId,TargetResource,Authority,State,Suspension,ExpireTime,...}.
// updated_v1 (via its After snapshot, a *SubscriptionDetail) reuses this too;
// deleted_v1 does not (its body carries ONLY SubscriptionId).
func normalizeLifecycleEvent(eventType, eventID string, subscriptionID, targetResource *string, authority *larkeventv1.Authority, state *string, suspension *larkeventv1.Suspension, expireTime *int) LifecycleEvent {
	le := LifecycleEvent{EventType: eventType, EventID: eventID}
	if subscriptionID != nil {
		le.RemoteSubscriptionID = *subscriptionID
	}
	if targetResource != nil {
		le.TargetResource = *targetResource
	}
	if state != nil {
		le.State = *state
	}
	le.Authority = formatLifecycleAuthority(authority)
	if suspension != nil && suspension.Code != nil {
		le.SuspensionCode = *suspension.Code
	}
	if expireTime != nil {
		le.ExpireTime = int64(*expireTime)
	}
	return le
}

func (s *FeishuSource) handleSubscriptionActivated(ctx context.Context, e *larkeventv1.P2SubscriptionActivatedV1) error {
	if e == nil || e.Event == nil {
		return nil
	}
	d := e.Event
	s.dispatchLifecycleEvent(ctx, normalizeLifecycleEvent(lifecycleEventTypeActivated, headerEventID(e.EventV2Base),
		d.SubscriptionId, d.TargetResource, d.Authority, d.State, d.Suspension, d.ExpireTime))
	return nil
}

func (s *FeishuSource) handleSubscriptionSuspended(ctx context.Context, e *larkeventv1.P2SubscriptionSuspendedV1) error {
	if e == nil || e.Event == nil {
		return nil
	}
	d := e.Event
	s.dispatchLifecycleEvent(ctx, normalizeLifecycleEvent(lifecycleEventTypeSuspended, headerEventID(e.EventV2Base),
		d.SubscriptionId, d.TargetResource, d.Authority, d.State, d.Suspension, d.ExpireTime))
	return nil
}

func (s *FeishuSource) handleSubscriptionExpirationReminder(ctx context.Context, e *larkeventv1.P2SubscriptionExpirationReminderV1) error {
	if e == nil || e.Event == nil {
		return nil
	}
	d := e.Event
	s.dispatchLifecycleEvent(ctx, normalizeLifecycleEvent(lifecycleEventTypeExpirationReminder, headerEventID(e.EventV2Base),
		d.SubscriptionId, d.TargetResource, d.Authority, d.State, d.Suspension, d.ExpireTime))
	return nil
}

func (s *FeishuSource) handleSubscriptionExpired(ctx context.Context, e *larkeventv1.P2SubscriptionExpiredV1) error {
	if e == nil || e.Event == nil {
		return nil
	}
	d := e.Event
	s.dispatchLifecycleEvent(ctx, normalizeLifecycleEvent(lifecycleEventTypeExpired, headerEventID(e.EventV2Base),
		d.SubscriptionId, d.TargetResource, d.Authority, d.State, d.Suspension, d.ExpireTime))
	return nil
}

// handleSubscriptionUpdated normalizes the AFTER snapshot (spec §5.1: "updated
// → e.Event.After.SubscriptionId"); Before is never used for normalization.
// A nil After (a malformed/unexpected payload) normalizes to a
// RemoteSubscriptionID=="" event — Submit's own validation (spec §5.2) drops
// that, so no special early-return is needed here.
func (s *FeishuSource) handleSubscriptionUpdated(ctx context.Context, e *larkeventv1.P2SubscriptionUpdatedV1) error {
	if e == nil || e.Event == nil {
		return nil
	}
	eventID := headerEventID(e.EventV2Base)
	after := e.Event.After
	if after == nil {
		s.dispatchLifecycleEvent(ctx, LifecycleEvent{EventType: lifecycleEventTypeUpdated, EventID: eventID})
		return nil
	}
	s.dispatchLifecycleEvent(ctx, normalizeLifecycleEvent(lifecycleEventTypeUpdated, eventID,
		after.SubscriptionId, after.TargetResource, after.Authority, after.State, after.Suspension, after.ExpireTime))
	return nil
}

// handleSubscriptionDeleted is body-only (spec §5.1: deleted_v1's SDK body
// carries ONLY subscription_id — no target_resource/authority/state/
// suspension/expire_time to normalize).
func (s *FeishuSource) handleSubscriptionDeleted(ctx context.Context, e *larkeventv1.P2SubscriptionDeletedV1) error {
	if e == nil || e.Event == nil {
		return nil
	}
	le := LifecycleEvent{EventType: lifecycleEventTypeDeleted, EventID: headerEventID(e.EventV2Base)}
	if e.Event.SubscriptionId != nil {
		le.RemoteSubscriptionID = *e.Event.SubscriptionId
	}
	s.dispatchLifecycleEvent(ctx, le)
	return nil
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
