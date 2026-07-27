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
	// this package's declared name is ALSO "larkevent" (a documented
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
	// handlers registered in buildDispatcher whenever the SDK
	// routes one of the event.subscription.*_v1 meta-events to this source —
	// le is already normalized (see LifecycleEvent's own doc). Same
	// plain-func-over-plain-value reasoning as OnConnReady above: the bus-side
	// bounded lifecycle executor (internal/event/bus/lifecycle.go) this is
	// wired to needs internal/core-adjacent state this SDK-typed-only package
	// must stay out of. nil is tolerated (no lifecycle executor configured).
	OnLifecycleEvent func(ctx context.Context, le LifecycleEvent)

	// EncryptKeyProvider supplies per-subscription_id encrypt_keys so the SDK
	// EventDispatcher can transparently decrypt whole-envelope encrypted
	// subscription events. An SDK interface type
	// (larkevent.EncryptKeyProvider) so this SDK-typed-only package needs no
	// bus import — the bus wires its own implementation in (bus.go's
	// startSources). nil is tolerated and is the 100%-unchanged path: without
	// it buildDispatcher never calls WithEncryptKeyProvider, so an encrypted
	// envelope simply fails the existing parse (fail-closed, no plaintext
	// fallback) exactly as before decryption support. The key never crosses back out of
	// the SDK through this field.
	EncryptKeyProvider larkevent.EncryptKeyProvider

	// OnDecryptFailure is invoked (best-effort) when the SDK dispatcher
	// fail-closes on an undecryptable subscription envelope. The undecryptable
	// event is already dropped by the SDK — it
	// never reaches emit/Hub/stdout; this is purely so the bus can count the
	// failure and mark the matched consumer degraded. subscriptionID is parsed
	// from the SDK's stable decrypt-failure log line (the SDK exposes no typed
	// per-message error callback for the WS path); it never carries a key,
	// ciphertext, or plaintext. nil is tolerated (no observability wired).
	OnDecryptFailure func(subscriptionID string)
}

// LifecycleEvent is FeishuSource's normalized shape for one of the SDK's 6
// subscription lifecycle meta-events (activated/updated/
// suspended/expiration_reminder/expired/deleted). Every field is a plain
// primitive (never an SDK pointer type) since this crosses the
// OnLifecycleEvent boundary into internal/event/bus — bus.go already imports
// this package (source.FeishuSource/source.Source/source.All()), so this
// package must never import bus back; defining LifecycleEvent here (rather
// than in bus) is what
// keeps that one-way dependency intact. internal/event/bus/lifecycle.go
// re-exports this exact type under its own package via a type alias so bus
// code never has to spell out the "source." package qualifier.
type LifecycleEvent struct {
	EventType string // header.event_type, e.g. "event.subscription.suspended_v1"
	EventID   string // header.event_id

	// RemoteSubscriptionID prefers the event BODY's subscription_id:
	// After.SubscriptionId for updated_v1 (Before is
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

	// SuspensionCode is body.suspension.code carried VERBATIM (an
	// open string, never a closed enum — an unrecognized future value must
	// round-trip unchanged, never fail normalization). "" when absent.
	SuspensionCode string

	TargetResource string // body.target_resource; "" when absent (e.g. deleted_v1)

	// Authority is formatted through the SAME vocabulary as
	// formatSubscriptionAuthority (RawEvent.Authority) — "user:<id>" / "app" /
	// passthrough / "" — even though the SOURCE shape differs (this is the
	// OpenAPI management-side Authority{Type,OpenId,UnionId,AppId}, not the
	// push envelope's authority{type, app_id, open_id}).
	Authority string

	ExpireTime int64 // body.expire_time, unix seconds; 0 when absent

	// IncludeResourceData is the AFTER snapshot's
	// payload_options.include_resource_data, populated ONLY for updated_v1
	// (handleSubscriptionUpdated) — meaningful ONLY when PayloadOptionsPresent
	// is true. The other 5 lifecycle event types never populate either of
	// these two fields, so both stay at their zero value ("false") for those
	// — bus/lifecycle.go's classifyUpdateCompatibility is the only reader,
	// and it only ever consults these for an updated_v1 event.
	IncludeResourceData bool

	// PayloadOptionsPresent distinguishes "the after snapshot carried a
	// payload_options.include_resource_data value" (true) from "it did not"
	// (false — e.g. a malformed/unexpected payload) — IncludeResourceData's
	// zero value (false) is ALSO what an absent payload_options normalizes
	// to, so this flag is what lets a caller tell a genuine "false" apart
	// from "unknown" rather than silently treating a missing field as a
	// confirmed false.
	PayloadOptionsPresent bool

	// Filter is the AFTER snapshot's server-side event filter, projected into
	// the CLI filter model and populated ONLY for updated_v1
	// (handleSubscriptionUpdated) — meaningful ONLY when FilterPresent is true.
	// nil means the after snapshot carried no filter section.
	Filter *event.Filter

	// FilterPresent distinguishes "the after snapshot carried a filter section"
	// (true) from "it did not" (false). A nil Filter is ALSO what an absent
	// filter normalizes to, so this flag is what lets a caller tell a genuine
	// "no filter" apart from "unknown". Like PayloadOptionsPresent, an
	// updated_v1 that omits the filter stays FilterPresent==false and is
	// resolved authoritatively via a Get, never guessed as a confirmed
	// "no filter"; a Get-projected snapshot always sets it true.
	FilterPresent bool
}

func (s *FeishuSource) Name() string { return "feishu-websocket" }

func (s *FeishuSource) Start(ctx context.Context, eventTypes []string, emit func(*event.RawEvent), notify StatusNotifier) error {
	d := s.buildDispatcher(eventTypes, emit)

	opts := []larkws.ClientOption{larkws.WithEventHandler(d)}
	if s.Domain != "" {
		opts = append(opts, larkws.WithDomain(s.Domain))
	}
	if s.Logger != nil || notify != nil || s.OnDecryptFailure != nil {
		opts = append(opts, larkws.WithLogLevel(larkcore.LogLevelInfo))
		opts = append(opts, larkws.WithLogger(&sdkLogger{l: s.Logger, notify: notify, onDecryptFailure: s.OnDecryptFailure}))
	}

	// var cli up-front so the ready closure can capture it before NewClient
	// returns (WithOnReady/WithOnReconnected only ever fire AFTER Start(),
	// by which time cli is assigned) — cli.Connection().ConnectionID and
	// cli.BindUser are read fresh on every invocation and passed straight
	// through: THIS package/closure never memoizes them across calls. (The
	// bus-side identityGate DOES memoize the latest (connID, bindUser) pair
	// it receives via OnConnReady — so its own bindConsumer can
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
// subscription lifecycle handlers, extracted from Start so unit
// tests can exercise registration and routing without a live WS client
// (mirrors buildRawHandler's own extraction below).
func (s *FeishuSource) buildDispatcher(eventTypes []string, emit func(*event.RawEvent)) *dispatcher.EventDispatcher {
	d := dispatcher.NewEventDispatcher("", "")

	// Opt into per-subscription decryption. Leaving the
	// provider unset keeps the non-encrypted path 100% unchanged (the SDK only
	// consults it for envelopes carrying a top-level encrypt_info).
	if s.EncryptKeyProvider != nil {
		d.WithEncryptKeyProvider(s.EncryptKeyProvider)
	}

	rawHandler := s.buildRawHandler(emit)

	businessEventTypes := make(map[string]struct{}, len(eventTypes))
	for _, et := range eventTypes {
		d.OnCustomizedEvent(et, rawHandler)
		businessEventTypes[et] = struct{}{}
	}

	// Lifecycle handlers are bus-internal control plane only:
	// registering them here makes the SDK route these 6 event types (by
	// header.event_type) to dispatchLifecycleEvent instead of
	// rawHandler/emit — they must NEVER reach Hub.Publish/business consumers.
	s.registerLifecycleHandlers(d, businessEventTypes)

	return d
}

// --- subscription lifecycle handlers ---

// The 6 subscription lifecycle meta-event types (SDK facts).
// NO "deactivated" — suspend is "suspended_v1", carrying suspension.code.
const (
	lifecycleEventTypeActivated          = "event.subscription.activated_v1"
	lifecycleEventTypeUpdated            = "event.subscription.updated_v1"
	lifecycleEventTypeSuspended          = "event.subscription.suspended_v1"
	lifecycleEventTypeExpirationReminder = "event.subscription.expiration_reminder_v1"
	lifecycleEventTypeExpired            = "event.subscription.expired_v1"
	lifecycleEventTypeDeleted            = "event.subscription.deleted_v1"
)

// Exported aliases of the 6 constants above — the single source of truth
// internal/event/bus/lifecycle's own event-type constants are derived from,
// so the two packages can never drift apart. This package's own code keeps
// using the unexported names above unchanged; these exist purely for the
// other package's import.
const (
	LifecycleEventTypeActivated          = lifecycleEventTypeActivated
	LifecycleEventTypeUpdated            = lifecycleEventTypeUpdated
	LifecycleEventTypeSuspended          = lifecycleEventTypeSuspended
	LifecycleEventTypeExpirationReminder = lifecycleEventTypeExpirationReminder
	LifecycleEventTypeExpired            = lifecycleEventTypeExpired
	LifecycleEventTypeDeleted            = lifecycleEventTypeDeleted
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

// FormatLifecycleAuthority normalizes the OpenAPI management-side
// Authority{Type,OpenId,UnionId,AppId} (all pointers) through the SAME
// vocabulary formatSubscriptionAuthority already implements for the push
// envelope's structured authority{type, app_id, open_id} shape — reusing it
// here (rather than re-deriving the user/app/passthrough rules) is why that
// helper takes an already-resolved user-id string rather than a whole
// authority struct. OpenId is preferred over UnionId when a "user" authority carries
// both (OpenId is this CLI's canonical user identifier elsewhere, e.g.
// cmd/event/subscription's own formatAuthority).
//
// Exported so bus/lifecycle's own action code (reconcileWithGet's Get-response
// projection) can normalize a SubscriptionDetail.Authority through the exact
// same vocabulary a lifecycle event's own After.Authority already uses,
// rather than re-deriving it.
func FormatLifecycleAuthority(a *larkeventv1.Authority) string {
	if a == nil || a.Type == nil {
		return ""
	}
	principalID := ""
	if a.OpenId != nil {
		principalID = *a.OpenId
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
	le.Authority = FormatLifecycleAuthority(authority)
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

// handleSubscriptionUpdated normalizes the AFTER snapshot ("updated
// → e.Event.After.SubscriptionId"); Before is never used for normalization.
// A nil After (a malformed/unexpected payload) normalizes to a
// RemoteSubscriptionID=="" event — Submit's own validation drops
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
	le := normalizeLifecycleEvent(lifecycleEventTypeUpdated, eventID,
		after.SubscriptionId, after.TargetResource, after.Authority, after.State, after.Suspension, after.ExpireTime)
	// The bus-side compatibility check needs payload_options.include_resource_data
	// alongside target_resource and authority so it can compare the remote
	// subscription snapshot against the local listening intent field by field.
	// PayloadOptionsPresent stays false (and
	// IncludeResourceData stays its zero value) when the after snapshot
	// didn't carry a payload_options.include_resource_data at all — never
	// guessed as a confirmed "false".
	if after.PayloadOptions != nil && after.PayloadOptions.IncludeResourceData != nil {
		le.PayloadOptionsPresent = true
		le.IncludeResourceData = *after.PayloadOptions.IncludeResourceData
	}
	// The filter is applied server-side, so an updated_v1 that changes it away
	// from what this consumer asked for is a compatibility break too. Capture it
	// only when the after snapshot actually carried a filter section; an absent
	// one stays FilterPresent==false (resolved authoritatively via a Get later),
	// never guessed as a confirmed "no filter".
	if after.Filter != nil {
		le.Filter = event.FilterFromSDK(after.Filter)
		le.FilterPresent = true
	}
	s.dispatchLifecycleEvent(ctx, le)
	return nil
}

// handleSubscriptionDeleted is body-only (deleted_v1's SDK body
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
				// Subscription follows the platform's 通用事件信封 (general
				// event envelope) push spec: header.subscription carries
				// target_resource plus a structured authority{type, app_id,
				// open_id} — an "app" authority carries app_id, a "user"
				// authority carries open_id. This is NOT the OpenAPI
				// management-side Subscription shape (whose authority also has
				// union_id) — do not conflate the two. Absent on non-refined
				// events, in which case every subfield below zero-values to "".
				Subscription struct {
					SubscriptionID string `json:"subscription_id"`
					TargetResource string `json:"target_resource"`
					Authority      struct {
						Type   string `json:"type"`
						AppID  string `json:"app_id"`
						OpenID string `json:"open_id"`
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
			Resource:             envelope.Header.Subscription.TargetResource,
			Authority: formatSubscriptionAuthority(
				envelope.Header.Subscription.Authority.Type,
				envelope.Header.Subscription.Authority.OpenID,
			),
			SubscriptionEventID: envelope.Header.Subscription.SubscriptionEventID,
		})
		return nil
	}
}

// formatSubscriptionAuthority normalizes a subscription authority into this
// package's compact identity vocabulary. Mirrors
// cmd/event/subscription/subscription.go's formatAuthority (same "user:<id>" /
// "user" / "app" / passthrough rules). The push envelope's structured
// authority{type, app_id, open_id} maps in as (type, open_id): a "user"
// authority is keyed by its open_id, while an "app" authority's app_id is NOT
// part of this vocabulary — a bus is per-app, so the bare "app" already means
// "this app". authType is an open string, not a closed enum, so an
// unrecognized value is passed through verbatim rather than dropped.
func formatSubscriptionAuthority(authType, openID string) string {
	switch authType {
	case "":
		return ""
	case "user":
		if openID != "" {
			return "user:" + openID
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
	// onDecryptFailure is called with the subscription_id
	// extracted from a fail-closed decrypt-failure SDK Error line. nil = not wired.
	onDecryptFailure func(subscriptionID string)
}

// subscriptionDecryptFailureRe matches the SDK's stable whole-envelope
// decrypt-failure error (event/dispatcher/dispatcher.go's
// decryptSubscriptionEnvelope: "subscription event decryption failed
// (subscription_id=%s): ...") and captures the subscription_id. This couples to
// the pinned SDK's error string — the only surface the WS path exposes for a
// per-message decrypt failure (Do returns the error; the ws client logs it via
// this logger). The capture stops at ')' so it never swallows the ": <detail>"
// tail (which carries no key, but also no useful routing info).
var subscriptionDecryptFailureRe = regexp.MustCompile(`subscription event decryption failed \(subscription_id=([^)]*)\)`)

// decryptFailureLogClass is the fixed classification used when a decrypt-failure
// SDK error line is rewritten before it reaches bus.log. The log must not carry
// the crypto/padding detail in the SDK's raw error tail, e.g. "illegal base64
// data", "cipher too short", or "ciphertext is not a multiple of the block
// size", because those details could become a decryption oracle. The token
// mirrors Conn.decryptStateFailed's "decrypt_failed" state.
const decryptFailureLogClass = "decrypt_failed"

// redactDecryptFailureLine detects a decrypt-failure SDK Error line
// (subscriptionDecryptFailureRe) and truncates it right after the
// "(subscription_id=...)" it already carries, discarding everything from
// there to the end of the line — event/dispatcher/dispatcher.go's
// decryptSubscriptionEnvelope always appends the raw crypto/padding detail as
// the LAST component of the wrapped ws-client log line ("...: <detail>"), so
// this is the ONLY place that detail appears on this path — and replacing it
// with a fixed classification instead. Any harmless prefix context the ws
// client adds (message_type/message_id/trace_id, etc.) is kept verbatim. A
// line that is not a decrypt failure is returned byte-for-byte unchanged.
func redactDecryptFailureLine(msg string) string {
	loc := subscriptionDecryptFailureRe.FindStringIndex(msg)
	if loc == nil {
		return msg
	}
	return msg[:loc[1]] + ": classification=" + decryptFailureLogClass
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
	// A decrypt-failure line's raw SDK tail (base64/cipher/padding detail)
	// must never reach bus.log. Redact before logging or notifying.
	// tryDecryptFailure below only ever extracts a
	// subscription_id from the ORIGINAL msg and never logs/forwards the line
	// itself, so it intentionally keeps using msg, not the redacted copy.
	logLine := redactDecryptFailureLine(msg)
	if a.l != nil {
		a.l.Output(2, "[SDK ERROR] "+logLine)
	}
	// A fail-closed subscription decrypt failure surfaces
	// here (the WS client logs the dispatcher's Do error via this logger).
	// Detect it and hand the subscription_id to the bus for counting/degrade.
	a.tryDecryptFailure(msg)
	// Errors usually manifest as disconnects; pass the redacted line as
	// detail — a decrypt-failure line never matches any of tryNotify's own
	// prefixes below (sdk_log_patterns.go), so this is a no-op for it today;
	// using the redacted copy here anyway keeps this call site fail-closed
	// regardless of future drift.
	a.tryNotify(logLine, logLine)
}

// tryDecryptFailure fires onDecryptFailure with the subscription_id parsed from
// a fail-closed decrypt-failure SDK Error line. Best-effort:
// no callback wired, or a line that is not a decrypt failure, is a no-op.
func (a *sdkLogger) tryDecryptFailure(msg string) {
	if a.onDecryptFailure == nil {
		return
	}
	if m := subscriptionDecryptFailureRe.FindStringSubmatch(msg); len(m) == 2 {
		a.onDecryptFailure(m[1])
	}
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
