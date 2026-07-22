// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package protocol

import "encoding/json"

const (
	MsgTypeHello            = "hello"
	MsgTypeHelloAck         = "hello_ack"
	MsgTypeEvent            = "event"
	MsgTypeBye              = "bye"
	MsgTypePreShutdownCheck = "pre_shutdown_check"
	MsgTypePreShutdownAck   = "pre_shutdown_ack"
	MsgTypeStatusQuery      = "status_query"
	MsgTypeStatusResponse   = "status_response"
	MsgTypeShutdown         = "shutdown"
	MsgTypeSourceStatus     = "source_status"
)

const (
	SourceStateConnecting   = "connecting"
	SourceStateConnected    = "connected"
	SourceStateDisconnected = "disconnected"
	SourceStateReconnecting = "reconnecting"
)

// SourceStatus is best-effort: hub drops it when consumer's send channel is full.
type SourceStatus struct {
	Type   string `json:"type"`
	Source string `json:"source"`
	State  string `json:"state"`
	Detail string `json:"detail,omitempty"`
}

type Hello struct {
	Type           string   `json:"type"`
	PID            int      `json:"pid"`
	EventKey       string   `json:"event_key"`
	EventTypes     []string `json:"event_types"`
	Version        string   `json:"version"`
	SubscriptionID string   `json:"subscription_id,omitempty"` // empty = fallback to EventKey on bus side

	// --- v2 additive fields (spec §4.2). All optional/omitempty so a v1
	// Hello (missing every field below) still decodes unchanged. A bus
	// seeing them absent MUST treat the sender as a plain (non-refined) Key
	// registration — no silent downgrade, no new requirement placed on it.
	// Populating/consuming these is out of scope here (later Phase-C tasks);
	// this struct only carries them over the wire.

	// ConsumerScopeID groups fan-out for a refined-subscription consumer.
	// Computed client-side from base key + canonical refined key + authority
	// type + app_id + user_open_id (spec §4.3). Empty for legacy consumers.
	ConsumerScopeID string `json:"consumer_scope_id,omitempty"`

	// RemoteSubscriptionID is the OpenAPI Subscription primary key (e.g.
	// "sub_xxx") this consumer is bound to. This is a DIFFERENT id from
	// SubscriptionID above: SubscriptionID keeps its frozen meaning as the
	// local parameter fingerprint (see internal/event/consume/fingerprint.go)
	// and must never be repurposed to carry this remote id.
	RemoteSubscriptionID string `json:"remote_subscription_id,omitempty"`

	// Identity is the resolved caller identity for this consumer ("user" or
	// "bot"). Kept as a plain string rather than core.Identity to keep this
	// package SDK/domain independent. Consumed by the bus-side owner/current
	// identity gate (spec §4.4); this field only carries the value.
	Identity string `json:"identity,omitempty"`

	// Profile is the active multi-app config profile name
	// (core.AppConfig.ProfileName()) at Hello time, recorded as part of the
	// owner identity fixed at registration (spec §4.4).
	Profile string `json:"profile,omitempty"`

	// UserOpenID is the open_id of the Identity above (empty when Identity
	// is "bot" or unset).
	UserOpenID string `json:"user_open_id,omitempty"`

	// Capabilities lists protocol/feature markers this Hello's sender
	// understands, e.g. "hello_v2". This is the chosen capability/version
	// marker for v2 — a slice instead of a single "v2" bool — so future
	// capabilities can be introduced without another wire bump; it also
	// mirrors StatusResponse.Capabilities below for a consistent negotiation
	// shape on both frame types. An absent/empty Capabilities is
	// indistinguishable from a v1 sender, which is the desired fallback.
	Capabilities []string `json:"capabilities,omitempty"`
}

type HelloAck struct {
	Type         string `json:"type"`
	BusVersion   string `json:"bus_version"`
	FirstForKey  bool   `json:"first_for_key"`
	Rejected     bool   `json:"rejected,omitempty"`
	RejectReason string `json:"reject_reason,omitempty"`
}

// Event: Seq is per-conn monotonic; gaps signal bus drop-oldest backpressure loss.
type Event struct {
	Type       string          `json:"type"`
	EventType  string          `json:"event_type"`
	EventID    string          `json:"event_id,omitempty"`
	SourceTime string          `json:"source_time,omitempty"` // ms-precision unix timestamp, stringified
	Seq        uint64          `json:"seq,omitempty"`
	Payload    json.RawMessage `json:"payload"`

	// --- v2 additive fields (spec §4.2/§4.3). Optional; populated only when
	// this Event is fanned out for a refined-subscription consumer.
	// Normalized from the push-envelope's EventHeader.Subscription — a
	// DIFFERENT shape from the OpenAPI management-side Subscription
	// (target_resource/authority{open_id,union_id,app_id}). Normalization
	// happens upstream (internal/event); this package only carries the
	// result, it does not compute it.

	// RemoteSubscriptionID identifies which remote Subscription delivered
	// this event, normalized from header.subscription.SubscriptionID. Same
	// id space as Hello.RemoteSubscriptionID above.
	RemoteSubscriptionID string `json:"remote_subscription_id,omitempty"`

	// TargetResource is the normalized resource this event was delivered
	// for (e.g. "im.message?chat_id=oc_xxx"), derived from the push
	// envelope's Resource.
	TargetResource string `json:"target_resource,omitempty"`

	// Authority is the normalized subscription authority descriptor (e.g.
	// "user:ou_xxx" or "app"), derived from
	// header.subscription.Authority{Type, PrincipalID}.
	Authority string `json:"authority,omitempty"`

	// SubscriptionEventID is header.subscription.SubscriptionEventID: the
	// preferred component of the refined dedup key, paired with
	// RemoteSubscriptionID (spec §4.3 dedup priority ①).
	SubscriptionEventID string `json:"subscription_event_id,omitempty"`
}

type Bye struct {
	Type string `json:"type"`
}

// PreShutdownCheck atomically reserves the cleanup lock for (EventKey, SubscriptionID).
type PreShutdownCheck struct {
	Type           string `json:"type"`
	EventKey       string `json:"event_key"`
	SubscriptionID string `json:"subscription_id,omitempty"` // empty = fallback to EventKey
}

type PreShutdownAck struct {
	Type       string `json:"type"`
	LastForKey bool   `json:"last_for_key"`
}

type StatusQuery struct {
	Type string `json:"type"`
}

type ConsumerInfo struct {
	PID            int    `json:"pid"`
	EventKey       string `json:"event_key"`
	SubscriptionID string `json:"subscription_id,omitempty"`
	Received       int64  `json:"received"`
	Dropped        int64  `json:"dropped"`
}

type StatusResponse struct {
	Type        string         `json:"type"`
	PID         int            `json:"pid"`
	UptimeSec   int            `json:"uptime_sec"`
	ActiveConns int            `json:"active_conns"`
	Consumers   []ConsumerInfo `json:"consumers"`

	// --- v2 additive fields (spec §4.2). Optional; let a future
	// ProbeBusEligibility (Task 15) read bus capability over the existing
	// status_query/status_response IPC without a new message type. An old
	// (pre-Phase-C) bus omits all three of these — their absence is itself
	// the incompatibility signal a prober checks for.

	// ProtocolVersion is the bus's IPC protocol marker (e.g. "v2"). Distinct
	// from Hello.Version, which stays frozen at "v1" and unvalidated.
	ProtocolVersion string `json:"protocol_version,omitempty"`

	// Capabilities lists bus feature flags, e.g. "refined_routing",
	// "hello_v2". Mirrors Hello.Capabilities's shape (a slice, not a single
	// bool) for the same forward-extensibility reason. Absent/empty on an
	// old bus reads as neither capability present.
	Capabilities []string `json:"capabilities,omitempty"`

	// RegisteredEventTypes lists event types currently registered on the
	// bus. Kept as a plain list rather than a hash: easier to read directly
	// in `event status --json` output and needs no hashing scheme agreed on
	// upfront; can be revisited as a hash if the list becomes unwieldy.
	RegisteredEventTypes []string `json:"registered_event_types,omitempty"`
}

type Shutdown struct {
	Type string `json:"type"`
}

func NewHello(pid int, eventKey string, eventTypes []string, version string, subscriptionID string) *Hello {
	return &Hello{
		Type:           MsgTypeHello,
		PID:            pid,
		EventKey:       eventKey,
		EventTypes:     eventTypes,
		Version:        version,
		SubscriptionID: subscriptionID,
	}
}

func NewHelloAck(busVersion string, firstForKey bool) *HelloAck {
	return &HelloAck{
		Type:        MsgTypeHelloAck,
		BusVersion:  busVersion,
		FirstForKey: firstForKey,
	}
}

// NewHelloAckRejected builds a hello_ack that tells the consumer the bus refused
// registration (e.g. a SingleConsumer EventKey already has a running consumer).
func NewHelloAckRejected(busVersion, reason string) *HelloAck {
	return &HelloAck{
		Type:         MsgTypeHelloAck,
		BusVersion:   busVersion,
		Rejected:     true,
		RejectReason: reason,
	}
}

func NewEvent(eventType, eventID, sourceTime string, seq uint64, payload json.RawMessage) *Event {
	return &Event{
		Type:       MsgTypeEvent,
		EventType:  eventType,
		EventID:    eventID,
		SourceTime: sourceTime,
		Seq:        seq,
		Payload:    payload,
	}
}

func NewPreShutdownCheck(eventKey, subscriptionID string) *PreShutdownCheck {
	return &PreShutdownCheck{Type: MsgTypePreShutdownCheck, EventKey: eventKey, SubscriptionID: subscriptionID}
}

func NewPreShutdownAck(lastForKey bool) *PreShutdownAck {
	return &PreShutdownAck{Type: MsgTypePreShutdownAck, LastForKey: lastForKey}
}

func NewStatusQuery() *StatusQuery {
	return &StatusQuery{Type: MsgTypeStatusQuery}
}

func NewStatusResponse(pid int, uptimeSec int, activeConns int, consumers []ConsumerInfo) *StatusResponse {
	return &StatusResponse{
		Type:        MsgTypeStatusResponse,
		PID:         pid,
		UptimeSec:   uptimeSec,
		ActiveConns: activeConns,
		Consumers:   consumers,
	}
}

func NewShutdown() *Shutdown { return &Shutdown{Type: MsgTypeShutdown} }

func NewSourceStatus(source, state, detail string) *SourceStatus {
	return &SourceStatus{
		Type:   MsgTypeSourceStatus,
		Source: source,
		State:  state,
		Detail: detail,
	}
}
