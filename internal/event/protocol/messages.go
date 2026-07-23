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

// Bus IPC protocol version + capability markers. Defined once,
// here, so every side that needs to agree on them — the bus, which
// advertises them in StatusResponse (internal/event/bus's
// handleStatusQuery), and a later prober (ProbeBusEligibility)
// that reads them back over status_query/status_response — references the
// SAME identifiers instead of each hand-rolling matching string literals
// that could silently drift out of sync.
const (
	// ProtocolVersionV2 is the bus's IPC protocol marker once it advertises
	// v2 status fields (ProtocolVersion/Capabilities/RegisteredEventTypes)
	// and understands a v2 Hello. An old (pre-v2) bus never sets
	// StatusResponse.ProtocolVersion at all — its ABSENCE, not a mismatched
	// value, is the incompatibility signal a prober checks for.
	ProtocolVersionV2 = "v2"

	// CapabilityRefinedRouting marks that this bus's Hub can route by
	// remote_subscription_id (the dual-index routing matrix), not
	// just by event_type.
	CapabilityRefinedRouting = "refined_routing"

	// CapabilityHelloV2 marks that this bus understands (and expects) the
	// v2 Hello fields (ConsumerScopeID/RemoteSubscriptionID/Identity/
	// Profile/UserOpenID) — mirrors Hello.Capabilities's own "hello_v2"
	// marker below, so a status_response and a hello frame use the
	// identical capability name for the same concept.
	CapabilityHelloV2 = "hello_v2"
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

	// --- v2 additive fields. All optional/omitempty so a v1
	// Hello (missing every field below) still decodes unchanged. A bus
	// seeing them absent MUST treat the sender as a plain (non-refined) Key
	// registration — no silent downgrade, no new requirement placed on it.
	// Populating/consuming these is out of scope here (later work);
	// this struct only carries them over the wire.

	// ConsumerScopeID groups fan-out for a refined-subscription consumer.
	// Computed client-side from base key + canonical refined key + authority
	// type + app_id + user_open_id. Empty for legacy consumers.
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
	// identity gate; this field only carries the value.
	Identity string `json:"identity,omitempty"`

	// Profile is the active multi-app config profile name
	// (core.AppConfig.ProfileName()) at Hello time, recorded as part of the
	// owner identity fixed at registration.
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

	// --- v2 additive fields. Optional; populated only when
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
	// RemoteSubscriptionID (dedup priority ①).
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

	// --- refined-status additive fields. All optional/omitempty
	// so an older consumer entry (only the fields above set) marshals
	// to exactly the old wire shape — this is what
	// TestDecode_OldStatusResponse_BackwardCompat pins. `event status`
	// (cmd/event/status.go) is the sole consumer of these; Hub.Consumers()
	// (internal/event/bus/hub.go) is the sole producer for the bus-known
	// subset — see each field's own comment for exactly which side sets it.

	// RefinedSubscription is true when this consumer is bound to a remote
	// Subscription (RemoteSubscriptionID != ""), i.e. routed by the
	// dual-index matrix rather than by EventTypes() alone. A legacy consumer
	// is always false here.
	RefinedSubscription bool `json:"refined_subscription,omitempty"`

	// RemoteSubscriptionID is the same id as Hello.RemoteSubscriptionID /
	// Event.RemoteSubscriptionID above (the OpenAPI Subscription primary
	// key) — "" for a legacy consumer.
	RemoteSubscriptionID string `json:"remote_subscription_id,omitempty"`

	// OwnerIdentity/OwnerAppID/OwnerUserOpenID are the owner identity fixed
	// at this consumer's registration (Hello.Identity/Profile/
	// UserOpenID + the bus's own AppID) — owner_app_id + owner_user_open_id
	// is the ONLY authoritative comparison key against a freshly-resolved
	// "current" identity; UAT is never part of this. OwnerAppID is set for
	// EVERY consumer (bot, user, or legacy) once registered
	// against a bus — it is simply that bus's own AppID, so its
	// presence alone is not a "refined" or "user" signal. OwnerUserOpenID
	//=="" is the discriminator for "bot or legacy registration"
	// (mirrors Subscriber.OwnerUserOpenID's own doc): only a non-empty
	// OwnerUserOpenID makes current_profile_match (computed by status.go,
	// NOT stored here — the bus does not know the querying profile)
	// meaningful at all.
	OwnerIdentity   string `json:"owner_identity,omitempty"`
	OwnerAppID      string `json:"owner_app_id,omitempty"`
	OwnerUserOpenID string `json:"owner_user_open_id,omitempty"`

	// StaleIdentity/DegradedReason mirror Conn's identical-named accessors
	// (internal/event/bus/conn.go) as of the last bus-side identity-gate
	// evaluation. ADVISORY ONLY: no liveness signal exists and status is a
	// single-shot query, so a live, actively-receiving consumer can still
	// carry a stale flag from an earlier profile switch that was never
	// cleared — a display MUST NOT treat these as
	// "this consumer is dead", only as informational.
	StaleIdentity  bool   `json:"stale_identity,omitempty"`
	DegradedReason string `json:"degraded_reason,omitempty"`

	// RemoteState is a short summary of this consumer's remote Subscription
	// state. Two independent producers, neither the bus's live Publish path:
	// (a) status.go's weak, optional remote supplement sets it
	// from a live SubscriptionClient.Get when every precondition holds — the
	// only producer in that path; (b) the bus itself ALSO
	// remembers the last subscription lifecycle meta-event it received for
	// this consumer's remote Subscription (internal/event/bus/lifecycle.go's
	// executor, via Conn.SetLifecycleSummary/Hub.Consumers()) and can take
	// the real per-event action
	// rather than only recording a summary. Empty until
	// either producer runs.
	RemoteState string `json:"remote_state,omitempty"`

	// RemoteSubscription carries the raw remote Subscription snapshot from
	// status.go's remote supplement — nil unless that weak read
	// actually ran and succeeded for this consumer (current app + resolvable
	// identity + valid unrefreshed token + held event:subscription:read
	// scope); a nil value always means "local-only for this consumer", never
	// "fetched and empty".
	RemoteSubscription *RemoteSubscriptionInfo `json:"remote_subscription,omitempty"`

	// LastLifecycleEvent records the most recent typed lifecycle event
	// (Activated/Updated/Suspended/ExpirationReminder/Expired/Deleted)
	// the bus observed for this consumer's remote Subscription.
	// POPULATED by Hub.Consumers() (from Conn.LastLifecycleEvent()
	// — the lifecycle executor's action writes it on every processed event);
	// "" only means "no lifecycle event observed yet for this consumer", not
	// "not implemented".
	LastLifecycleEvent string `json:"last_lifecycle_event,omitempty"`

	// --- lifecycle ACTION fields. All optional/
	// omitempty so an older consumer entry (only the fields above set)
	// marshals to exactly the old wire shape. Populated by Hub.Consumers()
	// from the identically-named Conn getters (internal/event/bus/conn.go),
	// which internal/event/bus/lifecycle.go's subscriptionLifecycleAction
	// writes to as it processes each lifecycle event — see that type's own
	// doc for exactly when each field changes.

	// SuspensionReason is the most recent suspended_v1's suspension.code,
	// carried verbatim (an open string, never a closed enum) —
	// "" once cleared by a later activated_v1, or if never suspended.
	SuspensionReason string `json:"suspension_reason,omitempty"`

	// LastAction is the most recent remote SubscriptionClient call
	// subscriptionLifecycleAction attempted for this consumer: "reactivate" /
	// "renew" / "get" (at most one such call per event, no
	// retry"). "" means no such call has been attempted yet.
	LastAction string `json:"last_action,omitempty"`

	// LastActionError is LastAction's classified failure reason ("" =
	// succeeded, or LastAction itself is ""). Reuses typed error
	// classification (errs.Problem's Category/Subtype, with permission
	// failures normalized to "missing_scopes") rather than a new private
	// error code.
	LastActionError string `json:"last_action_error,omitempty"`

	// NextAction is a short, stable hint for what an operator/AI should do
	// next while this consumer is degraded (the recovery command
	// is uniformly "reactivate", never "reactive"/"resume"). "" means no
	// outstanding recommendation.
	NextAction string `json:"next_action,omitempty"`

	// --- decryption observability. All optional/omitempty so an older
	// consumer entry
	// marshals to exactly the old wire shape (TestDecode_OldStatusResponse_
	// BackwardCompat). Populated by Hub.Consumers() from Conn's decrypt
	// getters. A key, ciphertext, or decrypted plaintext NEVER appears here —
	// only a short classification, a count, and a timestamp.

	// DecryptState is this consumer's most recent decryption status:
	// "decrypted" (a usable key is available), "decrypt_key_unavailable" (no
	// key: identity mismatch / missing event:encrypt_key:read / GetEncryptKey
	// failed), or "decrypt_failed" (key held but the SDK could not decrypt).
	// "" for a plaintext (non-encrypted) subscription.
	DecryptState string `json:"decrypt_state,omitempty"`

	// LastDecryptError summarizes the most recent decrypt FAILURE (class,
	// running count, time). nil when none has occurred.
	LastDecryptError *DecryptError `json:"last_decrypt_error,omitempty"`

	// ResourceData is a display rollup of resource-data availability derived
	// from DecryptState: "decrypted" (flowing) or "unavailable" (key
	// unavailable, or decryption failing). "" for a plaintext subscription
	// (no resource data at all).
	ResourceData string `json:"resource_data,omitempty"`
}

// DecryptError is ConsumerInfo.LastDecryptError's wire shape: a
// short classification, a running count, and a timestamp — NEVER a key,
// ciphertext, decrypted plaintext, or raw SDK error (that would risk an
// oracle). All omitempty so an absent error contributes nothing.
type DecryptError struct {
	Class string `json:"class,omitempty"`
	Count int64  `json:"count,omitempty"`
	Time  string `json:"time,omitempty"`
}

// RemoteSubscriptionInfo is the CLI-facing snapshot of one remote
// Subscription's live state (the `remote_subscription{state,
// expire_time,include_resource_data}`), as returned by status.go's weak
// remote supplement (internal/event/subscription_client.go's
// SubscriptionClient.Get). Deliberately minimal — only those three fields,
// plus SuspensionCode (the
// one extra piece of the ALREADY-fetched response needed to surface a
// grounded degraded advisory verbatim, with no new remote call) — rather
// than reusing cmd/event/subscription's own richer subscriptionRow/
// remoteState shapes, which live in a sibling CLI package this
// SDK-independent protocol package must not import.
type RemoteSubscriptionInfo struct {
	State               string `json:"state,omitempty"`
	ExpireTime          int64  `json:"expire_time,omitempty"` // unix seconds; 0 = unknown
	IncludeResourceData bool   `json:"include_resource_data,omitempty"`

	// SuspensionCode is the remote Subscription's suspension code (SDK
	// service/event/v1/model.go's Suspension.Code), carried verbatim from
	// status.go's remote supplement. Meaningful ONLY when State=="suspended"
	// (the SDK's own doc says it is returned only when
	// state=suspended); "" for every other state, including a suspended
	// state whose response happened to omit the Suspension object. This
	// package does not interpret the code, it only carries it — status.go's
	// remoteDegradedAdvisory is the sole consumer.
	SuspensionCode string `json:"suspension_code,omitempty"`
}

type StatusResponse struct {
	Type        string         `json:"type"`
	PID         int            `json:"pid"`
	UptimeSec   int            `json:"uptime_sec"`
	ActiveConns int            `json:"active_conns"`
	Consumers   []ConsumerInfo `json:"consumers"`

	// --- v2 additive fields. Optional; let a future
	// ProbeBusEligibility read bus capability over the existing
	// status_query/status_response IPC without a new message type. An old
	// (pre-v2) bus omits all three of these — their absence is itself
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
