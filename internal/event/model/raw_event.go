// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package model

import (
	"encoding/json"
	"time"
)

// RawEvent: SourceTime is upstream create_time; Timestamp is local source observation time.
type RawEvent struct {
	EventID    string          `json:"event_id"`
	EventType  string          `json:"event_type"`
	SourceTime string          `json:"source_time,omitempty"`
	Payload    json.RawMessage `json:"payload"`
	Timestamp  time.Time       `json:"timestamp"`

	// --- refined-subscription push-envelope fields.
	// Normalized from the WS push envelope's header.subscription block,
	// which is a DIFFERENT shape from the OpenAPI management-side
	// Subscription (target_resource/authority{open_id,union_id,app_id}) —
	// the two must not be mixed. Empty for events with no
	// header.subscription (i.e. non-refined events, or events observed
	// before this normalization existed).
	//
	// RemoteSubscriptionID, Authority, and SubscriptionEventID are copied into
	// protocol.Event fields with the same names. Resource maps to
	// protocol.Event.TargetResource; the different names reflect the source
	// envelope vocabulary versus the consumer-facing IPC vocabulary.

	// RemoteSubscriptionID identifies which remote Subscription delivered
	// this event, from header.subscription.subscription_id.
	RemoteSubscriptionID string `json:"remote_subscription_id,omitempty"`
	// Resource is the target resource this event was delivered for (e.g.
	// "im.message?chat_id=oc_xxx"), from the 通用事件信封 (general event
	// envelope) push key header.subscription.target_resource, verbatim.
	Resource string `json:"resource,omitempty"`
	// Authority is the normalized subscription authority descriptor (e.g.
	// "user:ou_xxx" or "app"), derived from the 通用事件信封 push
	// header.subscription.authority{type, app_id, open_id} — a "user" authority
	// keyed by its open_id, an "app" authority as the bare "app".
	Authority string `json:"authority,omitempty"`
	// SubscriptionEventID is header.subscription.subscription_event_id: the
	// preferred component of the refined dedup key, paired with
	// RemoteSubscriptionID.
	SubscriptionEventID string `json:"subscription_event_id,omitempty"`
}
