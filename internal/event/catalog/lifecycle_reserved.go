// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package catalog

import "sort"

// reservedLifecycleEventTypes are the six subscription lifecycle meta-event
// types the platform reserves for its own control-plane pushes
// (activated/updated/suspended/expiration_reminder/expired/deleted). The event
// source layer registers one typed dispatcher handler per type; if a business
// EventKey declared one of these as its EventType, the SDK dispatcher would
// panic on the duplicate registration, so the source layer skips that lifecycle
// handler — silently losing the lifecycle signal for that key. Validate rejects
// such a collision at build so it can never ship.
//
// These mirror internal/event/source's exported LifecycleEventType* constants
// (the SDK's source of truth). catalog is SDK-free and must not import source
// (that would cycle: source -> event -> catalog), so the strings are restated
// here; a drift-guard test in internal/event asserts the two lists stay equal.
var reservedLifecycleEventTypes = map[string]bool{
	"event.subscription.activated_v1":           true,
	"event.subscription.updated_v1":             true,
	"event.subscription.suspended_v1":           true,
	"event.subscription.expiration_reminder_v1": true,
	"event.subscription.expired_v1":             true,
	"event.subscription.deleted_v1":             true,
}

// IsReservedLifecycleEventType reports whether eventType is one of the reserved
// subscription lifecycle meta-event types a business EventKey must not use as
// its EventType.
func IsReservedLifecycleEventType(eventType string) bool {
	return reservedLifecycleEventTypes[eventType]
}

// ReservedLifecycleEventTypes returns the reserved subscription lifecycle
// meta-event types, sorted. Exported so a cross-package guard can assert this
// set stays equal to the source layer's own lifecycle constants.
func ReservedLifecycleEventTypes() []string {
	out := make([]string, 0, len(reservedLifecycleEventTypes))
	for et := range reservedLifecycleEventTypes {
		out = append(out, et)
	}
	sort.Strings(out)
	return out
}
