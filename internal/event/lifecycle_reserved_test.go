// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/larksuite/cli/internal/event/catalog"
	"github.com/larksuite/cli/internal/event/source"
)

// TestReservedLifecycleEventTypes_MatchSource is the drift guard for fail-fast
// #5c. catalog restates the six subscription lifecycle event types (it is
// SDK-free and cannot import source without a cycle), so this asserts its
// reserved set stays byte-equal to source's own lifecycle constants — the SDK's
// source of truth. If a lifecycle type is added or renamed in source, this fails
// until catalog's copy is updated, so catalog.Validate never silently misses a
// real event_type/lifecycle collision.
func TestReservedLifecycleEventTypes_MatchSource(t *testing.T) {
	fromSource := []string{
		source.LifecycleEventTypeActivated,
		source.LifecycleEventTypeUpdated,
		source.LifecycleEventTypeSuspended,
		source.LifecycleEventTypeExpirationReminder,
		source.LifecycleEventTypeExpired,
		source.LifecycleEventTypeDeleted,
	}
	sort.Strings(fromSource)

	got := catalog.ReservedLifecycleEventTypes()
	if !reflect.DeepEqual(got, fromSource) {
		t.Errorf("catalog.ReservedLifecycleEventTypes() = %v, want it to equal source's lifecycle constants %v", got, fromSource)
	}
	for _, et := range fromSource {
		if !catalog.IsReservedLifecycleEventType(et) {
			t.Errorf("IsReservedLifecycleEventType(%q) = false, want true", et)
		}
	}
}
