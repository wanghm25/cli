// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package catalog

import "fmt"

// Default filter limits shared by every Filter-supporting event_type. The
// platform caps operator list_value length at 10; a canonical filter payload
// must fit in 1 KiB. They are exported so the SDK-coupled filter validator in
// package event (which cannot live here) clamps against the same hard caps.
const (
	FilterMaxDepth      = 2
	FilterMaxConditions = 10
	FilterMaxBytes      = 1024
	FilterListValueCap  = 10
)

// FilterMeta describes the server-side filter capability of one event_type: the
// allowed logic ops, the global operator set, the structural limits, and the
// operands that can be filtered on. Supported is false for the zero value, so an
// event_type with no filter capability is fail-closed by default.
type FilterMeta struct {
	Supported     bool
	LogicOps      []string // {"and","or"}
	Operators     []string // {"eq","in","contains"}
	MaxDepth      int
	MaxConditions int
	MaxBytes      int
	Operands      []FilterOperandMeta
}

// FilterOperandMeta describes one filterable operand.
type FilterOperandMeta struct {
	Key            string   // operand name, e.g. "sender" / "message_type"
	Operators      []string // subset of FilterMeta.Operators allowed for this operand
	InputValueType string   // projected value shape, e.g. "open_id"; "" if unconstrained
	ListValueMax   int      // per-operand list_value cap (<= FilterListValueCap); 0 if the operand takes no list
}

// Operand returns the operand meta for key.
func (m FilterMeta) Operand(key string) (FilterOperandMeta, bool) {
	for _, o := range m.Operands {
		if o.Key == key {
			return o, true
		}
	}
	return FilterOperandMeta{}, false
}

// EffectiveMaxDepth / EffectiveMaxConditions / EffectiveMaxBytes fall back to
// the package caps when a capability omits a limit, so a mis-seeded meta can
// never widen a hard cap.
func (m FilterMeta) EffectiveMaxDepth() int {
	if m.MaxDepth > 0 && m.MaxDepth <= FilterMaxDepth {
		return m.MaxDepth
	}
	return FilterMaxDepth
}

func (m FilterMeta) EffectiveMaxConditions() int {
	if m.MaxConditions > 0 && m.MaxConditions <= FilterMaxConditions {
		return m.MaxConditions
	}
	return FilterMaxConditions
}

func (m FilterMeta) EffectiveMaxBytes() int {
	if m.MaxBytes > 0 && m.MaxBytes <= FilterMaxBytes {
		return m.MaxBytes
	}
	return FilterMaxBytes
}

// EffectiveListValueMax is the per-operand list_value cap, clamped to the
// package cap for the same mis-seed protection as the FilterMeta limits.
func (o FilterOperandMeta) EffectiveListValueMax() int {
	if o.ListValueMax > 0 && o.ListValueMax <= FilterListValueCap {
		return o.ListValueMax
	}
	return FilterListValueCap
}

// filterMetaRegistry holds the per-event_type filter capability, keyed by
// event_type. It is populated at startup by the business layers that own each
// event_type, each calling RegisterFilterMeta. The generic layer keeps only the
// registry mechanism and validation — never a specific event_type's capability.
var filterMetaRegistry = map[string]FilterMeta{}

// RegisterFilterMeta records the filter capability an event_type accepts. The
// business layer that owns an event_type calls it (typically from an init) to
// declare that event_type's operands and operators. It panics on a duplicate
// registration for the same event_type — mirroring RegisterKey, a second
// registration is a programming error (two owners, or a double init) that would
// otherwise silently overwrite the first and ship the wrong capability, so it
// fails fast at startup. A test seeding a synthetic event_type undoes it with
// UnregisterFilterMetaForTest / ResetRegistryForTest.
func RegisterFilterMeta(eventType string, meta FilterMeta) {
	mu.Lock()
	defer mu.Unlock()
	if _, exists := filterMetaRegistry[eventType]; exists {
		panic(fmt.Sprintf("duplicate FilterMeta registration for event_type: %s", eventType))
	}
	filterMetaRegistry[eventType] = meta
}

// FilterMetaFor returns the filter capability for an event_type. An event_type
// with no registered capability yields the zero FilterMeta (Supported == false),
// so a filter against it is rejected fail-closed.
func FilterMetaFor(eventType string) FilterMeta {
	return filterMetaRegistry[eventType]
}

// UnregisterFilterMetaForTest removes one event_type's filter capability — the
// companion to RegisterFilterMeta's panic-on-duplicate guard, so a test seeding
// a synthetic capability can undo it (and re-seed on a -count=N rerun) without
// tripping the duplicate panic.
func UnregisterFilterMetaForTest(eventType string) {
	mu.Lock()
	defer mu.Unlock()
	delete(filterMetaRegistry, eventType)
}
