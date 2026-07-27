// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

// Default filter limits shared by every Filter-supporting event_type. The
// platform caps operator list_value length at 10; a canonical filter payload
// must fit in 1 KiB.
const (
	filterMaxDepth      = 2
	filterMaxConditions = 10
	filterMaxBytes      = 1024
	filterListValueCap  = 10
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
	ListValueMax   int      // per-operand list_value cap (<= filterListValueCap); 0 if the operand takes no list
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

// filterMetaByEventType is the single embedded source of per-event_type filter
// capability. It is seeded with a mock mirroring the platform demo.
//
// meta-swap: replace this seeded map with the fetch_meta-driven capability
// (projecting the platform operand descriptors, including their value shape,
// down to input_value_type) once that meta ships. Callers and validation are
// unchanged — only this table's source changes.
var filterMetaByEventType = map[string]FilterMeta{
	"im.message.created_v1": {
		Supported:     true,
		LogicOps:      []string{logicAnd, logicOr},
		Operators:     []string{opEq, opIn, opContains},
		MaxDepth:      filterMaxDepth,
		MaxConditions: filterMaxConditions,
		MaxBytes:      filterMaxBytes,
		Operands: []FilterOperandMeta{
			{Key: "sender", Operators: []string{opEq}, InputValueType: "open_id"},
			{Key: "message_type", Operators: []string{opEq, opIn}, ListValueMax: filterListValueCap},
		},
	},
}

// FilterMetaFor returns the filter capability for an event_type. An event_type
// with no declared capability yields the zero FilterMeta (Supported == false),
// so a filter against it is rejected fail-closed.
func FilterMetaFor(eventType string) FilterMeta {
	return filterMetaByEventType[eventType]
}
