// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package refined

import "github.com/larksuite/cli/internal/event"

// init registers the filter capability of the mock refined key
// im.message.created_v1 with the generic event layer. The capability is
// declared here, alongside the mock catalog that owns this event_type, so the
// generic filter layer stays free of any specific business event.
//
// meta-swap: replace this hand-seeded capability with the fetch_meta-driven
// meta once it ships — the same swap the mock catalog (refined_keys_mock.json)
// will make. The generic layer's registry and validation are unchanged; only
// the source of this capability moves.
//
// The limits (max depth 2, max conditions 10, max bytes 1024, per-operand list
// cap 10) mirror the platform defaults the generic layer enforces as hard caps.
// They are stated explicitly so the schema disclosure surfaces the same numbers.
func init() {
	event.RegisterFilterMeta("im.message.created_v1", event.FilterMeta{
		Supported:     true,
		LogicOps:      []string{"and", "or"},
		Operators:     []string{"eq", "in", "contains"},
		MaxDepth:      2,
		MaxConditions: 10,
		MaxBytes:      1024,
		Operands: []event.FilterOperandMeta{
			{Key: "sender", Operators: []string{"eq"}, InputValueType: "open_id"},
			{Key: "message_type", Operators: []string{"eq", "in"}, ListValueMax: 10},
		},
	})
}
