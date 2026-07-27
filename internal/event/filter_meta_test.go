// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import "testing"

func TestFilterMetaFor(t *testing.T) {
	// An event_type with no registered capability is fail-closed: the zero
	// FilterMeta reports unsupported.
	if FilterMetaFor("does.not.exist").Supported {
		t.Error("unregistered event_type must yield an unsupported zero FilterMeta")
	}

	// A registered capability round-trips through the registry: FilterMetaFor
	// returns exactly what RegisterFilterMeta recorded, operands included.
	const eventType = "test.filter.roundtrip_v1"
	RegisterFilterMeta(eventType, FilterMeta{
		Supported: true,
		Operators: []string{opEq, opIn},
		Operands: []FilterOperandMeta{
			{Key: "sender", Operators: []string{opEq}, InputValueType: "open_id"},
			{Key: "message_type", Operators: []string{opEq, opIn}},
		},
	})

	got := FilterMetaFor(eventType)
	if !got.Supported {
		t.Fatal("registered event_type should support filtering")
	}
	if _, ok := got.Operand("sender"); !ok {
		t.Error("sender operand should be declared")
	}
	if _, ok := got.Operand("message_type"); !ok {
		t.Error("message_type operand should be declared")
	}
	if _, ok := got.Operand("not_declared"); ok {
		t.Error("undeclared operand must not resolve")
	}
}
