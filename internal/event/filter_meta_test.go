// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import "testing"

func TestFilterMetaFor(t *testing.T) {
	supported := FilterMetaFor("im.message.created_v1")
	if !supported.Supported {
		t.Fatal("im.message.created_v1 should support filtering")
	}
	if _, ok := supported.Operand("sender"); !ok {
		t.Error("sender operand should be declared")
	}
	if _, ok := supported.Operand("message_type"); !ok {
		t.Error("message_type operand should be declared")
	}
	if _, ok := supported.Operand("not_declared"); ok {
		t.Error("undeclared operand must not resolve")
	}

	unsupported := FilterMetaFor("im.message.receive_v1")
	if unsupported.Supported {
		t.Error("im.message.receive_v1 must not support filtering (fail-closed default)")
	}
	unknown := FilterMetaFor("does.not.exist")
	if unknown.Supported {
		t.Error("unknown event_type must yield an unsupported zero FilterMeta")
	}
}
