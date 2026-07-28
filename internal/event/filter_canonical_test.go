// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"bytes"
	"encoding/json"
	"testing"
)

// canonicalReps is a representative spread of filters — empty, single eq,
// single in, a composite mixing eq + in, a nested composite, and an eq with an
// empty value (which forces the always-emitted `"value":""`) — exercising every
// canonical-projection branch (empty, leaf, composite, list_value vs value).
func canonicalReps() map[string]*Filter {
	return map[string]*Filter{
		"empty":           {},
		"composite_eq_in": sampleFilter(),
		"single_eq": {Root: &FilterNode{LogicOp: logicAnd, Children: []*FilterNode{
			{Condition: &FilterCond{Operand: "sender", Op: opEq, Value: "ou_abc"}},
		}}},
		"single_in": {Root: &FilterNode{LogicOp: logicOr, Children: []*FilterNode{
			{Condition: &FilterCond{Operand: "message_type", Op: opIn, ListValue: []string{"text", "image", "file"}}},
		}}},
		"nested": {Root: &FilterNode{LogicOp: logicAnd, Children: []*FilterNode{
			{Condition: &FilterCond{Operand: "sender", Op: opEq, Value: "ou_abc"}},
			{LogicOp: logicOr, Children: []*FilterNode{
				{Condition: &FilterCond{Operand: "message_type", Op: opIn, ListValue: []string{"text", "image"}}},
				{Condition: &FilterCond{Operand: "chat_type", Op: opContains, Value: "group"}},
			}},
		}}},
		"eq_empty_value": {Root: &FilterNode{LogicOp: logicAnd, Children: []*FilterNode{
			{Condition: &FilterCond{Operand: "sender", Op: opEq, Value: ""}},
		}}},
	}
}

// TestCanonicalize_ByteIdenticalToSDKPath is the byte-identity proof: the new,
// SDK-free Canonicalize must produce EXACTLY the bytes the SDK projection path
// (json.Marshal(FilterToSDK(f))) produced. Equal, the <=1KB size limit, and the
// entire reconcile filter-conflict matrix depend on this canonical form, so any
// drift is a bug. Once FilterToSDK moves to platform/lark, this proof moves with
// it (comparing model.Canonicalize against lark.FilterToSDK).
func TestCanonicalize_ByteIdenticalToSDKPath(t *testing.T) {
	for name, f := range canonicalReps() {
		t.Run(name, func(t *testing.T) {
			got, err := f.Canonicalize()
			if err != nil {
				t.Fatalf("Canonicalize: %v", err)
			}
			want, err := json.Marshal(FilterToSDK(f))
			if err != nil {
				t.Fatalf("SDK-path marshal: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("Canonicalize drifted from the SDK wire bytes:\n  new: %s\n  sdk: %s", got, want)
			}
		})
	}
}
