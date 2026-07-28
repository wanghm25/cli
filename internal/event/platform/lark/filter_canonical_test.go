// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package lark

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/larksuite/cli/internal/event/model"
)

// canonicalReps is a representative spread of filters — empty, single eq,
// single in, a composite mixing eq + in, a nested composite, and an eq with an
// empty value (which forces the always-emitted `"value":""`) — exercising every
// canonical-projection branch (empty, leaf, composite, list_value vs value).
func canonicalReps() map[string]*model.Filter {
	return map[string]*model.Filter{
		"empty":           {},
		"composite_eq_in": sampleFilter(),
		"single_eq": {Root: &model.FilterNode{LogicOp: model.LogicAnd, Children: []*model.FilterNode{
			{Condition: &model.FilterCond{Operand: "sender", Op: model.OpEq, Value: "ou_abc"}},
		}}},
		"single_in": {Root: &model.FilterNode{LogicOp: model.LogicOr, Children: []*model.FilterNode{
			{Condition: &model.FilterCond{Operand: "message_type", Op: model.OpIn, ListValue: []string{"text", "image", "file"}}},
		}}},
		"nested": {Root: &model.FilterNode{LogicOp: model.LogicAnd, Children: []*model.FilterNode{
			{Condition: &model.FilterCond{Operand: "sender", Op: model.OpEq, Value: "ou_abc"}},
			{LogicOp: model.LogicOr, Children: []*model.FilterNode{
				{Condition: &model.FilterCond{Operand: "message_type", Op: model.OpIn, ListValue: []string{"text", "image"}}},
				{Condition: &model.FilterCond{Operand: "chat_type", Op: model.OpContains, Value: "group"}},
			}},
		}}},
		"eq_empty_value": {Root: &model.FilterNode{LogicOp: model.LogicAnd, Children: []*model.FilterNode{
			{Condition: &model.FilterCond{Operand: "sender", Op: model.OpEq, Value: ""}},
		}}},
	}
}

// TestCanonicalize_ByteIdenticalToSDKPath is the byte-identity proof: the
// SDK-free model.Filter.Canonicalize must produce EXACTLY the bytes the SDK
// projection path (json.Marshal(FilterToSDK(f))) produces. Equal, the <=1KB
// size limit, and the entire reconcile filter-conflict matrix depend on this
// canonical form, so any drift is a bug. This test lives with FilterToSDK — the
// only remaining SDK toucher — so it can compare the two directly.
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
