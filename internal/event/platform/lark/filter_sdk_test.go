// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package lark

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/larksuite/cli/internal/event/model"
)

// sampleFilter is a representative composite (an "and" of an eq leaf and an in
// leaf) shared by the projection and byte-identity tests.
func sampleFilter() *model.Filter {
	return &model.Filter{Root: &model.FilterNode{
		LogicOp: model.LogicAnd,
		Children: []*model.FilterNode{
			{Condition: &model.FilterCond{Operand: "sender", Op: model.OpEq, Value: "ou_abc"}},
			{Condition: &model.FilterCond{Operand: "message_type", Op: model.OpIn, ListValue: []string{"text", "image"}}},
		},
	}}
}

func TestFilterToSDK_SerializationContract(t *testing.T) {
	raw, err := json.Marshal(FilterToSDK(sampleFilter()))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(raw)

	for _, want := range []string{
		`"composite_condition"`,
		`"logic_op"`,
		`"and"`,
		`"composite_conditions"`,
		`"condition"`,
		`"operand"`,
		`"op"`,
		`"value"`,
		`"list_value"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("projected filter JSON missing %s\n%s", want, got)
		}
	}

	// The in operands must ride list_value, never a misspelled value_list.
	if strings.Contains(got, "value_list") {
		t.Errorf("projected filter JSON must not contain value_list:\n%s", got)
	}
}

func TestFilterProjection_RoundTrip(t *testing.T) {
	original := sampleFilter()

	back := FilterFromSDK(FilterToSDK(original))

	if !model.Equal(original, back) {
		oc, _ := original.Canonicalize()
		bc, _ := back.Canonicalize()
		t.Fatalf("round-trip changed the filter:\n from: %s\n to:   %s", oc, bc)
	}
}

func TestFilterToSDK_ClearForm(t *testing.T) {
	for name, f := range map[string]*model.Filter{
		"nil":         nil,
		"empty-model": {},
	} {
		t.Run(name, func(t *testing.T) {
			sdk := FilterToSDK(f)
			if sdk == nil {
				t.Fatal("clear filter must project to a non-nil &Filter{}, not nil")
			}
			if sdk.CompositeCondition != nil {
				t.Errorf("clear filter must have no composite_condition, got %+v", sdk.CompositeCondition)
			}
			raw, err := json.Marshal(sdk)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if string(raw) != "{}" {
				t.Errorf("clear filter marshals to %s, want {}", raw)
			}
		})
	}
}

func TestFilterFromSDK_NilAndEmptyAreEmpty(t *testing.T) {
	if !FilterFromSDK(nil).IsEmpty() {
		t.Error("FilterFromSDK(nil) should be empty")
	}
	if !FilterFromSDK(FilterToSDK(&model.Filter{})).IsEmpty() {
		t.Error("round-tripped empty filter should be empty")
	}
}
