// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package model

import (
	"bytes"
	"testing"
)

// sampleFilter is a representative composite (an "and" of an eq leaf and an in
// leaf) shared by the canonicalization / equality tests.
func sampleFilter() *Filter {
	return &Filter{Root: &FilterNode{
		LogicOp: LogicAnd,
		Children: []*FilterNode{
			{Condition: &FilterCond{Operand: "sender", Op: OpEq, Value: "ou_abc"}},
			{Condition: &FilterCond{Operand: "message_type", Op: OpIn, ListValue: []string{"text", "image"}}},
		},
	}}
}

func TestFilter_IsEmpty(t *testing.T) {
	if !(*Filter)(nil).IsEmpty() {
		t.Error("nil *Filter must be empty")
	}
	if !(&Filter{}).IsEmpty() {
		t.Error("Filter with nil Root must be empty")
	}
	if sampleFilter().IsEmpty() {
		t.Error("populated Filter must not be empty")
	}
}

func TestCanonicalize_Deterministic(t *testing.T) {
	a, err := sampleFilter().Canonicalize()
	if err != nil {
		t.Fatalf("canonicalize: %v", err)
	}
	b, err := sampleFilter().Canonicalize()
	if err != nil {
		t.Fatalf("canonicalize: %v", err)
	}
	if !bytes.Equal(a, b) {
		t.Errorf("canonical form is not deterministic:\n a=%s\n b=%s", a, b)
	}
}

func TestCanonicalize_OrderSensitive(t *testing.T) {
	// Same leaves, reversed child order => different canonical bytes.
	reversed := &Filter{Root: &FilterNode{
		LogicOp: LogicAnd,
		Children: []*FilterNode{
			{Condition: &FilterCond{Operand: "message_type", Op: OpIn, ListValue: []string{"text", "image"}}},
			{Condition: &FilterCond{Operand: "sender", Op: OpEq, Value: "ou_abc"}},
		},
	}}
	base, _ := sampleFilter().Canonicalize()
	rev, _ := reversed.Canonicalize()
	if bytes.Equal(base, rev) {
		t.Error("reordered children must produce different canonical bytes (order-sensitive)")
	}
}

func TestEqual(t *testing.T) {
	reorderedChildren := &Filter{Root: &FilterNode{
		LogicOp: LogicAnd,
		Children: []*FilterNode{
			{Condition: &FilterCond{Operand: "message_type", Op: OpIn, ListValue: []string{"text", "image"}}},
			{Condition: &FilterCond{Operand: "sender", Op: OpEq, Value: "ou_abc"}},
		},
	}}
	reorderedList := &Filter{Root: &FilterNode{
		LogicOp: LogicAnd,
		Children: []*FilterNode{
			{Condition: &FilterCond{Operand: "sender", Op: OpEq, Value: "ou_abc"}},
			{Condition: &FilterCond{Operand: "message_type", Op: OpIn, ListValue: []string{"image", "text"}}},
		},
	}}

	tests := []struct {
		name string
		a, b *Filter
		want bool
	}{
		{"identical", sampleFilter(), sampleFilter(), true},
		{"empty-empty", &Filter{}, &Filter{}, true},
		{"nil-nil", nil, nil, true},
		{"nil-empty", nil, &Filter{}, true},
		{"empty-vs-nonempty", &Filter{}, sampleFilter(), false},
		{"nonempty-vs-empty", sampleFilter(), &Filter{}, false},
		{"reordered-children", sampleFilter(), reorderedChildren, false},
		{"reordered-list", sampleFilter(), reorderedList, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Equal(tc.a, tc.b); got != tc.want {
				t.Errorf("Equal = %v, want %v", got, tc.want)
			}
		})
	}
}
