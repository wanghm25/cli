// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import "encoding/json"

// FilterExample returns a representative, wire-valid filter for a capability as
// the exact JSON the CLI would send. It is built from the first declared operand
// through the same canonicalization used for real filters, so a schema example
// can never drift from the actual wire form. It returns nil when the capability
// declares no usable operand.
//
// It is SDK-free — it builds the CLI Filter model and canonicalizes it — so it
// stays in the facade layer beside the validator rather than in the SDK gateway;
// its only inputs are the catalog FilterMeta and the model Filter.
func FilterExample(meta FilterMeta) json.RawMessage {
	if !meta.Supported || len(meta.Operands) == 0 {
		return nil
	}
	operand := meta.Operands[0]
	if len(operand.Operators) == 0 {
		return nil
	}
	placeholder := "example"
	if operand.InputValueType == inputValueTypeOpenID {
		placeholder = "ou_xxx"
	}
	cond := &FilterCond{Operand: operand.Key, Op: operand.Operators[0]}
	if cond.Op == opIn {
		cond.ListValue = []string{placeholder}
	} else {
		cond.Value = placeholder
	}
	example := &Filter{Root: &FilterNode{
		LogicOp:  logicAnd,
		Children: []*FilterNode{{Condition: cond}},
	}}
	raw, err := example.Canonicalize()
	if err != nil {
		return nil
	}
	return raw
}
