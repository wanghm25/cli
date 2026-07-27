// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"encoding/json"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"
)

// FilterToSDK projects the CLI filter model onto the SDK filter type at the API
// boundary. An empty (or cleared) filter becomes a non-nil empty *Filter, which
// serializes to the product's clear-filter wire form {"filter":{}} — returning
// nil instead would be omitted by the request encoder and read as "leave the
// filter unchanged", which is a different intent.
//
// It is exported so the command layer (create/consume/reads) can project a CLI
// filter to the SDK request type at its own API boundary without duplicating
// this mapping; those packages already import the SDK type to build requests.
func FilterToSDK(f *Filter) *larkeventv1.Filter {
	if f.IsEmpty() {
		return &larkeventv1.Filter{}
	}
	return &larkeventv1.Filter{CompositeCondition: nodeToSDK(f.Root)}
}

func nodeToSDK(n *FilterNode) *larkeventv1.CompositeCondition {
	if n == nil {
		return nil
	}
	cc := &larkeventv1.CompositeCondition{}
	if n.Condition != nil {
		cc.Condition = condToSDK(n.Condition)
		return cc
	}
	logicOp := n.LogicOp
	cc.LogicOp = &logicOp
	for _, child := range n.Children {
		cc.CompositeConditions = append(cc.CompositeConditions, nodeToSDK(child))
	}
	return cc
}

func condToSDK(c *FilterCond) *larkeventv1.Contidion {
	operand := c.Operand
	op := c.Op
	out := &larkeventv1.Contidion{Operand: &operand, Op: &op}
	// in carries its operands in list_value; eq / contains carry a scalar value.
	if c.Op == opIn {
		out.ListValue = append([]string(nil), c.ListValue...)
	} else {
		value := c.Value
		out.Value = &value
	}
	return out
}

// FilterFromSDK builds the CLI filter model from the SDK filter type. A nil or
// empty SDK filter yields an empty (no-filter) CLI model.
//
// Exported alongside FilterToSDK so callers can project a remote SDK filter
// back into the CLI model — to compare it against a requested filter, or to
// surface it (canonicalized) in read output.
func FilterFromSDK(sf *larkeventv1.Filter) *Filter {
	if sf == nil || sf.CompositeCondition == nil {
		return &Filter{}
	}
	return &Filter{Root: nodeFromSDK(sf.CompositeCondition)}
}

func nodeFromSDK(cc *larkeventv1.CompositeCondition) *FilterNode {
	if cc == nil {
		return nil
	}
	n := &FilterNode{}
	if cc.Condition != nil {
		n.Condition = condFromSDK(cc.Condition)
		return n
	}
	if cc.LogicOp != nil {
		n.LogicOp = *cc.LogicOp
	}
	for _, child := range cc.CompositeConditions {
		n.Children = append(n.Children, nodeFromSDK(child))
	}
	return n
}

func condFromSDK(c *larkeventv1.Contidion) *FilterCond {
	fc := &FilterCond{}
	if c.Operand != nil {
		fc.Operand = *c.Operand
	}
	if c.Op != nil {
		fc.Op = *c.Op
	}
	if c.Value != nil {
		fc.Value = *c.Value
	}
	if len(c.ListValue) > 0 {
		fc.ListValue = append([]string(nil), c.ListValue...)
	}
	return fc
}

// FilterExample returns a representative, wire-valid filter for a capability as
// the exact JSON the CLI would send. It is built from the first declared operand
// through the same projection used for real filters, so a schema example can
// never drift from the actual wire form. It returns nil when the capability
// declares no usable operand.
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
