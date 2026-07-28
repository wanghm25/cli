// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package lark

import (
	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/internal/event/model"
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
//
// This is the SOLE SDK projection of the filter model: it lives in the platform
// gateway (the only SDK toucher), keeping the model itself SDK-free. Its output
// must stay byte-identical to model.Filter.Canonicalize — the filter_canonical
// test pins that here.
func FilterToSDK(f *model.Filter) *larkeventv1.Filter {
	if f.IsEmpty() {
		return &larkeventv1.Filter{}
	}
	return &larkeventv1.Filter{CompositeCondition: nodeToSDK(f.Root)}
}

func nodeToSDK(n *model.FilterNode) *larkeventv1.CompositeCondition {
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

func condToSDK(c *model.FilterCond) *larkeventv1.Contidion {
	operand := c.Operand
	op := c.Op
	out := &larkeventv1.Contidion{Operand: &operand, Op: &op}
	// in carries its operands in list_value; eq / contains carry a scalar value.
	if c.Op == model.OpIn {
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
func FilterFromSDK(sf *larkeventv1.Filter) *model.Filter {
	if sf == nil || sf.CompositeCondition == nil {
		return &model.Filter{}
	}
	return &model.Filter{Root: nodeFromSDK(sf.CompositeCondition)}
}

func nodeFromSDK(cc *larkeventv1.CompositeCondition) *model.FilterNode {
	if cc == nil {
		return nil
	}
	n := &model.FilterNode{}
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

func condFromSDK(c *larkeventv1.Contidion) *model.FilterCond {
	fc := &model.FilterCond{}
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
