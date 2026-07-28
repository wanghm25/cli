// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"bytes"
	"encoding/json"
)

// DSL vocabulary. Operators are the supported comparison ops; logic ops combine
// conditions. "not" is deliberately absent — it is rejected even though some SDK
// comments still list it.
const (
	opEq       = "eq"
	opIn       = "in"
	opContains = "contains"

	logicAnd = "and"
	logicOr  = "or"
)

// Filter is the CLI-owned model of a server-side event filter. It is aligned to
// the platform filter wire shape but owned by the CLI so the SDK's exact type
// spelling never leaks into CLI-facing contracts. A nil Root means "no filter".
type Filter struct {
	Root *FilterNode // nil Root == empty / no filter requested
}

// FilterNode is one node of the filter tree. A node is EITHER a composite
// (LogicOp set, Children ordered) OR a leaf (Condition set); never both. The
// top-level node is always a composite.
type FilterNode struct {
	LogicOp   string        // "and" / "or" for a composite; "" for a leaf
	Children  []*FilterNode // composite children, in declaration order
	Condition *FilterCond   // leaf payload; nil for a composite
}

// FilterCond is a single leaf condition: an operand tested by an operator
// against either a scalar Value (eq / contains) or a ListValue (in).
type FilterCond struct {
	Operand   string
	Op        string
	Value     string
	ListValue []string
}

// IsEmpty reports whether no filter was requested. A nil *Filter is empty.
func (f *Filter) IsEmpty() bool {
	return f == nil || f.Root == nil
}

// Canonicalize returns the deterministic wire JSON used for equality comparison
// and the byte-size limit. It marshals the CLI model DIRECTLY to the canonical
// wire shape (no SDK dependency), so the model — and this method — can live in
// the SDK-free value layer. The bytes are byte-identical to what the SDK
// projection (FilterToSDK) would marshal, because the canonical wire structs
// below mirror the SDK filter type's exact JSON field names, declaration order,
// and omitempty semantics, and the projection reproduces the SDK projector's
// exact nil/non-nil field decisions. The filter_canonical byte-identity test
// pins that equivalence against the real SDK path.
//
// Canonicalization is intentionally conservative and order-SENSITIVE: it
// normalizes only serialization (fixed field order via struct encoding, no
// whitespace, each value decoded once into the model then re-encoded) and
// PRESERVES composite_conditions child order and list_value item order. A
// reordered filter therefore compares as different, which errs toward a
// conflict/human decision rather than a wrong reuse. This can be relaxed to
// unordered/set semantics later if the platform confirms order-insensitive
// matching.
func (f *Filter) Canonicalize() ([]byte, error) {
	return json.Marshal(f.toCanonical())
}

// canonFilter / canonComposite / canonCondition mirror the platform filter wire
// type's JSON contract EXACTLY — field names, struct declaration order (which
// fixes JSON key order), pointer/slice types, and omitempty — so marshaling them
// yields the same bytes the SDK filter type does. They are the CLI's own copy of
// that contract, kept here so canonicalization has no SDK dependency.
type canonFilter struct {
	CompositeCondition *canonComposite `json:"composite_condition,omitempty"`
}

type canonComposite struct {
	LogicOp             *string           `json:"logic_op,omitempty"`
	Condition           *canonCondition   `json:"condition,omitempty"`
	CompositeConditions []*canonComposite `json:"composite_conditions,omitempty"`
}

type canonCondition struct {
	Operand   *string  `json:"operand,omitempty"`
	Op        *string  `json:"op,omitempty"`
	Value     *string  `json:"value,omitempty"`
	ListValue []string `json:"list_value,omitempty"`
}

// toCanonical projects the CLI filter model onto the canonical wire structs. It
// reproduces the SDK projector's field decisions one-for-one: an empty filter
// becomes a bare {} (no composite_condition); a leaf node emits only condition;
// a composite always emits logic_op (a non-nil pointer, even for an empty op)
// plus its ordered children; a condition always emits operand and op, and rides
// list_value for the in operator or a (non-nil, always-emitted) value otherwise.
func (f *Filter) toCanonical() *canonFilter {
	if f.IsEmpty() {
		return &canonFilter{}
	}
	return &canonFilter{CompositeCondition: nodeToCanonical(f.Root)}
}

func nodeToCanonical(n *FilterNode) *canonComposite {
	if n == nil {
		return nil
	}
	cc := &canonComposite{}
	if n.Condition != nil {
		cc.Condition = condToCanonical(n.Condition)
		return cc
	}
	logicOp := n.LogicOp
	cc.LogicOp = &logicOp
	for _, child := range n.Children {
		cc.CompositeConditions = append(cc.CompositeConditions, nodeToCanonical(child))
	}
	return cc
}

func condToCanonical(c *FilterCond) *canonCondition {
	operand := c.Operand
	op := c.Op
	out := &canonCondition{Operand: &operand, Op: &op}
	// in carries its operands in list_value; eq / contains carry a scalar value.
	if c.Op == opIn {
		out.ListValue = append([]string(nil), c.ListValue...)
	} else {
		value := c.Value
		out.Value = &value
	}
	return out
}

// Equal reports whether two filters are semantically equal via their canonical
// (order-preserving) form. Empty == empty is true; empty vs non-empty is false.
// It never string-compares raw input JSON.
func Equal(a, b *Filter) bool {
	if a.IsEmpty() || b.IsEmpty() {
		return a.IsEmpty() && b.IsEmpty()
	}
	ca, err := a.Canonicalize()
	if err != nil {
		return false
	}
	cb, err := b.Canonicalize()
	if err != nil {
		return false
	}
	return bytes.Equal(ca, cb)
}
