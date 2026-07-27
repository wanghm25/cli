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
// and the byte-size limit. It delegates to the SDK projection so the bytes are
// exactly what would be sent on the wire.
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
	return json.Marshal(filterToSDK(f))
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
