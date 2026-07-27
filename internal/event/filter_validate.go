// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"encoding/json"
	"slices"
	"strings"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
)

const inputValueTypeOpenID = "open_id"

// ParseAndValidateFilter parses an inline filter JSON string and validates it
// against an event_type's filter capability, returning the CLI filter model.
//
// An empty (or blank) input is "no filter requested" and returns an empty model
// regardless of capability. A non-empty filter against an event_type that does
// not support filtering is rejected fail-closed. Every rejection is a typed
// invalid_argument on --filter that names the failing rule; the raw filter and
// its values are never echoed back in the error.
func ParseAndValidateFilter(raw string, meta FilterMeta) (*Filter, error) {
	if strings.TrimSpace(raw) == "" {
		return &Filter{}, nil
	}
	if !meta.Supported {
		return nil, newFilterError("this event type does not support --filter; upgrade the CLI or use a Filter-supporting event type")
	}

	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	var sf larkeventv1.Filter
	if err := dec.Decode(&sf); err != nil {
		return nil, newFilterError("filter is not valid JSON or contains an unknown field").WithCause(err)
	}
	if dec.More() {
		return nil, newFilterError("filter must be a single JSON object")
	}
	if sf.CompositeCondition == nil {
		return nil, newFilterError("filter must have a composite_condition at its root")
	}
	root := sf.CompositeCondition
	if root.Condition != nil {
		return nil, newFilterError("filter root must be a composite_condition with a logic_op, not a single condition")
	}

	v := &filterValidator{meta: meta}
	node, err := v.walkComposite(root, 1)
	if err != nil {
		return nil, err
	}

	f := &Filter{Root: node}
	canonical, err := f.Canonicalize()
	if err != nil {
		return nil, newFilterError("filter could not be serialized").WithCause(err)
	}
	if len(canonical) > meta.maxBytes() {
		return nil, newFilterError("filter exceeds the maximum size of %d bytes", meta.maxBytes())
	}
	return f, nil
}

// filterValidator carries the capability being enforced and the running leaf
// count across the recursive walk.
type filterValidator struct {
	meta      FilterMeta
	condCount int
}

// walkNode classifies a node as a leaf condition or a nested composite and
// validates it. A node that is both, or neither, is malformed.
func (v *filterValidator) walkNode(cc *larkeventv1.CompositeCondition, depth int) (*FilterNode, error) {
	if cc == nil {
		return nil, newFilterError("a filter composite contains an empty node")
	}
	isLeaf := cc.Condition != nil
	isComposite := cc.LogicOp != nil || len(cc.CompositeConditions) > 0
	switch {
	case isLeaf && isComposite:
		return nil, newFilterError("a filter node must be either a condition or a composite, not both")
	case isLeaf:
		return v.walkLeaf(cc.Condition)
	case isComposite:
		return v.walkComposite(cc, depth)
	default:
		return nil, newFilterError("a filter node must contain either a condition or a composite")
	}
}

// walkComposite validates a composite node: nesting depth, logic_op, and each
// child in order.
func (v *filterValidator) walkComposite(cc *larkeventv1.CompositeCondition, depth int) (*FilterNode, error) {
	if depth > v.meta.maxDepth() {
		return nil, newFilterError("filter nesting exceeds the maximum of %d levels", v.meta.maxDepth())
	}
	if cc.LogicOp == nil {
		return nil, newFilterError("a composite filter requires a logic_op")
	}
	logicOp := *cc.LogicOp
	if !slices.Contains(v.meta.LogicOps, logicOp) {
		return nil, newFilterError("filter logic_op is not one of the supported values")
	}
	if len(cc.CompositeConditions) == 0 {
		return nil, newFilterError("a composite filter requires at least one nested condition")
	}

	node := &FilterNode{LogicOp: logicOp}
	for _, child := range cc.CompositeConditions {
		childNode, err := v.walkNode(child, depth+1)
		if err != nil {
			return nil, err
		}
		node.Children = append(node.Children, childNode)
	}
	return node, nil
}

// walkLeaf validates one leaf condition against the operand's capability.
func (v *filterValidator) walkLeaf(c *larkeventv1.Contidion) (*FilterNode, error) {
	v.condCount++
	if v.condCount > v.meta.maxConditions() {
		return nil, newFilterError("filter exceeds the maximum of %d conditions", v.meta.maxConditions())
	}
	if c.Operand == nil || *c.Operand == "" {
		return nil, newFilterError("a filter condition requires an operand")
	}
	if c.Op == nil || *c.Op == "" {
		return nil, newFilterError("a filter condition requires an op")
	}
	operand := *c.Operand
	op := *c.Op

	if !slices.Contains(v.meta.Operators, op) {
		return nil, newFilterError("filter uses an unsupported operator")
	}
	opMeta, ok := v.meta.Operand(operand)
	if !ok {
		return nil, newFilterError("filter references an operand not supported by this event type")
	}
	if !slices.Contains(opMeta.Operators, op) {
		return nil, newFilterError("filter operator is not allowed for this operand")
	}

	hasValue := c.Value != nil
	hasList := c.ListValue != nil
	if hasValue && hasList {
		return nil, newFilterError("a filter condition must set value or list_value, not both")
	}

	cond := &FilterCond{Operand: operand, Op: op}
	if op == opIn {
		if hasValue {
			return nil, newFilterError("the in operator requires list_value, not value")
		}
		if len(c.ListValue) == 0 {
			return nil, newFilterError("the in operator requires a non-empty list_value")
		}
		if max := opMeta.listValueMax(); len(c.ListValue) > max {
			return nil, newFilterError("filter list_value exceeds the maximum of %d items", max)
		}
		if opMeta.InputValueType == inputValueTypeOpenID {
			for _, item := range c.ListValue {
				if !looksLikeOpenID(item) {
					return nil, newFilterError("filter value must be an open_id")
				}
			}
		}
		cond.ListValue = append([]string(nil), c.ListValue...)
	} else {
		if hasList {
			return nil, newFilterError("this operator requires value, not list_value")
		}
		if !hasValue || *c.Value == "" {
			return nil, newFilterError("this operator requires a non-empty value")
		}
		if opMeta.InputValueType == inputValueTypeOpenID && !looksLikeOpenID(*c.Value) {
			return nil, newFilterError("filter value must be an open_id")
		}
		cond.Value = *c.Value
	}
	return &FilterNode{Condition: cond}, nil
}

// maxDepth / maxConditions / maxBytes fall back to the package caps when a
// capability omits a limit, so a mis-seeded meta can never widen a hard cap.
func (m FilterMeta) maxDepth() int {
	if m.MaxDepth > 0 && m.MaxDepth <= filterMaxDepth {
		return m.MaxDepth
	}
	return filterMaxDepth
}

func (m FilterMeta) maxConditions() int {
	if m.MaxConditions > 0 && m.MaxConditions <= filterMaxConditions {
		return m.MaxConditions
	}
	return filterMaxConditions
}

func (m FilterMeta) maxBytes() int {
	if m.MaxBytes > 0 && m.MaxBytes <= filterMaxBytes {
		return m.MaxBytes
	}
	return filterMaxBytes
}

func (o FilterOperandMeta) listValueMax() int {
	if o.ListValueMax > 0 && o.ListValueMax <= filterListValueCap {
		return o.ListValueMax
	}
	return filterListValueCap
}

// looksLikeOpenID reports whether s has the shape of an open_id ("ou_" followed
// by url-safe characters). It rejects user_id / union_id ("on_") / display names
// and other malformed inputs.
func looksLikeOpenID(s string) bool {
	const prefix = "ou_"
	if !strings.HasPrefix(s, prefix) {
		return false
	}
	rest := s[len(prefix):]
	if rest == "" {
		return false
	}
	for _, r := range rest {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
		default:
			return false
		}
	}
	return true
}

// newFilterError builds the typed invalid_argument error for --filter. Callers
// pass only rule descriptions and constant limits — never the raw filter or its
// values — so nothing user-supplied leaks into the message.
func newFilterError(format string, args ...any) *errs.ValidationError {
	return errs.NewValidationError(errs.SubtypeInvalidArgument, format, args...).
		WithParam("--filter").
		WithHint("run `lark-cli event schema <EventKey> --json` to see the supported --filter operands, operators, and limits")
}
