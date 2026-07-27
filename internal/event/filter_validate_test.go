// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/larksuite/cli/errs"
)

// seededFilterMeta is the demo capability used across the validation tests.
func seededFilterMeta() FilterMeta { return FilterMetaFor("im.message.created_v1") }

// containsFilterMeta declares an operand that allows contains, which the seeded
// demo operands do not, so the contains happy-path and R7 can be exercised.
func containsFilterMeta() FilterMeta {
	return FilterMeta{
		Supported:     true,
		LogicOps:      []string{logicAnd, logicOr},
		Operators:     []string{opEq, opIn, opContains},
		MaxDepth:      filterMaxDepth,
		MaxConditions: filterMaxConditions,
		MaxBytes:      filterMaxBytes,
		Operands: []FilterOperandMeta{
			{Key: "text", Operators: []string{opContains, opEq}},
		},
	}
}

func assertRejected(t *testing.T, raw string, meta FilterMeta) *errs.Problem {
	t.Helper()
	_, err := ParseAndValidateFilter(raw, meta)
	if err == nil {
		t.Fatal("expected the filter to be rejected, got nil error")
	}
	p, ok := errs.ProblemOf(err)
	if !ok {
		t.Fatalf("error is not a typed problem: %T %v", err, err)
	}
	if p.Category != errs.CategoryValidation || p.Subtype != errs.SubtypeInvalidArgument {
		t.Errorf("classification = %s/%s, want validation/invalid_argument", p.Category, p.Subtype)
	}
	var ve *errs.ValidationError
	if errors.As(err, &ve) {
		if ve.Param != "--filter" {
			t.Errorf("param = %q, want --filter", ve.Param)
		}
	} else {
		t.Errorf("error is not a *ValidationError: %T", err)
	}
	return p
}

func TestParseAndValidateFilter_Valid(t *testing.T) {
	raw := `{"composite_condition":{"logic_op":"and","composite_conditions":[
		{"condition":{"operand":"sender","op":"eq","value":"ou_abc123"}},
		{"logic_op":"or","composite_conditions":[
			{"condition":{"operand":"message_type","op":"eq","value":"text"}},
			{"condition":{"operand":"message_type","op":"in","list_value":["image","file"]}}
		]}
	]}}`
	f, err := ParseAndValidateFilter(raw, seededFilterMeta())
	if err != nil {
		t.Fatalf("valid filter rejected: %v", err)
	}
	if f.IsEmpty() {
		t.Fatal("valid filter should not be empty")
	}
	if f.Root.LogicOp != logicAnd || len(f.Root.Children) != 2 {
		t.Fatalf("unexpected root shape: %+v", f.Root)
	}
	// The nested composite (second child) must preserve its two leaves in order.
	nested := f.Root.Children[1]
	if nested.LogicOp != logicOr || len(nested.Children) != 2 {
		t.Fatalf("unexpected nested shape: %+v", nested)
	}
}

func TestParseAndValidateFilter_ContainsHappyPath(t *testing.T) {
	raw := `{"composite_condition":{"logic_op":"and","composite_conditions":[
		{"condition":{"operand":"text","op":"contains","value":"hello"}}
	]}}`
	if _, err := ParseAndValidateFilter(raw, containsFilterMeta()); err != nil {
		t.Fatalf("valid contains filter rejected: %v", err)
	}
}

func TestParseAndValidateFilter_EmptyIsNoFilter(t *testing.T) {
	for _, raw := range []string{"", "   ", "\n\t"} {
		// Empty input is "no filter" even when the event type does not support it.
		f, err := ParseAndValidateFilter(raw, FilterMeta{Supported: false})
		if err != nil {
			t.Fatalf("empty filter %q rejected: %v", raw, err)
		}
		if !f.IsEmpty() {
			t.Errorf("empty input %q should yield an empty filter", raw)
		}
	}
}

func TestParseAndValidateFilter_Unsupported(t *testing.T) {
	raw := `{"composite_condition":{"logic_op":"and","composite_conditions":[
		{"condition":{"operand":"sender","op":"eq","value":"ou_abc"}}
	]}}`
	p := assertRejected(t, raw, FilterMeta{Supported: false})
	if !strings.Contains(p.Message, "does not support --filter") {
		t.Errorf("unsupported message = %q, want mention of unsupported --filter", p.Message)
	}
}

// TestParseAndValidateFilter_RuleViolations pins one rejection per DSL rule and
// limit. All use the seeded demo capability unless noted.
func TestParseAndValidateFilter_RuleViolations(t *testing.T) {
	longList, _ := json.Marshal(func() []string {
		items := make([]string, 11) // one over the operand list max of 10
		for i := range items {
			items[i] = "t"
		}
		return items
	}())
	oversizeList, _ := json.Marshal(func() []string {
		items := make([]string, 10) // within the item cap, but pushes canonical over 1 KiB
		for i := range items {
			items[i] = strings.Repeat("a", 150)
		}
		return items
	}())
	manyConditions := func() string {
		leaves := make([]string, 11) // one over the 10-condition limit
		for i := range leaves {
			leaves[i] = `{"condition":{"operand":"message_type","op":"eq","value":"text"}}`
		}
		return `{"composite_condition":{"logic_op":"and","composite_conditions":[` + strings.Join(leaves, ",") + `]}}`
	}()

	tests := []struct {
		name string
		raw  string
		meta FilterMeta
	}{
		{"R1 logic_op not", `{"composite_condition":{"logic_op":"not","composite_conditions":[{"condition":{"operand":"sender","op":"eq","value":"ou_a"}}]}}`, seededFilterMeta()},
		{"R1 logic_op unknown", `{"composite_condition":{"logic_op":"xor","composite_conditions":[{"condition":{"operand":"sender","op":"eq","value":"ou_a"}}]}}`, seededFilterMeta()},
		{"R2 op unsupported", `{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"sender","op":"neq","value":"ou_a"}}]}}`, seededFilterMeta()},
		{"R3 op not allowed for operand", `{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"sender","op":"in","list_value":["ou_a"]}}]}}`, seededFilterMeta()},
		{"R4 unknown operand", `{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"mystery","op":"eq","value":"x"}}]}}`, seededFilterMeta()},
		{"R5 union_id not open_id", `{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"sender","op":"eq","value":"on_union123"}}]}}`, seededFilterMeta()},
		{"R5 bare id not open_id", `{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"sender","op":"eq","value":"user123"}}]}}`, seededFilterMeta()},
		{"R5 open_id prefix only", `{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"sender","op":"eq","value":"ou_"}}]}}`, seededFilterMeta()},
		{"R6 in with value", `{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"message_type","op":"in","value":"text"}}]}}`, seededFilterMeta()},
		{"R7 eq with list_value", `{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"message_type","op":"eq","list_value":["text"]}}]}}`, seededFilterMeta()},
		{"R8 three-level nesting", `{"composite_condition":{"logic_op":"and","composite_conditions":[{"logic_op":"or","composite_conditions":[{"logic_op":"and","composite_conditions":[{"condition":{"operand":"message_type","op":"eq","value":"text"}}]}]}]}}`, seededFilterMeta()},
		{"limit list_value items", `{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"message_type","op":"in","list_value":` + string(longList) + `}}]}}`, seededFilterMeta()},
		{"limit canonical bytes", `{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"message_type","op":"in","list_value":` + string(oversizeList) + `}}]}}`, seededFilterMeta()},
		{"limit condition count", manyConditions, seededFilterMeta()},
		{"both value and list_value", `{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"message_type","op":"eq","value":"text","list_value":["text"]}}]}}`, seededFilterMeta()},
		{"empty leaf", `{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{}}]}}`, seededFilterMeta()},
		{"empty composite_conditions", `{"composite_condition":{"logic_op":"and","composite_conditions":[]}}`, seededFilterMeta()},
		{"unknown field", `{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"sender","op":"eq","value":"ou_a","extra":1}}]}}`, seededFilterMeta()},
		{"root is bare condition", `{"composite_condition":{"condition":{"operand":"sender","op":"eq","value":"ou_a"}}}`, seededFilterMeta()},
		{"missing composite_condition", `{}`, seededFilterMeta()},
		{"not valid json", `{"composite_condition":`, seededFilterMeta()},
		{"contains with list_value", `{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"text","op":"contains","list_value":["x"]}}]}}`, containsFilterMeta()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertRejected(t, tc.raw, tc.meta)
		})
	}
}

// TestParseAndValidateFilter_ErrorNeverLeaksValues verifies the operand/value
// tokens from a rejected filter do not appear in the error text.
func TestParseAndValidateFilter_ErrorNeverLeaksValues(t *testing.T) {
	raw := `{"composite_condition":{"logic_op":"and","composite_conditions":[
		{"condition":{"operand":"sender","op":"eq","value":"on_secret_value_123"}}
	]}}`
	_, err := ParseAndValidateFilter(raw, seededFilterMeta())
	if err == nil {
		t.Fatal("expected rejection")
	}
	msg := err.Error()
	for _, leak := range []string{"on_secret_value_123"} {
		if strings.Contains(msg, leak) {
			t.Errorf("error message leaked the raw value %q: %q", leak, msg)
		}
	}
}

// TestParseAndValidateFilter_EqualIgnoresFormatting confirms two inputs that
// differ only in whitespace and object field order canonicalize equal, while a
// reordered child list does not.
func TestParseAndValidateFilter_EqualIgnoresFormatting(t *testing.T) {
	compact := `{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"sender","op":"eq","value":"ou_a"}},{"condition":{"operand":"message_type","op":"in","list_value":["text","image"]}}]}}`
	// Same filter, reflowed with whitespace and with condition object keys reordered.
	reflowed := `{
		"composite_condition": {
			"composite_conditions": [
				{ "condition": { "value": "ou_a", "op": "eq", "operand": "sender" } },
				{ "condition": { "list_value": ["text", "image"], "op": "in", "operand": "message_type" } }
			],
			"logic_op": "and"
		}
	}`
	reordered := `{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"message_type","op":"in","list_value":["text","image"]}},{"condition":{"operand":"sender","op":"eq","value":"ou_a"}}]}}`

	meta := seededFilterMeta()
	a, err := ParseAndValidateFilter(compact, meta)
	if err != nil {
		t.Fatalf("compact rejected: %v", err)
	}
	b, err := ParseAndValidateFilter(reflowed, meta)
	if err != nil {
		t.Fatalf("reflowed rejected: %v", err)
	}
	c, err := ParseAndValidateFilter(reordered, meta)
	if err != nil {
		t.Fatalf("reordered rejected: %v", err)
	}

	if !Equal(a, b) {
		t.Error("filters differing only in whitespace / field order must be Equal")
	}
	if Equal(a, c) {
		t.Error("filters differing in child order must NOT be Equal (order-sensitive)")
	}
}
