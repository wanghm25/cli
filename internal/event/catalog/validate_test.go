// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package catalog

import (
	"strings"
	"testing"
)

// TestValidate_GoodCatalog: a well-formed catalog (legacy + refined key + a
// consistent filter meta) passes validation.
func TestValidate_GoodCatalog(t *testing.T) {
	resetRegistry()
	t.Cleanup(resetRegistry)

	RegisterKey(KeyDefinition{Key: "a.legacy_v1", EventType: "a.legacy_v1", Schema: nativeSchema()})
	RegisterKey(KeyDefinition{
		Key: "a.refined_v1", EventType: "a.refined_v1", Schema: nativeSchema(),
		RefinedSubscription: true, ResourceType: "a.res", AuthTypes: []string{"user"},
		KeyTemplates: []KeyTemplate{
			{Template: "a.refined_v1/id/{id}", Example: "a.refined_v1/id/1", SelectorKey: "id", PathSegment: "id", AuthTypes: []string{"user"}},
			{Template: "a.refined_v1/owner/me", Example: "a.refined_v1/owner/me", SelectorKey: "owner", PathSegment: "owner", FixedValue: "me", AuthTypes: []string{"user"}},
		},
	})

	const et = "a.validate.good_v1"
	RegisterFilterMeta(et, FilterMeta{
		Supported: true, LogicOps: []string{"and", "or"}, Operators: []string{"eq", "in"},
		Operands: []FilterOperandMeta{{Key: "sender", Operators: []string{"eq"}}},
	})
	// resetRegistry (t.Cleanup above) clears filter metas too, so no explicit
	// meta reset is needed — and re-registering et would trip the duplicate panic.

	if err := Validate(); err != nil {
		t.Fatalf("well-formed catalog must validate, got: %v", err)
	}
}

// TestValidate_CatchesDuplicatePathSegment seeds a base key with two templates
// sharing a path segment — allowed by RegisterKey, rejected by Validate.
func TestValidate_CatchesDuplicatePathSegment(t *testing.T) {
	resetRegistry()
	t.Cleanup(resetRegistry)

	RegisterKey(KeyDefinition{
		Key: "b.dup_v1", EventType: "b.dup_v1", Schema: nativeSchema(),
		RefinedSubscription: true, ResourceType: "b.res", AuthTypes: []string{"user"},
		KeyTemplates: []KeyTemplate{
			{Template: "b.dup_v1/id/{id}", Example: "b.dup_v1/id/1", SelectorKey: "id_a", PathSegment: "id", AuthTypes: []string{"user"}},
			{Template: "b.dup_v1/id/{id}", Example: "b.dup_v1/id/1", SelectorKey: "id_b", PathSegment: "id", AuthTypes: []string{"user"}},
		},
	})

	err := Validate()
	if err == nil {
		t.Fatal("expected a validation error for a duplicate template path segment")
	}
	if !strings.Contains(err.Error(), "duplicate template path segment") {
		t.Errorf("error = %v, want it to name the duplicate path segment", err)
	}
}

// TestValidate_CatchesPlaceholderSchema seeds a Custom schema whose raw body is
// an empty object — non-empty bytes (so RegisterKey accepts it) but no schema.
func TestValidate_CatchesPlaceholderSchema(t *testing.T) {
	resetRegistry()
	t.Cleanup(resetRegistry)

	RegisterKey(KeyDefinition{
		Key: "c.placeholder_v1", EventType: "c.placeholder_v1",
		Schema: SchemaDef{Custom: &SchemaSpec{Raw: []byte("{}")}},
	})

	err := Validate()
	if err == nil {
		t.Fatal("expected a validation error for a placeholder/empty schema")
	}
	if !strings.Contains(err.Error(), "placeholder/empty schema") {
		t.Errorf("error = %v, want it to flag the placeholder schema", err)
	}
}

// TestValidate_CatchesInvalidFilterMeta seeds a filter meta whose operand
// references an operator outside the event_type's global operator set.
func TestValidate_CatchesInvalidFilterMeta(t *testing.T) {
	resetRegistry()
	t.Cleanup(resetRegistry)

	const et = "d.badmeta_v1"
	RegisterFilterMeta(et, FilterMeta{
		Supported: true, LogicOps: []string{"and"}, Operators: []string{"eq", "in"},
		Operands: []FilterOperandMeta{{Key: "sender", Operators: []string{"eq", "regex"}}},
	})
	// resetRegistry (t.Cleanup above) clears filter metas too; re-registering et
	// here would trip the duplicate-registration panic.

	err := Validate()
	if err == nil {
		t.Fatal("expected a validation error for an operand operator outside the global set")
	}
	if !strings.Contains(err.Error(), "outside the event_type operator set") {
		t.Errorf("error = %v, want it to flag the out-of-set operator", err)
	}
}
