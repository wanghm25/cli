// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package catalog

import (
	"encoding/json"

	"github.com/larksuite/cli/internal/event/schemas"
)

// cloneKeyDefinition returns a deep copy of def whose mutable reference-typed
// fields — every slice, every map, and the schema Raw json.RawMessage — are
// independent of the registered definition. This is what makes Lookup/ListAll
// an immutable read: a caller may freely mutate the result without ever
// reaching back into the registry to alter a definition after RegisterKey
// validated it.
//
// The function-valued fields (NormalizeParams/Process/Match/PreConsume) and the
// reflect.Type in a SchemaSpec are immutable references shared by design —
// copying them would be meaningless (a func/Type value cannot be mutated in a
// way that corrupts the registry). Scalars are copied by the struct assignment.
func cloneKeyDefinition(def *KeyDefinition) *KeyDefinition {
	if def == nil {
		return nil
	}
	c := *def // scalars, func values, reflect.Type, and slice/map headers
	c.Params = cloneParams(def.Params)
	c.Scopes = cloneStrings(def.Scopes)
	c.AuthTypes = cloneStrings(def.AuthTypes)
	c.RequiredConsoleEvents = cloneStrings(def.RequiredConsoleEvents)
	c.KeyTemplates = cloneKeyTemplates(def.KeyTemplates)
	c.Schema = cloneSchemaDef(def.Schema)
	return &c
}

func cloneStrings(s []string) []string {
	if s == nil {
		return nil
	}
	out := make([]string, len(s))
	copy(out, s)
	return out
}

func cloneParams(ps []ParamDef) []ParamDef {
	if ps == nil {
		return nil
	}
	out := make([]ParamDef, len(ps))
	for i, p := range ps {
		p.Values = cloneParamValues(p.Values)
		out[i] = p
	}
	return out
}

func cloneParamValues(vs []ParamValue) []ParamValue {
	if vs == nil {
		return nil
	}
	out := make([]ParamValue, len(vs)) // ParamValue is all scalars
	copy(out, vs)
	return out
}

func cloneKeyTemplates(ts []KeyTemplate) []KeyTemplate {
	if ts == nil {
		return nil
	}
	out := make([]KeyTemplate, len(ts))
	for i, t := range ts {
		t.AuthTypes = cloneStrings(t.AuthTypes)
		out[i] = t
	}
	return out
}

func cloneSchemaDef(s SchemaDef) SchemaDef {
	return SchemaDef{
		Native:         cloneSchemaSpec(s.Native),
		Custom:         cloneSchemaSpec(s.Custom),
		FieldOverrides: cloneFieldOverrides(s.FieldOverrides),
	}
}

func cloneSchemaSpec(s *SchemaSpec) *SchemaSpec {
	if s == nil {
		return nil
	}
	c := *s // Type (reflect.Type) is an immutable reference, shared as-is
	if s.Raw != nil {
		c.Raw = make(json.RawMessage, len(s.Raw))
		copy(c.Raw, s.Raw)
	}
	return &c
}

func cloneFieldOverrides(m map[string]schemas.FieldMeta) map[string]schemas.FieldMeta {
	if m == nil {
		return nil
	}
	out := make(map[string]schemas.FieldMeta, len(m))
	for k, v := range m {
		v.Enum = cloneStrings(v.Enum)
		out[k] = v
	}
	return out
}
