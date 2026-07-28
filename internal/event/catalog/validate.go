// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package catalog

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Validate checks the registered catalog for integrity problems that
// RegisterKey and RegisterFilterMeta do not themselves reject, returning a
// single error listing every problem found or nil when the catalog is
// well-formed. It is meant to run over the fully-registered production catalog
// as a startup/CI guard.
//
// It detects:
//   - duplicate EventKeys;
//   - template conflicts: two KeyTemplates of one base key sharing a path
//     segment (ResolveEventKey would then always pick the first, silently
//     shadowing the second — RegisterKey does not check this);
//   - placeholder/empty schemas: a chosen schema spec that carries no Go type
//     and whose raw body is empty / "{}" / "null" (RegisterKey only checks the
//     raw body is non-empty bytes, so "{}" passes it);
//   - lifecycle collisions: a business event_type equal to a reserved
//     subscription lifecycle meta-event type, which would make the source layer
//     skip that lifecycle handler (RegisterKey does not know the reserved set);
//   - invalid Filter Meta: an operand operator outside its event_type's global
//     operator set, a logic op that is not and/or, or an operand list cap above
//     the hard cap.
func Validate() error {
	var problems []string

	seen := map[string]bool{}
	for _, def := range ListAll() {
		if seen[def.Key] {
			problems = append(problems, fmt.Sprintf("duplicate EventKey %q", def.Key))
		}
		seen[def.Key] = true

		problems = append(problems, validateTemplates(def)...)
		problems = append(problems, validateSchemaShape(def)...)
		problems = append(problems, validateLifecycleCollision(def)...)
	}

	problems = append(problems, validateFilterMetas()...)

	if len(problems) == 0 {
		return nil
	}
	sort.Strings(problems)
	return fmt.Errorf("catalog validation failed:\n  - %s", strings.Join(problems, "\n  - "))
}

// validateTemplates flags two templates of one base key that share a path
// segment.
func validateTemplates(def *KeyDefinition) []string {
	var problems []string
	seen := map[string]bool{}
	for _, tmpl := range def.KeyTemplates {
		if seen[tmpl.PathSegment] {
			problems = append(problems, fmt.Sprintf("EventKey %q: duplicate template path segment %q", def.Key, tmpl.PathSegment))
		}
		seen[tmpl.PathSegment] = true
	}
	return problems
}

// validateLifecycleCollision flags a business EventKey whose event_type collides
// with a reserved subscription lifecycle meta-event type. The source layer skips
// registering a lifecycle handler whose event_type a business key already owns
// (the SDK dispatcher panics on duplicate registration), so such a key would
// silently drop that lifecycle signal.
func validateLifecycleCollision(def *KeyDefinition) []string {
	if IsReservedLifecycleEventType(def.EventType) {
		return []string{fmt.Sprintf("EventKey %q: event_type %q collides with a reserved subscription lifecycle event type; the lifecycle handler for it would be skipped", def.Key, def.EventType)}
	}
	return nil
}

// validateSchemaShape flags a missing or placeholder/empty schema.
func validateSchemaShape(def *KeyDefinition) []string {
	spec := def.Schema.Native
	if spec == nil {
		spec = def.Schema.Custom
	}
	if spec == nil {
		return []string{fmt.Sprintf("EventKey %q: schema has neither Native nor Custom", def.Key)}
	}
	if spec.Type != nil {
		return nil // a Go type is always a concrete schema
	}
	if isPlaceholderRaw(spec.Raw) {
		return []string{fmt.Sprintf("EventKey %q: placeholder/empty schema (raw body declares no fields)", def.Key)}
	}
	return nil
}

// isPlaceholderRaw reports whether a raw schema body carries no schema at all:
// blank, an empty object, or null.
func isPlaceholderRaw(raw json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "{}" || trimmed == "null" {
		return true
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err == nil && len(obj) == 0 {
		return true
	}
	return false
}

// validateFilterMetas flags a Filter Meta whose operands reference an operator
// outside the event_type's declared operator set, a non-and/or logic op, or an
// operand list cap above the hard cap.
func validateFilterMetas() []string {
	types := make([]string, 0, len(filterMetaRegistry))
	for eventType := range filterMetaRegistry {
		types = append(types, eventType)
	}
	sort.Strings(types)

	var problems []string
	for _, eventType := range types {
		meta := filterMetaRegistry[eventType]
		if !meta.Supported {
			continue
		}
		global := map[string]bool{}
		for _, op := range meta.Operators {
			global[op] = true
		}
		for _, lo := range meta.LogicOps {
			if lo != "and" && lo != "or" {
				problems = append(problems, fmt.Sprintf("filter meta %q: logic op %q is not one of and/or", eventType, lo))
			}
		}
		for _, o := range meta.Operands {
			for _, op := range o.Operators {
				if !global[op] {
					problems = append(problems, fmt.Sprintf("filter meta %q: operand %q operator %q is outside the event_type operator set %v", eventType, o.Key, op, meta.Operators))
				}
			}
			if o.ListValueMax > FilterListValueCap {
				problems = append(problems, fmt.Sprintf("filter meta %q: operand %q ListValueMax %d exceeds the hard cap %d", eventType, o.Key, o.ListValueMax, FilterListValueCap))
			}
		}
	}
	return problems
}
