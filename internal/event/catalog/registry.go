// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package catalog

import (
	"fmt"
	"sort"
	"sync"
)

var (
	keys   = map[string]*KeyDefinition{}
	frozen bool // guarded by mu; see Freeze
	mu     sync.RWMutex
)

// Freeze marks the registry immutable: after Freeze, RegisterKey and
// RegisterFilterMeta panic. Registration is a package-init-time activity — each
// business layer declares its keys and filter-meta from an init — so Freeze
// draws an explicit line at the end of that phase, turning a late (and possibly
// racy) registration into a fail-fast programming-error panic instead of a
// silent overwrite or an unvalidated definition slipping in after the catalog
// is in use. It is idempotent and independent of the copy-on-read guarantee:
// Lookup/ListAll/FilterMetaFor always hand back independent copies, so the
// catalog is an immutable fact source whether or not Freeze has been called.
func Freeze() {
	mu.Lock()
	defer mu.Unlock()
	frozen = true
}

// RegisterKey panics on registration after Freeze, duplicate Key, empty
// EventType, or schema/process contract violations.
func RegisterKey(def KeyDefinition) {
	mu.Lock()
	defer mu.Unlock()

	if frozen {
		panic(fmt.Sprintf("RegisterKey(%q) after the catalog was frozen: every key must be registered at init time, before Freeze", def.Key))
	}
	if _, exists := keys[def.Key]; exists {
		panic(fmt.Sprintf("duplicate EventKey: %s", def.Key))
	}
	if def.EventType == "" {
		panic(fmt.Sprintf("EventKey %s: EventType must not be empty", def.Key))
	}

	if def.SubscriptionType == "" {
		def.SubscriptionType = SubTypeEvent
	}
	if def.SubscriptionType != SubTypeEvent && def.SubscriptionType != SubTypeCallback {
		panic(fmt.Sprintf("EventKey %s: SubscriptionType must be %q or %q; got %q",
			def.Key, SubTypeEvent, SubTypeCallback, def.SubscriptionType))
	}

	validateSchema(def)
	validateParams(def)
	validateAuth(def)
	validateRefinedSubscription(def)

	if def.BufferSize > MaxBufferSize {
		def.BufferSize = MaxBufferSize
	}
	if def.BufferSize <= 0 {
		def.BufferSize = DefaultBufferSize
	}
	if def.Workers <= 0 {
		def.Workers = 1
	}
	// Store a DEEP COPY, not &def: a struct parameter copies only the slice/map/
	// json.RawMessage headers, so &def would still alias the caller's Scopes,
	// Params, Schema.Raw, … — letting a caller mutate the validated registry
	// entry after registration (and copy-on-read would faithfully hand back the
	// tampered data). cloneKeyDefinition severs every mutable field, mirroring
	// RegisterFilterMeta's clone-on-write, so the registry owns its own copy.
	keys[def.Key] = cloneKeyDefinition(&def)
}

// validateSchema: exactly one of Native/Custom; Native incompatible with Process.
func validateSchema(def KeyDefinition) {
	nativeSet := def.Schema.Native != nil
	customSet := def.Schema.Custom != nil
	if nativeSet && customSet {
		panic(fmt.Sprintf("EventKey %s: Schema.Native and Schema.Custom are mutually exclusive", def.Key))
	}
	if !nativeSet && !customSet {
		panic(fmt.Sprintf("EventKey %s: Schema requires either Native or Custom", def.Key))
	}
	if nativeSet && def.Process != nil {
		panic(fmt.Sprintf("EventKey %s: Schema.Native forbids Process (Process produces a complete shape — use Schema.Custom)", def.Key))
	}
	if spec := def.Schema.Native; spec != nil {
		validateSpec(def.Key, "Schema.Native", spec)
	}
	if spec := def.Schema.Custom; spec != nil {
		validateSpec(def.Key, "Schema.Custom", spec)
	}
}

func validateSpec(key, field string, s *SchemaSpec) {
	typeSet := s.Type != nil
	rawSet := len(s.Raw) > 0
	if typeSet == rawSet {
		panic(fmt.Sprintf("EventKey %s: %s requires exactly one of Type or Raw", key, field))
	}
}

func validateParams(def KeyDefinition) {
	for _, p := range def.Params {
		switch p.Type {
		case "", ParamString, ParamBool, ParamInt:
		case ParamEnum, ParamMulti:
			if len(p.Values) == 0 {
				panic(fmt.Sprintf("EventKey %s: param %q type %q requires Values", def.Key, p.Name, p.Type))
			}
			for _, v := range p.Values {
				if v.Desc == "" {
					panic(fmt.Sprintf("EventKey %s: param %q value %q requires non-empty Desc", def.Key, p.Name, v.Value))
				}
			}
		default:
			panic(fmt.Sprintf("EventKey %s: param %q has unknown type %q", def.Key, p.Name, p.Type))
		}
	}
}

func validateAuth(def KeyDefinition) {
	for _, t := range def.AuthTypes {
		if t != "user" && t != "bot" {
			panic(fmt.Sprintf("EventKey %s: AuthTypes elements must be \"user\" or \"bot\"; got %q", def.Key, t))
		}
	}
}

// validateRefinedSubscription: RefinedSubscription requires non-empty
// KeyTemplates; each template requires non-empty PathSegment/SelectorKey and
// AuthTypes restricted to {"user","bot"}.
func validateRefinedSubscription(def KeyDefinition) {
	if !def.RefinedSubscription {
		return
	}
	if len(def.KeyTemplates) == 0 {
		panic(fmt.Sprintf("EventKey %s: RefinedSubscription requires non-empty KeyTemplates", def.Key))
	}
	for i, tmpl := range def.KeyTemplates {
		if tmpl.PathSegment == "" {
			panic(fmt.Sprintf("EventKey %s: KeyTemplates[%d] PathSegment must not be empty", def.Key, i))
		}
		if tmpl.SelectorKey == "" {
			panic(fmt.Sprintf("EventKey %s: KeyTemplates[%d] SelectorKey must not be empty", def.Key, i))
		}
		for _, t := range tmpl.AuthTypes {
			if t != "user" && t != "bot" {
				panic(fmt.Sprintf("EventKey %s: KeyTemplates[%d] AuthTypes elements must be \"user\" or \"bot\"; got %q", def.Key, i, t))
			}
		}
	}
}

// Lookup returns an independent deep copy of the registered definition for key.
// Mutating the returned value (or anything reachable through it) can never
// reach back into the registry, so a caller cannot alter a registered
// KeyDefinition after the fact and bypass RegisterKey's validation — the
// catalog stays the single, immutable source of truth. See cloneKeyDefinition
// for exactly which fields are deep-copied vs shared.
func Lookup(key string) (*KeyDefinition, bool) {
	mu.RLock()
	defer mu.RUnlock()
	def, ok := keys[key]
	if !ok {
		return nil, false
	}
	return cloneKeyDefinition(def), true
}

// ListAll returns independent deep copies of all KeyDefinitions sorted by Key.
// As with Lookup, mutating any returned element cannot affect the registry.
func ListAll() []*KeyDefinition {
	mu.RLock()
	defer mu.RUnlock()
	result := make([]*KeyDefinition, 0, len(keys))
	for _, def := range keys {
		result = append(result, cloneKeyDefinition(def))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})
	return result
}

func resetRegistry() {
	mu.Lock()
	defer mu.Unlock()
	keys = map[string]*KeyDefinition{}
	filterMetaRegistry = map[string]FilterMeta{}
	frozen = false
}

func ResetRegistryForTest() { resetRegistry() }

// UnregisterKeyForTest removes one key — use this (not Reset) in tests with synthetic keys
// alongside production keys to keep -count=N reruns idempotent.
func UnregisterKeyForTest(key string) {
	mu.Lock()
	defer mu.Unlock()
	delete(keys, key)
}
