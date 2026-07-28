// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/larksuite/cli/internal/event/model"
)

func mustPanic(t *testing.T, substring string) {
	t.Helper()
	r := recover()
	if r == nil {
		t.Fatal("expected panic, got none")
	}
	msg, _ := r.(string)
	if msg == "" {
		if err, ok := r.(error); ok {
			msg = err.Error()
		} else {
			msg = fmt.Sprintf("%v", r)
		}
	}
	if !strings.Contains(msg, substring) {
		t.Errorf("panic %q does not contain %q", msg, substring)
	}
}

type emptyOut struct {
	A string `json:"a"`
}

func nativeSchema() SchemaDef {
	return SchemaDef{Native: &SchemaSpec{Type: reflect.TypeOf(emptyOut{})}}
}

func customSchema() SchemaDef {
	return SchemaDef{Custom: &SchemaSpec{Type: reflect.TypeOf(emptyOut{})}}
}

func customProcess() func(context.Context, model.APIClient, *model.RawEvent, map[string]string) (json.RawMessage, error) {
	return func(context.Context, model.APIClient, *model.RawEvent, map[string]string) (json.RawMessage, error) {
		return nil, nil
	}
}

func TestRegisterKey_NativeOnly(t *testing.T) {
	resetRegistry()
	RegisterKey(KeyDefinition{
		Key:       "t.native",
		EventType: "t.native",
		Schema:    nativeSchema(),
	})
	def, ok := Lookup("t.native")
	if !ok {
		t.Fatal("Lookup failed")
	}
	if def.Schema.Native == nil {
		t.Fatal("Native not stored")
	}
	if def.Process != nil {
		t.Error("Process should be nil for Native")
	}
}

func TestRegisterKey_CustomWithProcess(t *testing.T) {
	resetRegistry()
	RegisterKey(KeyDefinition{
		Key:       "t.custom",
		EventType: "t.custom",
		Schema:    customSchema(),
		Process:   customProcess(),
	})
	def, ok := Lookup("t.custom")
	if !ok {
		t.Fatal("Lookup failed")
	}
	if def.Schema.Custom == nil {
		t.Fatal("Custom not stored")
	}
	if def.Process == nil {
		t.Error("Process should be set")
	}
}

func TestRegisterKey_DuplicatePanics(t *testing.T) {
	resetRegistry()
	RegisterKey(KeyDefinition{Key: "t.dup", EventType: "t.dup", Schema: nativeSchema()})
	defer mustPanic(t, "duplicate EventKey")
	RegisterKey(KeyDefinition{Key: "t.dup", EventType: "t.dup", Schema: nativeSchema()})
}

// TestRegisterFilterMeta_DuplicatePanics (fail-fast #5b): a second FilterMeta
// registration for an already-registered event_type is a programming error that
// must fail fast, mirroring RegisterKey — never a silent overwrite.
func TestRegisterFilterMeta_DuplicatePanics(t *testing.T) {
	resetRegistry()
	t.Cleanup(resetRegistry)
	const et = "t.filter.dup_v1"
	RegisterFilterMeta(et, FilterMeta{Supported: true})
	defer mustPanic(t, "duplicate FilterMeta registration for event_type: "+et)
	RegisterFilterMeta(et, FilterMeta{Supported: true})
}

// TestUnregisterFilterMetaForTest_AllowsReRegistration: the panic companion lets
// a test undo a synthetic capability and seed it again (idempotent -count=N).
func TestUnregisterFilterMetaForTest_AllowsReRegistration(t *testing.T) {
	resetRegistry()
	t.Cleanup(resetRegistry)
	const et = "t.filter.rereg_v1"
	RegisterFilterMeta(et, FilterMeta{Supported: true})
	UnregisterFilterMetaForTest(et)
	if FilterMetaFor(et).Supported {
		t.Fatal("UnregisterFilterMetaForTest must remove the capability")
	}
	RegisterFilterMeta(et, FilterMeta{Supported: true}) // must not panic
	if !FilterMetaFor(et).Supported {
		t.Fatal("re-registration after unregister must take effect")
	}
}

func TestRegisterKey_EmptyEventTypePanics(t *testing.T) {
	resetRegistry()
	defer mustPanic(t, "EventType must not be empty")
	RegisterKey(KeyDefinition{Key: "t.no_type", Schema: nativeSchema()})
}

func TestRegisterKey_PanicsWhenBothSchemasSet(t *testing.T) {
	resetRegistry()
	defer mustPanic(t, "mutually exclusive")
	RegisterKey(KeyDefinition{
		Key:       "t.both",
		EventType: "t.both",
		Schema: SchemaDef{
			Native: &SchemaSpec{Type: reflect.TypeOf(emptyOut{})},
			Custom: &SchemaSpec{Type: reflect.TypeOf(emptyOut{})},
		},
	})
}

func TestRegisterKey_PanicsWhenNoSchemaSet(t *testing.T) {
	resetRegistry()
	defer mustPanic(t, "Schema requires either Native or Custom")
	RegisterKey(KeyDefinition{Key: "t.empty", EventType: "t.empty"})
}

func TestRegisterKey_PanicsWhenNativeWithProcess(t *testing.T) {
	resetRegistry()
	defer mustPanic(t, "Schema.Native forbids Process")
	RegisterKey(KeyDefinition{
		Key:       "t.badcombo",
		EventType: "t.badcombo",
		Schema:    nativeSchema(),
		Process:   customProcess(),
	})
}

func TestRegisterKey_PanicsWhenSpecHasBothTypeAndRaw(t *testing.T) {
	resetRegistry()
	defer mustPanic(t, "requires exactly one of Type or Raw")
	RegisterKey(KeyDefinition{
		Key:       "t.bothsrc",
		EventType: "t.bothsrc",
		Schema: SchemaDef{
			Custom: &SchemaSpec{Type: reflect.TypeOf(emptyOut{}), Raw: json.RawMessage(`{}`)},
		},
	})
}

func TestRegisterKey_PanicsWhenSpecHasNeitherTypeNorRaw(t *testing.T) {
	resetRegistry()
	defer mustPanic(t, "requires exactly one of Type or Raw")
	RegisterKey(KeyDefinition{
		Key:       "t.nosrc",
		EventType: "t.nosrc",
		Schema: SchemaDef{
			Custom: &SchemaSpec{},
		},
	})
}

func TestRegisterKey_ParamMultiRequiresValues(t *testing.T) {
	resetRegistry()
	defer mustPanic(t, "requires Values")
	RegisterKey(KeyDefinition{
		Key:       "t.paramnovalues",
		EventType: "t.paramnovalues",
		Schema:    nativeSchema(),
		Params:    []ParamDef{{Name: "fields", Type: ParamMulti}},
	})
}

func TestRegisterKey_ParamEnumRequiresValues(t *testing.T) {
	resetRegistry()
	defer mustPanic(t, "requires Values")
	RegisterKey(KeyDefinition{
		Key:       "t.enumnovalues",
		EventType: "t.enumnovalues",
		Schema:    nativeSchema(),
		Params:    []ParamDef{{Name: "mode", Type: ParamEnum}},
	})
}

func TestRegisterKey_ParamValueRequiresDesc(t *testing.T) {
	resetRegistry()
	defer mustPanic(t, "requires non-empty Desc")
	RegisterKey(KeyDefinition{
		Key:       "t.paramdesc",
		EventType: "t.paramdesc",
		Schema:    nativeSchema(),
		Params: []ParamDef{{
			Name:   "f",
			Type:   ParamEnum,
			Values: []ParamValue{{Value: "x"}},
		}},
	})
}

func TestRegisterKey_UnknownParamType(t *testing.T) {
	resetRegistry()
	defer mustPanic(t, "unknown type")
	RegisterKey(KeyDefinition{
		Key:       "t.badtype",
		EventType: "t.badtype",
		Schema:    nativeSchema(),
		Params:    []ParamDef{{Name: "x", Type: ParamType("wtf")}},
	})
}

func TestRegisterKey_InvalidAuthTypesPanics(t *testing.T) {
	resetRegistry()
	defer mustPanic(t, "AuthTypes elements must be")
	RegisterKey(KeyDefinition{
		Key:       "t.badauth",
		EventType: "t.badauth",
		Schema:    nativeSchema(),
		AuthTypes: []string{"invalid"},
	})
}

func TestRegisterKey_ValidAuthTypes(t *testing.T) {
	resetRegistry()
	RegisterKey(KeyDefinition{Key: "u.e", EventType: "u.e", Schema: nativeSchema(), AuthTypes: []string{"user"}})
	RegisterKey(KeyDefinition{Key: "b.e", EventType: "b.e", Schema: nativeSchema(), AuthTypes: []string{"bot"}})
	RegisterKey(KeyDefinition{Key: "ub.e", EventType: "ub.e", Schema: nativeSchema(), AuthTypes: []string{"bot", "user"}})
	RegisterKey(KeyDefinition{Key: "na.e", EventType: "na.e", Schema: nativeSchema()})
}

func TestListAll_SortedByKey(t *testing.T) {
	resetRegistry()
	RegisterKey(KeyDefinition{Key: "z.event", EventType: "z", Schema: nativeSchema()})
	RegisterKey(KeyDefinition{Key: "a.event", EventType: "a", Schema: nativeSchema()})
	RegisterKey(KeyDefinition{Key: "m.event", EventType: "m", Schema: nativeSchema()})
	all := ListAll()
	if len(all) != 3 || all[0].Key != "a.event" || all[1].Key != "m.event" || all[2].Key != "z.event" {
		t.Errorf("keys not sorted: %v", []string{all[0].Key, all[1].Key, all[2].Key})
	}
}

func TestBufferSize_Clamped(t *testing.T) {
	resetRegistry()
	RegisterKey(KeyDefinition{
		Key: "big", EventType: "big", Schema: nativeSchema(),
		BufferSize: 5000,
	})
	def, _ := Lookup("big")
	if def.BufferSize != MaxBufferSize {
		t.Errorf("BufferSize = %d, want %d", def.BufferSize, MaxBufferSize)
	}
}

func TestRegisterKey_SubscriptionTypeDefaultsToEvent(t *testing.T) {
	const key = "test.subtype.default"
	RegisterKey(KeyDefinition{
		Key:       key,
		EventType: key,
		Schema:    SchemaDef{Native: &SchemaSpec{Raw: []byte(`{"type":"object"}`)}},
	})
	defer UnregisterKeyForTest(key)

	def, ok := Lookup(key)
	if !ok {
		t.Fatalf("Lookup(%q) failed", key)
	}
	if def.SubscriptionType != SubTypeEvent {
		t.Errorf("SubscriptionType = %q, want %q", def.SubscriptionType, SubTypeEvent)
	}
	if def.SingleConsumer {
		t.Errorf("SingleConsumer = true, want false (default)")
	}
}

func TestRegisterKey_SubscriptionTypeCallbackPreserved(t *testing.T) {
	const key = "test.subtype.callback"
	RegisterKey(KeyDefinition{
		Key:              key,
		EventType:        key,
		SubscriptionType: SubTypeCallback,
		SingleConsumer:   true,
		Schema:           SchemaDef{Native: &SchemaSpec{Raw: []byte(`{"type":"object"}`)}},
	})
	defer UnregisterKeyForTest(key)

	def, _ := Lookup(key)
	if def.SubscriptionType != SubTypeCallback {
		t.Errorf("SubscriptionType = %q, want %q", def.SubscriptionType, SubTypeCallback)
	}
	if !def.SingleConsumer {
		t.Errorf("SingleConsumer = false, want true")
	}
}

func TestRegisterKey_InvalidSubscriptionTypePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic for invalid SubscriptionType")
		}
	}()
	RegisterKey(KeyDefinition{
		Key:              "test.subtype.bogus",
		EventType:        "test.subtype.bogus",
		SubscriptionType: "bogus",
		Schema:           SchemaDef{Native: &SchemaSpec{Raw: []byte(`{"type":"object"}`)}},
	})
}

// assertPanics runs fn and fails the test if it does not panic.
func assertPanics(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	fn()
}

func TestRegisterKey_RefinedValidation(t *testing.T) {
	defer UnregisterKeyForTest("x.y.created_v1")

	// refined key missing templates -> panic
	bad := KeyDefinition{Key: "x.y.created_v1", EventType: "x.y.created_v1", Schema: nativeSchema(),
		AuthTypes: []string{"user"}, RefinedSubscription: true}
	assertPanics(t, func() { RegisterKey(bad) })

	// valid refined key registers successfully and is Lookup-able
	good := KeyDefinition{Key: "x.y.created_v1", EventType: "x.y.created_v1", ResourceType: "x.y", Schema: nativeSchema(),
		AuthTypes: []string{"user", "bot"}, RefinedSubscription: true,
		KeyTemplates: []KeyTemplate{{Template: "x.y.created_v1/z-id/{z_id}", Example: "x.y.created_v1/z-id/z1",
			SelectorKey: "z_id", PathSegment: "z-id", AuthTypes: []string{"user", "bot"}}}}
	RegisterKey(good)
	def, ok := Lookup("x.y.created_v1")
	if !ok || !def.RefinedSubscription || len(def.KeyTemplates) != 1 {
		t.Fatalf("refined key not registered correctly: %+v", def)
	}
}

// TestLookup_ResultDoesNotAliasRegistry proves the immutable-catalog guarantee
// (#17): a caller mutating a Lookup result — any slice, map, or the schema Raw
// bytes — cannot reach back into the registry, so a later Lookup is unaffected.
// This is what stops a caller from mutating a registered KeyDefinition after
// RegisterKey validated it.
func TestLookup_ResultDoesNotAliasRegistry(t *testing.T) {
	resetRegistry()
	t.Cleanup(resetRegistry)
	RegisterKey(KeyDefinition{
		Key:                   "t.immutable_v1",
		EventType:             "t.immutable_v1",
		ResourceType:          "t.immutable",
		Schema:                SchemaDef{Custom: &SchemaSpec{Raw: json.RawMessage(`{"type":"object"}`)}},
		AuthTypes:             []string{"user", "bot"},
		Scopes:                []string{"scope:one"},
		RequiredConsoleEvents: []string{"e1"},
		Params:                []ParamDef{{Name: "mode", Type: ParamEnum, Values: []ParamValue{{Value: "a", Desc: "A"}}}},
		RefinedSubscription:   true,
		KeyTemplates: []KeyTemplate{{
			Template: "t.immutable_v1/x-id/{x}", Example: "t.immutable_v1/x-id/x1",
			SelectorKey: "x_id", PathSegment: "x-id", AuthTypes: []string{"user"},
		}},
	})

	first, ok := Lookup("t.immutable_v1")
	if !ok {
		t.Fatal("Lookup failed")
	}
	// Mutate every mutable reference-typed part of the returned copy.
	first.Params[0].Name = "HACKED"
	first.Params[0].Values[0].Value = "HACKED"
	first.Scopes[0] = "HACKED"
	first.AuthTypes[0] = "HACKED"
	first.RequiredConsoleEvents[0] = "HACKED"
	first.Schema.Custom.Raw[0] = 'X'
	first.KeyTemplates[0].SelectorKey = "HACKED"
	first.KeyTemplates[0].AuthTypes[0] = "HACKED"

	second, _ := Lookup("t.immutable_v1")
	switch {
	case second.Params[0].Name != "mode":
		t.Errorf("Params[0].Name mutated via Lookup result: %q", second.Params[0].Name)
	case second.Params[0].Values[0].Value != "a":
		t.Errorf("Params[0].Values mutated: %q", second.Params[0].Values[0].Value)
	case second.Scopes[0] != "scope:one":
		t.Errorf("Scopes mutated: %q", second.Scopes[0])
	case second.AuthTypes[0] != "user":
		t.Errorf("AuthTypes mutated: %q", second.AuthTypes[0])
	case second.RequiredConsoleEvents[0] != "e1":
		t.Errorf("RequiredConsoleEvents mutated: %q", second.RequiredConsoleEvents[0])
	case string(second.Schema.Custom.Raw) != `{"type":"object"}`:
		t.Errorf("Schema.Custom.Raw mutated: %s", second.Schema.Custom.Raw)
	case second.KeyTemplates[0].SelectorKey != "x_id":
		t.Errorf("KeyTemplates[0].SelectorKey mutated: %q", second.KeyTemplates[0].SelectorKey)
	case second.KeyTemplates[0].AuthTypes[0] != "user":
		t.Errorf("KeyTemplates[0].AuthTypes mutated: %q", second.KeyTemplates[0].AuthTypes[0])
	}
}

// TestRegisterKey_DoesNotAliasCallerDefinition proves the WRITE-side
// immutability: a caller that mutates the ORIGINAL KeyDefinition (its Scopes/
// Params/Schema.Raw/…) AFTER registration cannot tamper with the validated
// registry entry. Without the clone-on-write in RegisterKey, storing &def would
// alias the caller's slices/maps (a struct param copies only slice/map headers),
// and copy-on-read would then faithfully hand back the tampered data.
func TestRegisterKey_DoesNotAliasCallerDefinition(t *testing.T) {
	resetRegistry()
	t.Cleanup(resetRegistry)
	def := KeyDefinition{
		Key:                   "t.writeimm_v1",
		EventType:             "t.writeimm_v1",
		ResourceType:          "t.writeimm",
		Schema:                SchemaDef{Custom: &SchemaSpec{Raw: json.RawMessage(`{"type":"object"}`)}},
		AuthTypes:             []string{"user", "bot"},
		Scopes:                []string{"scope:one"},
		RequiredConsoleEvents: []string{"e1"},
		Params:                []ParamDef{{Name: "mode", Type: ParamEnum, Values: []ParamValue{{Value: "a", Desc: "A"}}}},
		RefinedSubscription:   true,
		KeyTemplates: []KeyTemplate{{
			Template: "t.writeimm_v1/x-id/{x}", Example: "t.writeimm_v1/x-id/x1",
			SelectorKey: "x_id", PathSegment: "x-id", AuthTypes: []string{"user"},
		}},
	}
	RegisterKey(def)

	// Mutate every mutable reference-typed part of the caller's ORIGINAL def
	// AFTER registration — a clone-on-write registry must be unaffected.
	def.Scopes[0] = "HACKED"
	def.AuthTypes[0] = "HACKED"
	def.RequiredConsoleEvents[0] = "HACKED"
	def.Params[0].Name = "HACKED"
	def.Params[0].Values[0].Value = "HACKED"
	def.Schema.Custom.Raw[0] = 'X'
	def.KeyTemplates[0].SelectorKey = "HACKED"
	def.KeyTemplates[0].AuthTypes[0] = "HACKED"

	got, ok := Lookup("t.writeimm_v1")
	if !ok {
		t.Fatal("Lookup failed")
	}
	switch {
	case got.Scopes[0] != "scope:one":
		t.Errorf("Scopes tampered via caller's def after registration: %q", got.Scopes[0])
	case got.AuthTypes[0] != "user":
		t.Errorf("AuthTypes tampered: %q", got.AuthTypes[0])
	case got.RequiredConsoleEvents[0] != "e1":
		t.Errorf("RequiredConsoleEvents tampered: %q", got.RequiredConsoleEvents[0])
	case got.Params[0].Name != "mode":
		t.Errorf("Params[0].Name tampered: %q", got.Params[0].Name)
	case got.Params[0].Values[0].Value != "a":
		t.Errorf("Params[0].Values tampered: %q", got.Params[0].Values[0].Value)
	case string(got.Schema.Custom.Raw) != `{"type":"object"}`:
		t.Errorf("Schema.Custom.Raw tampered: %s", got.Schema.Custom.Raw)
	case got.KeyTemplates[0].SelectorKey != "x_id":
		t.Errorf("KeyTemplates[0].SelectorKey tampered: %q", got.KeyTemplates[0].SelectorKey)
	case got.KeyTemplates[0].AuthTypes[0] != "user":
		t.Errorf("KeyTemplates[0].AuthTypes tampered: %q", got.KeyTemplates[0].AuthTypes[0])
	}
}

// TestListAll_ResultDoesNotAliasRegistry proves the same immutability for the
// ListAll read path.
func TestListAll_ResultDoesNotAliasRegistry(t *testing.T) {
	resetRegistry()
	t.Cleanup(resetRegistry)
	RegisterKey(KeyDefinition{
		Key: "t.listimm", EventType: "t.listimm", Schema: nativeSchema(),
		AuthTypes: []string{"user"},
	})
	all := ListAll()
	if len(all) != 1 {
		t.Fatalf("ListAll len = %d, want 1", len(all))
	}
	all[0].AuthTypes[0] = "HACKED"
	if again := ListAll(); again[0].AuthTypes[0] != "user" {
		t.Errorf("registry AuthTypes mutated via ListAll result: %q", again[0].AuthTypes[0])
	}
}

// TestFreeze_RegisterKeyPanics: after Freeze, a late RegisterKey is a fail-fast
// programming error, not a silent post-registration mutation of the catalog.
func TestFreeze_RegisterKeyPanics(t *testing.T) {
	resetRegistry()
	t.Cleanup(resetRegistry)
	Freeze()
	defer mustPanic(t, "after the catalog was frozen")
	RegisterKey(KeyDefinition{Key: "t.afterfreeze", EventType: "t.afterfreeze", Schema: nativeSchema()})
}

// TestFreeze_RegisterFilterMetaPanics: the same fail-fast guard covers the
// filter-meta registry.
func TestFreeze_RegisterFilterMetaPanics(t *testing.T) {
	resetRegistry()
	t.Cleanup(resetRegistry)
	Freeze()
	defer mustPanic(t, "after the catalog was frozen")
	RegisterFilterMeta("t.freeze.meta_v1", FilterMeta{Supported: true})
}

// TestFreeze_ReadsStillWork: Freeze closes the door on registration only; the
// read paths keep working (and keep returning independent copies).
func TestFreeze_ReadsStillWork(t *testing.T) {
	resetRegistry()
	t.Cleanup(resetRegistry)
	RegisterKey(KeyDefinition{Key: "t.frozen.read", EventType: "t.frozen.read", Schema: nativeSchema()})
	Freeze()
	if _, ok := Lookup("t.frozen.read"); !ok {
		t.Fatal("Lookup must still work after Freeze")
	}
	if len(ListAll()) == 0 {
		t.Fatal("ListAll must still work after Freeze")
	}
}

// TestFilterMetaFor_ResultDoesNotAliasRegistry proves the filter-meta read is
// also an immutable copy: mutating the returned Operators/Operands cannot alter
// the registered capability.
func TestFilterMetaFor_ResultDoesNotAliasRegistry(t *testing.T) {
	resetRegistry()
	t.Cleanup(resetRegistry)
	const et = "t.filter.immutable_v1"
	RegisterFilterMeta(et, FilterMeta{
		Supported: true,
		LogicOps:  []string{"and"},
		Operators: []string{"eq"},
		Operands:  []FilterOperandMeta{{Key: "sender", Operators: []string{"eq"}}},
	})
	first := FilterMetaFor(et)
	first.LogicOps[0] = "HACKED"
	first.Operators[0] = "HACKED"
	first.Operands[0].Key = "HACKED"
	first.Operands[0].Operators[0] = "HACKED"

	second := FilterMetaFor(et)
	if second.LogicOps[0] != "and" || second.Operators[0] != "eq" {
		t.Errorf("FilterMeta scalars mutated via result: %+v", second)
	}
	if second.Operands[0].Key != "sender" || second.Operands[0].Operators[0] != "eq" {
		t.Errorf("FilterMeta Operands mutated via result: %+v", second.Operands)
	}
}
