// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/event/catalog"
	"github.com/larksuite/cli/internal/event/model"
)

// This file re-exports the internal/event/catalog and internal/event/model
// symbols under package event so every existing event.* caller compiles
// unchanged. It is a pure facade: aliases carry no behavior and the functions
// only delegate.

// --- value objects (internal/event/model) ---

type (
	// RawEvent is owned by internal/event/model.
	RawEvent = model.RawEvent
	// APIClient is owned by internal/event/model.
	APIClient = model.APIClient
)

// --- catalog types (internal/event/catalog) ---

type (
	KeyDefinition     = catalog.KeyDefinition
	KeyTemplate       = catalog.KeyTemplate
	ResolvedEventKey  = catalog.ResolvedEventKey
	SubscriptionType  = catalog.SubscriptionType
	ParamType         = catalog.ParamType
	ParamValue        = catalog.ParamValue
	ParamDef          = catalog.ParamDef
	SchemaDef         = catalog.SchemaDef
	SchemaSpec        = catalog.SchemaSpec
	FilterMeta        = catalog.FilterMeta
	FilterOperandMeta = catalog.FilterOperandMeta
)

// --- catalog constants ---

const (
	DefaultBufferSize = catalog.DefaultBufferSize
	MaxBufferSize     = catalog.MaxBufferSize

	SubTypeEvent    = catalog.SubTypeEvent
	SubTypeCallback = catalog.SubTypeCallback

	ParamString = catalog.ParamString
	ParamEnum   = catalog.ParamEnum
	ParamMulti  = catalog.ParamMulti
	ParamBool   = catalog.ParamBool
	ParamInt    = catalog.ParamInt
)

// Filter hard caps, re-exported unexported so the package-event filter
// validator and its tests reference the catalog-owned values by their original
// lowercase names.
const (
	filterMaxDepth      = catalog.FilterMaxDepth
	filterMaxConditions = catalog.FilterMaxConditions
	filterMaxBytes      = catalog.FilterMaxBytes
	filterListValueCap  = catalog.FilterListValueCap
)

// --- catalog functions ---

// RegisterKey registers an EventKey definition. See catalog.RegisterKey.
func RegisterKey(def KeyDefinition) { catalog.RegisterKey(def) }

// Lookup returns the registered definition for key. See catalog.Lookup.
func Lookup(key string) (*KeyDefinition, bool) { return catalog.Lookup(key) }

// ListAll returns all registered definitions sorted by Key. See catalog.ListAll.
func ListAll() []*KeyDefinition { return catalog.ListAll() }

// ResolveEventKey resolves an EventKey string. See catalog.ResolveEventKey.
func ResolveEventKey(input string) (ResolvedEventKey, error) { return catalog.ResolveEventKey(input) }

// CheckTemplateAuthTypes enforces the template identity tier. See
// catalog.CheckTemplateAuthTypes.
func CheckTemplateAuthTypes(identity core.Identity, resolved ResolvedEventKey) error {
	return catalog.CheckTemplateAuthTypes(identity, resolved)
}

// FilterMetaFor returns the filter capability for an event_type. See
// catalog.FilterMetaFor.
func FilterMetaFor(eventType string) FilterMeta { return catalog.FilterMetaFor(eventType) }

// RegisterFilterMeta records an event_type's filter capability. See
// catalog.RegisterFilterMeta.
func RegisterFilterMeta(eventType string, meta FilterMeta) {
	catalog.RegisterFilterMeta(eventType, meta)
}

// ReverseResolve reconstructs a canonical EventKey from a remote event_type +
// target_resource. See catalog.ReverseResolve.
func ReverseResolve(eventType, targetResource string) (string, bool) {
	return catalog.ReverseResolve(eventType, targetResource)
}

// Validate checks the registered catalog for integrity problems. See
// catalog.Validate.
func Validate() error { return catalog.Validate() }

// ResetRegistryForTest clears the registry. See catalog.ResetRegistryForTest.
func ResetRegistryForTest() { catalog.ResetRegistryForTest() }

// UnregisterKeyForTest removes one key. See catalog.UnregisterKeyForTest.
func UnregisterKeyForTest(key string) { catalog.UnregisterKeyForTest(key) }
