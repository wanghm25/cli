// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import "github.com/larksuite/cli/internal/event/model"

// This file re-exports the Filter value type (now owned by internal/event/model)
// under package event so every existing event.Filter / event.Equal caller — and
// the package-event filter validator and its tests — compile unchanged. It is a
// pure facade: aliases carry no behavior and Equal only delegates.

// --- Filter value type (internal/event/model) ---

type (
	// Filter is owned by internal/event/model.
	Filter = model.Filter
	// FilterNode is owned by internal/event/model.
	FilterNode = model.FilterNode
	// FilterCond is owned by internal/event/model.
	FilterCond = model.FilterCond
)

// Equal reports whether two filters are semantically equal. See model.Equal.
func Equal(a, b *Filter) bool { return model.Equal(a, b) }

// Filter DSL vocabulary, re-exported under the original lowercase names so the
// package-event filter validator, the schema example, and their tests reference
// the model-owned constants without spelling the wire strings themselves.
const (
	opEq       = model.OpEq
	opIn       = model.OpIn
	opContains = model.OpContains

	logicAnd = model.LogicAnd
	logicOr  = model.LogicOr
)
