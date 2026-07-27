// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

// Package model holds the cross-cutting event value objects shared across the
// event subsystem. It is the leaf of the event dependency graph: it imports
// only the standard library — never the Lark SDK and never package event or
// catalog — so any layer can depend on it without forming an import cycle.
package model
