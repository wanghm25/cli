// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

// Package catalog is the single owner of the EventKey catalog: the key
// definitions and their schema/param/template vocabulary, the forward resolver
// (ResolveEventKey), the reverse codec (ReverseResolve), the template-auth
// gate (CheckTemplateAuthTypes), the per-event_type Filter Meta registry, and
// catalog self-validation (Validate).
//
// It imports internal/event/model (value objects), internal/event/schemas
// (schema rendering metadata), internal/core (identity), and the CLI error
// package — never the Lark SDK and never package event. Package event re-exports
// this package's symbols as thin facades/aliases so callers stay unchanged.
package catalog
