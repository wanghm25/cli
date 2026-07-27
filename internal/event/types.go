// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

// Package event provides the event-consumption runtime: the dedup filter, the
// reconciler, the subscription client, and the CLI-owned Filter model with its
// SDK projection. The EventKey catalog is owned by internal/event/catalog and
// the cross-cutting value objects by internal/event/model; this package
// re-exports their symbols as thin facades/aliases (see catalog_facade.go) so
// existing callers keep using event.* unchanged.
package event

import (
	"context"
	"encoding/json"
)

// ProcessFunc is the KeyDefinition.Process signature, exposed as a named type
// for callers that build process functions. RawEvent and APIClient are the
// package-event aliases of the internal/event/model value objects.
type ProcessFunc = func(ctx context.Context, rt APIClient, raw *RawEvent, params map[string]string) (json.RawMessage, error)
