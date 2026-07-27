// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package events

import (
	"testing"

	"github.com/larksuite/cli/internal/event"
)

// TestValidate_RealCatalog runs the catalog self-validation over the fully
// registered production catalog (populated by this package's init). A failure
// means a real EventKey or Filter Meta is malformed — duplicate/conflicting
// templates, a placeholder schema, or an out-of-set filter operator.
func TestValidate_RealCatalog(t *testing.T) {
	if err := event.Validate(); err != nil {
		t.Fatalf("production catalog failed validation: %v", err)
	}
}
