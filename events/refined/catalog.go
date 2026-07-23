// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

// Package refined registers refined-subscription base EventKeys.
package refined

import (
	_ "embed"
	"encoding/json"

	"github.com/larksuite/cli/internal/event"
)

//go:embed refined_keys_mock.json
var refinedKeysMockJSON []byte

// Keys returns the MOCK refined-subscription catalog.
// meta-swap: replace this embedded mock with the fetch_meta-driven catalog
// once the new meta ships. Registry/consumers unchanged.
func Keys() []event.KeyDefinition {
	var defs []event.KeyDefinition
	if err := json.Unmarshal(refinedKeysMockJSON, &defs); err != nil {
		panic("refined mock catalog: " + err.Error())
	}
	return defs
}
