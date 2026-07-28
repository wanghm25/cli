// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT
//
// lark-cli — Feishu/Lark CLI tool (Go implementation).
package main

import (
	"os"

	"github.com/larksuite/cli/cmd"
	"github.com/larksuite/cli/internal/event/catalog"

	_ "github.com/larksuite/cli/extension/credential/env" // activate env credential provider
)

func main() {
	// Seal the EventKey / FilterMeta catalog once the process has started: in the
	// real binary every key and filter capability is registered from an init
	// (the events blank-import), so by the time main runs the registration phase
	// is over. Freeze turns any later (runtime) registration into a fail-fast
	// panic instead of a silent, unvalidated mutation of an in-use catalog. Tests
	// never run this main, so they can still register synthetic keys mid-process.
	catalog.Freeze()
	os.Exit(cmd.Execute())
}
