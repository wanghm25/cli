// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package imcontract

import (
	"time"

	"github.com/larksuite/cli/internal/imcontract/catalog"
)

func Lookup(key ContractKey) (Contract, bool) {
	return catalog.Lookup(key)
}

func All() []Contract {
	return catalog.All()
}

func ValidateRegistry(now time.Time) error {
	return catalog.ValidateRegistry(now)
}

func stringsFrom(field string) evidenceSpec {
	return evidenceSpec{Shape: evidenceStrings, Field: field}
}
