// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package rules

import (
	"fmt"
	"sort"
	"strings"
	"time"

	imcatalog "github.com/larksuite/cli/internal/imcontract/catalog"
	"github.com/larksuite/cli/internal/qualitygate/manifest"
	"github.com/larksuite/cli/internal/qualitygate/report"
)

const (
	imContractCoverageRule = "im_contract_coverage"
	expectedIMLeafCommands = 60
)

func CheckIMContractCoverage(commandIndex manifest.Manifest, contracts []imcatalog.Contract, _ time.Time) []report.Diagnostic {
	leafKeys := imLeafCommandKeys(commandIndex)
	leafSet := make(map[string]struct{}, len(leafKeys))
	for _, key := range leafKeys {
		leafSet[key] = struct{}{}
	}
	contractSet := make(map[string]imcatalog.Contract, len(contracts))
	for _, contract := range contracts {
		contractSet[string(contract.Key)] = contract
	}

	var diags []report.Diagnostic
	if len(leafKeys) != expectedIMLeafCommands {
		diags = append(diags, imContractDiagnostic(
			"",
			fmt.Sprintf("IM leaf command count is %d, want %d", len(leafKeys), expectedIMLeafCommands),
		))
	}
	for _, key := range leafKeys {
		if _, ok := contractSet[key]; !ok {
			diags = append(diags, imContractDiagnostic(key, "IM leaf command has no completion contract"))
		}
	}
	for _, contract := range contracts {
		key := string(contract.Key)
		if _, ok := leafSet[key]; !ok {
			diags = append(diags, imContractDiagnostic(key, "IM contract key does not match a runnable leaf command"))
		}
		if contract.Strategy.Kind != imcatalog.ExemptionKind {
			continue
		}
		if contract.Exemption == nil {
			diags = append(diags, imContractDiagnostic(key, "IM contract exemption marker is missing"))
		}
	}
	return diags
}

func imLeafCommandKeys(commandIndex manifest.Manifest) []string {
	var candidates []string
	for _, cmd := range commandIndex.Commands {
		if cmd.Domain == "im" && cmd.Runnable {
			candidates = append(candidates, cmd.Path)
		}
	}
	sort.Strings(candidates)
	leaves := make([]string, 0, len(candidates))
	for _, path := range candidates {
		parent := false
		for _, other := range candidates {
			if other != path && strings.HasPrefix(other, path+" ") {
				parent = true
				break
			}
		}
		if !parent {
			leaves = append(leaves, path)
		}
	}
	return leaves
}

func imContractDiagnostic(commandPath, message string) report.Diagnostic {
	return report.Diagnostic{
		Rule:        imContractCoverageRule,
		Action:      report.ActionReject,
		File:        "command-index",
		Message:     message,
		SubjectType: "command",
		CommandPath: commandPath,
	}
}
