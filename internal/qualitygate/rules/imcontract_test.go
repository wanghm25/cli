// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package rules

import (
	"fmt"
	"strings"
	"testing"
	"time"

	imcatalog "github.com/larksuite/cli/internal/imcontract/catalog"
	qdiff "github.com/larksuite/cli/internal/qualitygate/diff"
	"github.com/larksuite/cli/internal/qualitygate/manifest"
	"github.com/larksuite/cli/internal/qualitygate/report"
)

func TestIMLeafCommandsExcludeParentsAndOtherDomains(t *testing.T) {
	index := manifest.Manifest{Commands: []manifest.Command{
		{Path: "im chat", Domain: "im", Runnable: true},
		{Path: "im chat get", Domain: "im", Runnable: true},
		{Path: "im chat list", Domain: "im", Runnable: false},
		{Path: "docs chat get", Domain: "docs", Runnable: true},
	}}
	got := imLeafCommandKeys(index)
	if len(got) != 1 || got[0] != "im chat get" {
		t.Fatalf("IM leaves = %#v, want only runnable child", got)
	}
}

func TestIMContractCoverageReportsMissingAndStaleKeys(t *testing.T) {
	index, contracts := completeIMCoverageFixture()
	contracts = contracts[1:]
	contracts = append(contracts, imcatalog.Contract{
		Key: "im stale command", Strategy: imcatalog.Strategy{Kind: imcatalog.EntityReadKind},
	})
	diags := CheckIMContractCoverage(index, contracts, time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC))
	if !hasIMContractDiagnostic(diags, "im resource command00", "no completion contract") {
		t.Fatalf("missing-command diagnostic absent: %#v", diags)
	}
	if !hasIMContractDiagnostic(diags, "im stale command", "does not match") {
		t.Fatalf("stale-key diagnostic absent: %#v", diags)
	}
}

func TestIMContractCoverageReportsMissingExemptionMarker(t *testing.T) {
	index, contracts := completeIMCoverageFixture()
	contracts[0] = imcatalog.Contract{
		Key:      contracts[0].Key,
		Strategy: imcatalog.Strategy{Kind: imcatalog.ExemptionKind},
	}
	diags := CheckIMContractCoverage(index, contracts, time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC))
	if !hasIMContractDiagnostic(diags, string(contracts[0].Key), "exemption marker is missing") {
		t.Fatalf("missing exemption marker diagnostic absent: %#v", diags)
	}
}

func TestIMContractCoverageReportsMissingIMDomain(t *testing.T) {
	index := manifest.Manifest{Commands: []manifest.Command{
		{Path: "docs +fetch", Domain: "docs", Runnable: true},
	}}
	if leaves := imLeafCommandKeys(index); len(leaves) != 0 {
		t.Fatalf("IM leaves = %#v, want none", leaves)
	}
	diags := CheckIMContractCoverage(index, imcatalog.All(), time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC))
	if !hasIMContractDiagnostic(diags, "", "IM leaf command count is 0, want 60") {
		t.Fatalf("missing-domain diagnostic absent: %#v", diags)
	}
}

func TestIMContractCoverageDiagnosticIsNotChangedFileFiltered(t *testing.T) {
	diag := imContractDiagnostic("im +chat-list", "missing")
	got := filterPRDiagnostics(
		".",
		"origin/main",
		qdiff.FromChangedFiles([]string{"skills/lark-doc/SKILL.md"}),
		manifest.Manifest{},
		[]report.Diagnostic{diag},
	)
	if len(got) != 1 || got[0].Rule != imContractCoverageRule {
		t.Fatalf("global IM coverage diagnostic was filtered: %#v", got)
	}
}

func completeIMCoverageFixture() (manifest.Manifest, []imcatalog.Contract) {
	index := manifest.Manifest{SchemaVersion: 1}
	contracts := make([]imcatalog.Contract, 0, expectedIMLeafCommands)
	for i := 0; i < expectedIMLeafCommands; i++ {
		key := fmt.Sprintf("im resource command%02d", i)
		index.Commands = append(index.Commands, manifest.Command{Path: key, Domain: "im", Runnable: true})
		contracts = append(contracts, imcatalog.Contract{
			Key: imcatalog.ContractKey(key), Strategy: imcatalog.Strategy{Kind: imcatalog.EntityReadKind},
		})
	}
	return index, contracts
}

func hasIMContractDiagnostic(diags []report.Diagnostic, key, text string) bool {
	for _, diag := range diags {
		if diag.CommandPath == key && strings.Contains(diag.Message, text) {
			return true
		}
	}
	return false
}
