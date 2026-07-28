// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package domaincontract

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/larksuite/cli/lint/lintapi"
)

func gitTestCommand(t *testing.T, root string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func setupDomainDiffRepo(t *testing.T, target string) (root, base string) {
	t.Helper()
	root = t.TempDir()
	writeFile(t, root, "go.mod", "module example.com/domainfixture\n\ngo 1.23.0\n")
	writeFile(t, root, publicDomainsPath, "# public\npublic.example.com\n")
	writeFile(t, root, fixtureDomainsPath, "# fixtures\nfixture.example.com\n")
	writeFile(t, root, "policy_refs.go", "package sample\n\nvar APIHost = \"public.example.com\"\n")
	writeFile(t, root, "policy_refs_test.go", "package sample\n\nvar FixtureHost = \"fixture.example.com\"\n")
	writeFile(t, root, "target.go", target)

	gitTestCommand(t, root, "init", "-q")
	gitTestCommand(t, root, "config", "user.name", "Domain Contract Test")
	gitTestCommand(t, root, "config", "user.email", "domain-contract@example.com")
	gitTestCommand(t, root, "add", ".")
	gitTestCommand(t, root, "-c", "commit.gpgsign=false", "commit", "-qm", "base")
	return root, gitTestCommand(t, root, "rev-parse", "HEAD")
}

func commitDomainDiff(t *testing.T, root, message string) {
	t.Helper()
	gitTestCommand(t, root, "add", "-A")
	gitTestCommand(t, root, "-c", "commit.gpgsign=false", "commit", "-qm", message)
}

func violationsForRule(vs []lintapi.Violation, rule string) []lintapi.Violation {
	var out []lintapi.Violation
	for _, v := range vs {
		if v.Rule == rule {
			out = append(out, v)
		}
	}
	return out
}

func scanDomainDiff(t *testing.T, root, base string) []lintapi.Violation {
	t.Helper()
	vs, err := ScanRepoWithOptions(root, ScanOptions{ChangedFrom: base})
	if err != nil {
		t.Fatal(err)
	}
	return vs
}

func TestUnapprovedDomainDiffContract(t *testing.T) {
	t.Run("new PR 1975 case", func(t *testing.T) {
		root, base := setupDomainDiffRepo(t, "package sample\n\nvar unrelated = 1\n")
		writeFile(t, root, "target.go",
			"package sample\n\nvar unrelated = 1\nvar APIHost = \"internal-api-drive-stream.larkoffice.com\"\n")
		commitDomainDiff(t, root, "add internal host")

		got := violationsForRule(scanDomainDiff(t, root, base), unapprovedDomainRule)
		if len(got) != 1 || !strings.Contains(got[0].Message, "internal-api-drive-stream.larkoffice.com") {
			t.Fatalf("violations = %+v, want PR 1975 hostname", got)
		}
	})

	t.Run("new element in existing collection", func(t *testing.T) {
		root, base := setupDomainDiffRepo(t,
			"package sample\n\nvar ExtraHosts = []string{\n\t\"public.example.com\",\n}\n")
		writeFile(t, root, "target.go",
			"package sample\n\nvar ExtraHosts = []string{\n\t\"public.example.com\",\n\t\"attacker.zip\",\n}\n")
		commitDomainDiff(t, root, "add collection host")

		got := violationsForRule(scanDomainDiff(t, root, base), unapprovedDomainRule)
		if len(got) != 1 || !strings.Contains(got[0].Message, "attacker.zip") {
			t.Fatalf("violations = %+v, want attacker.zip", got)
		}
		if got[0].Line != 5 {
			t.Fatalf("violation line = %d, want 5", got[0].Line)
		}
	})

	t.Run("multiline expression changed segment", func(t *testing.T) {
		root, base := setupDomainDiffRepo(t,
			"package sample\n\nvar ExtraHost = \"private.corp.\" +\n\t\"example.com\"\n")
		writeFile(t, root, "target.go",
			"package sample\n\nvar ExtraHost = \"private.corp.\" +\n\t\"internal\"\n")
		commitDomainDiff(t, root, "change concatenated host")

		got := violationsForRule(scanDomainDiff(t, root, base), unapprovedDomainRule)
		if len(got) != 1 || !strings.Contains(got[0].Message, "private.corp.internal") {
			t.Fatalf("violations = %+v, want private.corp.internal", got)
		}
		if got[0].Line != 4 {
			t.Fatalf("violation line = %d, want changed line 4", got[0].Line)
		}
	})

	t.Run("unrelated change beside historical hostname", func(t *testing.T) {
		root, base := setupDomainDiffRepo(t,
			"package sample\n\nvar HistoricalHost = \"historical.private.internal\"\n")
		writeFile(t, root, "target.go",
			"package sample\n\nvar HistoricalHost = \"historical.private.internal\"\nvar unrelated = 1\n")
		commitDomainDiff(t, root, "add unrelated value")

		if got := violationsForRule(scanDomainDiff(t, root, base), unapprovedDomainRule); len(got) != 0 {
			t.Fatalf("unexpected historical-domain violation: %+v", got)
		}
	})

	t.Run("historical hostname expression changed", func(t *testing.T) {
		root, base := setupDomainDiffRepo(t,
			"package sample\n\nvar HistoricalHost = \"historical.private.internal\"\n")
		writeFile(t, root, "target.go",
			"package sample\n\nvar HistoricalHost = \"replacement.private.internal\"\n")
		commitDomainDiff(t, root, "change historical host")

		got := violationsForRule(scanDomainDiff(t, root, base), unapprovedDomainRule)
		if len(got) != 1 || !strings.Contains(got[0].Message, "replacement.private.internal") {
			t.Fatalf("violations = %+v, want replacement.private.internal", got)
		}
	})

	t.Run("new assignment references existing constant", func(t *testing.T) {
		root, base := setupDomainDiffRepo(t,
			"package sample\n\nconst existingConst = \"private.corp.internal\"\n")
		writeFile(t, root, "target.go",
			"package sample\n\nconst existingConst = \"private.corp.internal\"\nvar APIHost = existingConst\n")
		commitDomainDiff(t, root, "use existing hostname constant")

		got := violationsForRule(scanDomainDiff(t, root, base), unapprovedDomainRule)
		if len(got) != 1 || !strings.Contains(got[0].Message, "private.corp.internal") {
			t.Fatalf("violations = %+v, want private.corp.internal", got)
		}
		if got[0].Line != 4 {
			t.Fatalf("violation line = %d, want 4", got[0].Line)
		}
	})

	t.Run("allowlisted hostname", func(t *testing.T) {
		root, base := setupDomainDiffRepo(t, "package sample\n\nvar unrelated = 1\n")
		writeFile(t, root, "target.go",
			"package sample\n\nvar unrelated = 1\nvar BackupHost = \"public.example.com\"\n")
		commitDomainDiff(t, root, "add public host")

		if got := violationsForRule(scanDomainDiff(t, root, base), unapprovedDomainRule); len(got) != 0 {
			t.Fatalf("unexpected public-domain violation: %+v", got)
		}
	})

	t.Run("reserved example URL", func(t *testing.T) {
		root, base := setupDomainDiffRepo(t, "package sample\n\nvar unrelated = 1\n")
		writeFile(t, root, "target.go",
			"package sample\n\nvar unrelated = 1\nfunc fakeValue() string { return \"https://example.test/resource\" }\n")
		commitDomainDiff(t, root, "add safe example URL")

		if got := violationsForRule(scanDomainDiff(t, root, base), unapprovedDomainRule); len(got) != 0 {
			t.Fatalf("unexpected reserved-example violation: %+v", got)
		}
	})

	t.Run("allowlist does not approve subdomains", func(t *testing.T) {
		root, base := setupDomainDiffRepo(t, "package sample\n\nvar unrelated = 1\n")
		writeFile(t, root, "target.go",
			"package sample\n\nvar unrelated = 1\nvar BackupHost = \"evil.public.example.com\"\n")
		commitDomainDiff(t, root, "add unapproved public subdomain")

		got := violationsForRule(scanDomainDiff(t, root, base), unapprovedDomainRule)
		if len(got) != 1 || !strings.Contains(got[0].Message, "evil.public.example.com") {
			t.Fatalf("violations = %+v, want evil.public.example.com", got)
		}
	})

	t.Run("multi assignment pairs names and values", func(t *testing.T) {
		root, base := setupDomainDiffRepo(t, "package sample\n\nvar unrelated = 1\n")
		writeFile(t, root, publicDomainsPath,
			"# public\nopen.larksuite.com\npublic.example.com\n")
		writeFile(t, root, "target.go",
			"package sample\n\nvar unrelated = 1\nvar APIHost, BackupHost = \"open.larksuite.com\", \"attacker.zip\"\n")
		commitDomainDiff(t, root, "add multiple hosts")

		got := violationsForRule(scanDomainDiff(t, root, base), unapprovedDomainRule)
		if len(got) != 1 || !strings.Contains(got[0].Message, "attacker.zip") {
			t.Fatalf("violations = %+v, want only attacker.zip", got)
		}
	})

	t.Run("IDN hostname is rejected", func(t *testing.T) {
		root, base := setupDomainDiffRepo(t, "package sample\n\nvar unrelated = 1\n")
		writeFile(t, root, "target.go",
			"package sample\n\nvar unrelated = 1\nvar BackupHost = \"例子.公司.cn\"\n")
		commitDomainDiff(t, root, "add IDN hostname")

		got := violationsForRule(scanDomainDiff(t, root, base), unapprovedDomainRule)
		if len(got) != 1 || !strings.Contains(got[0].Message, "例子.公司.cn") {
			t.Fatalf("violations = %+v, want IDN hostname", got)
		}
	})

	t.Run("fixture limited to test files", func(t *testing.T) {
		root, base := setupDomainDiffRepo(t, "package sample\n\nvar unrelated = 1\n")
		writeFile(t, root, "target.go",
			"package sample\n\nvar unrelated = 1\nvar ProductionHost = \"fixture.example.com\"\n")
		commitDomainDiff(t, root, "use fixture in production")

		got := violationsForRule(scanDomainDiff(t, root, base), unapprovedDomainRule)
		if len(got) != 1 || !strings.Contains(got[0].Message, "fixture.example.com") {
			t.Fatalf("violations = %+v, want production fixture rejection", got)
		}
	})

	t.Run("fixture accepted in test file", func(t *testing.T) {
		root, base := setupDomainDiffRepo(t, "package sample\n\nvar unrelated = 1\n")
		writeFile(t, root, "new_target_test.go",
			"package sample\n\nvar BackupHost = \"fixture.example.com\"\n")
		commitDomainDiff(t, root, "use fixture in test")

		if got := violationsForRule(scanDomainDiff(t, root, base), unapprovedDomainRule); len(got) != 0 {
			t.Fatalf("unexpected fixture-domain violation: %+v", got)
		}
	})

	t.Run("fixture allowlist does not approve subdomains", func(t *testing.T) {
		root, base := setupDomainDiffRepo(t, "package sample\n\nvar unrelated = 1\n")
		writeFile(t, root, "new_target_test.go",
			"package sample\n\nvar BackupHost = \"evil.fixture.example.com\"\n")
		commitDomainDiff(t, root, "use unapproved fixture subdomain")

		got := violationsForRule(scanDomainDiff(t, root, base), unapprovedDomainRule)
		if len(got) != 1 || !strings.Contains(got[0].Message, "evil.fixture.example.com") {
			t.Fatalf("violations = %+v, want exact fixture match", got)
		}
	})

	t.Run("fixture rejected in skills", func(t *testing.T) {
		root, base := setupDomainDiffRepo(t, "package sample\n\nvar unrelated = 1\n")
		writeFile(t, root, "skills/example/example_test.go",
			"package example\n\nvar BackupHost = \"fixture.example.com\"\n")
		commitDomainDiff(t, root, "use fixture in skill")

		got := violationsForRule(scanDomainDiff(t, root, base), unapprovedDomainRule)
		if len(got) != 1 || !strings.Contains(got[0].Message, "fixture.example.com") {
			t.Fatalf("violations = %+v, want skill fixture rejection", got)
		}
	})

	t.Run("pure rename", func(t *testing.T) {
		root, base := setupDomainDiffRepo(t,
			"package sample\n\nvar HistoricalHost = \"historical.private.internal\"\n")
		gitTestCommand(t, root, "mv", "target.go", "renamed.go")
		commitDomainDiff(t, root, "rename file")

		if got := violationsForRule(scanDomainDiff(t, root, base), unapprovedDomainRule); len(got) != 0 {
			t.Fatalf("unexpected rename violation: %+v", got)
		}
	})
}

func TestUnapprovedDomainPolicyAndFailurePaths(t *testing.T) {
	t.Run("unused policy entry", func(t *testing.T) {
		root, base := setupDomainDiffRepo(t, "package sample\n\nvar unrelated = 1\n")
		writeFile(t, root, publicDomainsPath,
			"# public\npublic.example.com\nunused.example.com\n")
		commitDomainDiff(t, root, "add unused policy")

		got := violationsForRule(scanDomainDiff(t, root, base), unusedDomainRule)
		if len(got) != 1 || !strings.Contains(got[0].Message, "unused.example.com") {
			t.Fatalf("violations = %+v, want unused.example.com", got)
		}
	})

	t.Run("public entry used only by fixture", func(t *testing.T) {
		root, base := setupDomainDiffRepo(t, "package sample\n\nvar unrelated = 1\n")
		writeFile(t, root, publicDomainsPath,
			"# public\npublic.example.com\ntest-only.example.com\n")
		writeFile(t, root, "public_only_test.go",
			"package sample\n\nvar BackupHost = \"test-only.example.com\"\n")
		commitDomainDiff(t, root, "add test-only public policy")

		got := violationsForRule(scanDomainDiff(t, root, base), unusedDomainRule)
		if len(got) != 1 || !strings.Contains(got[0].Message, "test-only.example.com") {
			t.Fatalf("violations = %+v, want test-only.example.com", got)
		}
	})

	t.Run("changed Go parse failure", func(t *testing.T) {
		root, base := setupDomainDiffRepo(t, "package sample\n\nvar unrelated = 1\n")
		writeFile(t, root, "target.go", "package sample\n\nfunc broken(\n")
		commitDomainDiff(t, root, "break source")

		all := scanDomainDiff(t, root, base)
		got := violationsForRule(all, incompleteDomainRule)
		if len(got) != 1 || filepath.Base(got[0].File) != "target.go" {
			t.Fatalf("violations = %+v, want target.go scan-incomplete", got)
		}
		if unused := violationsForRule(all, unusedDomainRule); len(unused) != 0 {
			t.Fatalf("parse failure must not produce unreliable unused-policy diagnostics: %+v", unused)
		}
	})
}
