// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package drive

import (
	"context"
	"strings"
	"testing"
	"time"

	clie2e "github.com/larksuite/cli/tests/cli_e2e"
	"github.com/stretchr/testify/require"
)

func TestDriveCopyDryRun_DocxURL(t *testing.T) {
	setDriveDryRunConfigEnv(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args: []string{
			"drive", "+copy",
			"--url", "https://example.larksuite.com/docx/docxDryRunCopy?from=share",
			"--name", "Copied doc",
			"--folder-token", "https://example.larksuite.com/drive/folder/folderDryRunCopy",
			"--dry-run",
		},
		DefaultAs: "bot",
	})
	require.NoError(t, err)
	result.AssertExitCode(t, 0)

	out := result.Stdout
	if got := clie2e.DryRunGet(out, "api.0.method").String(); got != "POST" {
		t.Fatalf("api.0.method=%q, want POST\nstdout:\n%s", got, out)
	}
	if got := clie2e.DryRunGet(out, "api.0.url").String(); got != "/open-apis/drive/v1/files/docxDryRunCopy/copy" {
		t.Fatalf("api.0.url=%q, want copy endpoint\nstdout:\n%s", got, out)
	}
	if got := clie2e.DryRunGet(out, "api.0.body.name").String(); got != "Copied doc" {
		t.Fatalf("api.0.body.name=%q, want Copied doc\nstdout:\n%s", got, out)
	}
	if got := clie2e.DryRunGet(out, "api.0.body.type").String(); got != "docx" {
		t.Fatalf("api.0.body.type=%q, want docx\nstdout:\n%s", got, out)
	}
	if got := clie2e.DryRunGet(out, "api.0.body.folder_token").String(); got != "folderDryRunCopy" {
		t.Fatalf("api.0.body.folder_token=%q, want token parsed from folder URL\nstdout:\n%s", got, out)
	}
}

func TestDriveCopyDryRun_BareTokenBaseAlias(t *testing.T) {
	setDriveDryRunConfigEnv(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args: []string{
			"drive", "+copy",
			"--token", "bitableDryRunCopy",
			"--type", "base",
			"--name", "Copied base",
			"--folder-token", "folderDryRunCopy",
			"--extra", "target_type=bitable",
			"--dry-run",
		},
		DefaultAs: "bot",
	})
	require.NoError(t, err)
	result.AssertExitCode(t, 0)

	out := result.Stdout
	if got := clie2e.DryRunGet(out, "api.0.url").String(); got != "/open-apis/drive/v1/files/bitableDryRunCopy/copy" {
		t.Fatalf("api.0.url=%q, want copy endpoint\nstdout:\n%s", got, out)
	}
	if got := clie2e.DryRunGet(out, "api.0.body.type").String(); got != "bitable" {
		t.Fatalf("api.0.body.type=%q, want bitable (base alias normalized)\nstdout:\n%s", got, out)
	}
	if got := clie2e.DryRunGet(out, "api.0.body.extra.0.key").String(); got != "target_type" {
		t.Fatalf("api.0.body.extra.0.key=%q, want target_type\nstdout:\n%s", got, out)
	}
	if got := clie2e.DryRunGet(out, "api.0.body.extra.0.value").String(); got != "bitable" {
		t.Fatalf("api.0.body.extra.0.value=%q, want bitable\nstdout:\n%s", got, out)
	}
}

func TestDriveCopyDryRun_WikiURLRedirectsToWikiNodeCopy(t *testing.T) {
	setDriveDryRunConfigEnv(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args: []string{
			"drive", "+copy",
			"--url", "https://example.larksuite.com/wiki/wikiDryRunCopy",
			"--name", "Copied wiki",
			"--folder-token", "folderDryRunCopy",
			"--dry-run",
		},
		DefaultAs: "bot",
	})
	require.NoError(t, err)
	if result.ExitCode == 0 {
		t.Fatalf("wiki URL should be rejected with a redirect error\nstdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr)
	}
	if !strings.Contains(result.Stderr, `"type": "validation"`) {
		t.Fatalf("stderr should carry a typed validation error\nstderr:\n%s", result.Stderr)
	}
	if !strings.Contains(result.Stderr, "wiki +node-copy --space-id") ||
		!strings.Contains(result.Stderr, "--node-token wikiDryRunCopy") {
		t.Fatalf("stderr should guide to wiki +node-copy with the parsed node token\nstderr:\n%s", result.Stderr)
	}
	if !strings.Contains(result.Stderr, "wiki +node-get --token wikiDryRunCopy") {
		t.Fatalf("stderr should explain how to resolve the space id\nstderr:\n%s", result.Stderr)
	}
}
