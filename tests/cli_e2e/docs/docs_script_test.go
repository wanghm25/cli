// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package docs

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	clie2e "github.com/larksuite/cli/tests/cli_e2e"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestDocsScriptParseXMLFromFile(t *testing.T) {
	workDir := t.TempDir()
	input := `<title>标题</title><p>一个苹果是 an apple。</p>`
	if err := os.WriteFile(filepath.Join(workDir, "draft.xml"), []byte(input), 0o600); err != nil {
		t.Fatalf("write draft: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args: []string{
			"docs", "+script",
			"--command", "parse",
			"--content", "@draft.xml",
		},
		DefaultAs: "bot",
		WorkDir:   workDir,
		Env:       docsScriptE2EEnv(),
	})
	require.NoError(t, err)
	result.AssertExitCode(t, 0)
	result.AssertStdoutStatus(t, true)
	require.Equal(t, int64(10), gjson.Get(result.Stdout, "data.profile.word_count").Int())
	require.Equal(t, int64(15), gjson.Get(result.Stdout, "data.profile.char_count").Int())
	require.Equal(t, int64(2), gjson.Get(result.Stdout, "data.profile.block_count").Int())
	require.False(t, gjson.Get(result.Stdout, "data.xml").Exists())
	require.False(t, gjson.Get(result.Stdout, "data.input_format").Exists())
	require.False(t, gjson.Get(result.Stdout, "data.command").Exists())
	require.False(t, gjson.Get(result.Stdout, "data.profile.breakdown").Exists())
	require.False(t, gjson.Get(result.Stdout, "data.profile.compatibility").Exists())
}

func TestDocsScriptParseAcceptsServerSDKAttributeAmpersand(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	input := `<block_insert><parameter><block_id>-1</block_id><content><img href="https://picsum.photos/320/200?seed=lark-cli&raw=1"/></content></parameter></block_insert>`
	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args:      []string{"docs", "+script", "--command", "parse", "--content", input},
		DefaultAs: "bot",
		Env:       docsScriptE2EEnv(),
	})
	require.NoError(t, err)
	result.AssertExitCode(t, 0)
	result.AssertStdoutStatus(t, true)
	require.Equal(t, int64(1), gjson.Get(result.Stdout, "data.profile.block_count").Int())
}

func TestDocsScriptParseMarkdownFromFile(t *testing.T) {
	workDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(workDir, "draft.md"), []byte("# 标题\n\n- item"), 0o600); err != nil {
		t.Fatalf("write draft: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args: []string{
			"docs", "+script",
			"--command", "parse",
			"--content", "@draft.md",
		},
		DefaultAs: "bot",
		WorkDir:   workDir,
		Env:       docsScriptE2EEnv(),
	})
	require.NoError(t, err)
	result.AssertExitCode(t, 0)
	result.AssertStdoutStatus(t, true)
	require.Equal(t, int64(3), gjson.Get(result.Stdout, "data.profile.block_count").Int())
	require.False(t, gjson.Get(result.Stdout, "data.xml").Exists())
	require.False(t, gjson.Get(result.Stdout, "data.profile.breakdown").Exists())
}

func TestDocsScriptCreatesUniqueTempXMLFiles(t *testing.T) {
	workDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	const count = 8
	type outcome struct {
		result *clie2e.Result
		err    error
	}
	outcomes := make(chan outcome, count)
	for i := 0; i < count; i++ {
		go func() {
			result, err := clie2e.RunCmd(ctx, clie2e.Request{
				Args:      []string{"docs", "+script", "--command", "create-temp-xml", "--file-name", "川西"},
				DefaultAs: "bot",
				WorkDir:   workDir,
				Env:       docsScriptE2EEnv(),
			})
			outcomes <- outcome{result: result, err: err}
		}()
	}

	seen := make(map[string]struct{}, count)
	for i := 0; i < count; i++ {
		outcome := <-outcomes
		require.NoError(t, outcome.err)
		require.NotNil(t, outcome.result)
		outcome.result.AssertExitCode(t, 0)
		outcome.result.AssertStdoutStatus(t, true)

		path := gjson.Get(outcome.result.Stdout, "data.path").String()
		require.False(t, gjson.Get(outcome.result.Stdout, "data.saved_path").Exists())
		require.False(t, gjson.Get(outcome.result.Stdout, "data.size_bytes").Exists())
		directory := filepath.Dir(path)
		require.Equal(t, "川西.xml", filepath.Base(path))
		require.Equal(t, filepath.Base(directory), directory)
		require.True(t, strings.HasPrefix(directory, "川西_"), "path: %q", path)
		require.True(t, strings.HasSuffix(directory, "_folder"), "path: %q", path)
		_, duplicate := seen[path]
		require.False(t, duplicate, "duplicate temporary path: %q", path)
		seen[path] = struct{}{}
		info, err := os.Stat(filepath.Join(workDir, path))
		require.NoError(t, err)
		require.Zero(t, info.Size())
	}
	require.Len(t, seen, count)
}

func TestDocsScriptConvertsMarkdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args: []string{
			"docs", "+script",
			"--command", "markdown-to-xml",
			"--content", "# 标题\n\n- item",
		},
		DefaultAs: "bot",
		Env:       docsScriptE2EEnv(),
	})
	require.NoError(t, err)
	result.AssertExitCode(t, 0)
	result.AssertStdoutStatus(t, true)
	require.Equal(t, `<h1>标题</h1><ul><li>item</li></ul>`, gjson.Get(result.Stdout, "data.xml").String())
	require.False(t, gjson.Get(result.Stdout, "data.profile").Exists())
	require.False(t, gjson.Get(result.Stdout, "data.input_format").Exists())
	require.False(t, gjson.Get(result.Stdout, "data.command").Exists())
}

func TestDocsScriptConvertsMarkdownToFile(t *testing.T) {
	workDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(workDir, "draft.md"), []byte("# 标题\n\n- item"), 0o600); err != nil {
		t.Fatalf("write draft: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args: []string{
			"docs", "+script",
			"--command", "markdown-to-xml",
			"--content", "@draft.md",
			"--output", "draft.xml",
		},
		DefaultAs: "bot",
		WorkDir:   workDir,
		Env:       docsScriptE2EEnv(),
	})
	require.NoError(t, err)
	result.AssertExitCode(t, 0)
	result.AssertStdoutStatus(t, true)

	wantXML := `<h1>标题</h1><ul><li>item</li></ul>`
	gotXML, err := os.ReadFile(filepath.Join(workDir, "draft.xml"))
	require.NoError(t, err)
	require.Equal(t, wantXML, string(gotXML))
	require.Equal(t, filepath.Join(workDir, "draft.xml"), gjson.Get(result.Stdout, "data.saved_path").String())
	require.Equal(t, int64(len(wantXML)), gjson.Get(result.Stdout, "data.size_bytes").Int())
	require.False(t, gjson.Get(result.Stdout, "data.xml").Exists())
}

func TestDocsScriptConvertsSoftLineBreaksToSpaces(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args: []string{
			"docs", "+script",
			"--command", "markdown-to-xml",
			"--content", "**文号：桂汛旱指〔2026〕17号**\n**签发人：XXX**\n**发布日期：2026年7月13日**",
		},
		DefaultAs: "bot",
		Env:       docsScriptE2EEnv(),
	})
	require.NoError(t, err)
	result.AssertExitCode(t, 0)
	result.AssertStdoutStatus(t, true)
	require.Equal(t,
		`<p><b>文号：桂汛旱指〔2026〕17号</b> <b>签发人：XXX</b> <b>发布日期：2026年7月13日</b></p>`,
		gjson.Get(result.Stdout, "data.xml").String(),
	)
}

func TestDocsScriptDryRunIsLocal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args: []string{
			"docs", "+script",
			"--command", "parse",
			"--content", `<p>text</p>`,
			"--dry-run",
		},
		DefaultAs: "bot",
		Env:       docsScriptE2EEnv(),
	})
	require.NoError(t, err)
	result.AssertExitCode(t, 0)
	require.Equal(t, int64(0), gjson.Get(result.Stdout, "api.#").Int())
	require.False(t, gjson.Get(result.Stdout, "network").Bool())
	require.Equal(t, "parse", gjson.Get(result.Stdout, "command").String())
}

func TestDocsScriptCreateTempXMLDryRunDoesNotWrite(t *testing.T) {
	workDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args:      []string{"docs", "+script", "--command", "create-temp-xml", "--file-name", "川西", "--dry-run"},
		DefaultAs: "bot",
		WorkDir:   workDir,
		Env:       docsScriptE2EEnv(),
	})
	require.NoError(t, err)
	result.AssertExitCode(t, 0)
	require.Equal(t, int64(0), gjson.Get(result.Stdout, "api.#").Int())
	require.False(t, gjson.Get(result.Stdout, "network").Bool())
	require.False(t, gjson.Get(result.Stdout, "creates_file").Bool())
	require.Equal(t, "create-temp-xml", gjson.Get(result.Stdout, "command").String())
	require.Equal(t, "川西_*_folder", gjson.Get(result.Stdout, "directory_pattern").String())
	require.Equal(t, "川西", gjson.Get(result.Stdout, "file_name").String())
	require.Equal(t, "川西.xml", gjson.Get(result.Stdout, "xml_file_name").String())
	entries, err := os.ReadDir(workDir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestDocsScriptOnlineDryRunFetchesXML(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args: []string{
			"docs", "+script",
			"--command", "parse",
			"--doc", "https://example.larksuite.com/docx/doxcnScriptDryRun",
			"--dry-run",
		},
		DefaultAs: "bot",
		Env:       docsScriptE2EEnv(),
	})
	require.NoError(t, err)
	result.AssertExitCode(t, 0)
	require.Equal(t, int64(1), gjson.Get(result.Stdout, "api.#").Int())
	require.Equal(t, "POST", gjson.Get(result.Stdout, "api.0.method").String())
	require.Equal(t, "/open-apis/docs_ai/v1/documents/doxcnScriptDryRun/fetch", gjson.Get(result.Stdout, "api.0.url").String())
	require.Equal(t, "xml", gjson.Get(result.Stdout, "api.0.body.format").String())
	require.True(t, gjson.Get(result.Stdout, "network").Bool())
	require.Equal(t, "parse", gjson.Get(result.Stdout, "command").String())
}

func TestDocsScriptOutputDryRunIsLocal(t *testing.T) {
	workDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args: []string{
			"docs", "+script",
			"--command", "markdown-to-xml",
			"--content", "# title",
			"--output", "draft.xml",
			"--overwrite",
			"--dry-run",
		},
		DefaultAs: "bot",
		WorkDir:   workDir,
		Env:       docsScriptE2EEnv(),
	})
	require.NoError(t, err)
	result.AssertExitCode(t, 0)
	require.Equal(t, int64(0), gjson.Get(result.Stdout, "api.#").Int())
	require.False(t, gjson.Get(result.Stdout, "network").Bool())
	require.Equal(t, "markdown-to-xml", gjson.Get(result.Stdout, "command").String())
	require.Equal(t, "draft.xml", gjson.Get(result.Stdout, "output").String())
	require.True(t, gjson.Get(result.Stdout, "overwrite").Bool())
	_, statErr := os.Stat(filepath.Join(workDir, "draft.xml"))
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func docsScriptE2EEnv() map[string]string {
	return map[string]string{
		"LARKSUITE_CLI_APP_ID":     "docs-script-e2e",
		"LARKSUITE_CLI_APP_SECRET": "secret",
		"LARKSUITE_CLI_BRAND":      "feishu",
	}
}
