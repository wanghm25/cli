// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package doc

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/shortcuts/doc/internal/docxparse"
)

func TestDocsScriptParsesAndProfilesXML(t *testing.T) {
	f, stdout, _, _ := cmdutil.TestFactory(t, docsTestConfigWithAppID("docs-script-test"))
	source := `<title>标题</title><p>一个苹果是 an apple。</p>`

	err := mountAndRunDocs(t, DocsScript, []string{
		"+script",
		"--command", docsScriptParse,
		"--content", source,
		"--as", "bot",
	}, f, stdout)
	if err != nil {
		t.Fatalf("execute docs +script: %v", err)
	}

	var envelope struct {
		OK   bool                       `json:"ok"`
		Data map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatalf("decode stdout: %v\n%s", err, stdout)
	}
	if !envelope.OK {
		t.Fatalf("ok = false: %s", stdout)
	}
	if len(envelope.Data) != 1 || envelope.Data["profile"] == nil {
		t.Fatalf("data = %+v, want only profile", envelope.Data)
	}
	var profile docsScriptPublicProfile
	if err := json.Unmarshal(envelope.Data["profile"], &profile); err != nil {
		t.Fatalf("decode profile: %v", err)
	}
	var profileFields map[string]json.RawMessage
	if err := json.Unmarshal(envelope.Data["profile"], &profileFields); err != nil {
		t.Fatalf("decode profile fields: %v", err)
	}
	if len(profileFields) != 4 || profileFields["breakdown"] != nil {
		t.Fatalf("profile fields = %+v, want breakdown hidden", profileFields)
	}
	if profile.WordCount != 10 || profile.CharCount != 15 || profile.BlockCount != 2 {
		t.Fatalf("profile = %+v", profile)
	}
	if got := blockCount(profile.Blocks, "title"); got != 1 {
		t.Fatalf("title count = %d, want 1", got)
	}
	if got := blockCount(profile.Blocks, "p"); got != 1 {
		t.Fatalf("p count = %d, want 1", got)
	}
}

func TestDocsScriptParseAutoDetectsMarkdown(t *testing.T) {
	f, stdout, _, _ := cmdutil.TestFactory(t, docsTestConfigWithAppID("docs-script-auto-markdown"))

	err := mountAndRunDocs(t, DocsScript, []string{
		"+script",
		"--command", docsScriptParse,
		"--content", "# 标题\n\n- item",
		"--as", "bot",
	}, f, stdout)
	if err != nil {
		t.Fatalf("execute docs +script: %v", err)
	}

	var envelope struct {
		Data docsScriptParseResult `json:"data"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatalf("decode stdout: %v\n%s", err, stdout)
	}
	if envelope.Data.Profile.BlockCount != 3 {
		t.Fatalf("profile = %+v, want 3 blocks", envelope.Data.Profile)
	}
}

func TestDocsScriptParsesOnlineDocumentFromToken(t *testing.T) {
	t.Setenv("LARKSUITE_CLI_CONFIG_DIR", t.TempDir())
	f, stdout, _, reg := cmdutil.TestFactory(t, docsTestConfigWithAppID("docs-script-online-token"))
	registerDocsAIStub(reg, "POST", "/open-apis/docs_ai/v1/documents/doxcnScriptToken/fetch", map[string]interface{}{
		"document": map[string]interface{}{
			"document_id": "doxcnScriptToken",
			"content":     `<title>在线文档</title><p>Hello world</p>`,
		},
	})

	err := mountAndRunDocs(t, DocsScript, []string{
		"+script",
		"--command", docsScriptParse,
		"--doc", "doxcnScriptToken",
		"--as", "bot",
	}, f, stdout)
	if err != nil {
		t.Fatalf("execute docs +script with token: %v", err)
	}

	var envelope struct {
		Data docsScriptParseResult `json:"data"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatalf("decode stdout: %v\n%s", err, stdout)
	}
	if envelope.Data.Profile.BlockCount != 2 {
		t.Fatalf("profile = %+v, want 2 blocks", envelope.Data.Profile)
	}
	if got := blockCount(envelope.Data.Profile.Blocks, "title"); got != 1 {
		t.Fatalf("title count = %d, want 1", got)
	}
	if got := blockCount(envelope.Data.Profile.Blocks, "p"); got != 1 {
		t.Fatalf("p count = %d, want 1", got)
	}
}

func TestDocsScriptParsesOnlineDocumentFromURL(t *testing.T) {
	t.Setenv("LARKSUITE_CLI_CONFIG_DIR", t.TempDir())
	f, stdout, _, reg := cmdutil.TestFactory(t, docsTestConfigWithAppID("docs-script-online-url"))
	stub := registerDocsAIStub(reg, "POST", "/open-apis/docs_ai/v1/documents/wikcnScriptURL/fetch", map[string]interface{}{
		"document": map[string]interface{}{
			"document_id": "doxcnResolvedScriptURL",
			"content":     `<p>从 Wiki URL 读取</p>`,
		},
	})

	err := mountAndRunDocs(t, DocsScript, []string{
		"+script",
		"--command", docsScriptParse,
		"--doc", "https://example.larksuite.com/wiki/wikcnScriptURL",
		"--as", "bot",
	}, f, stdout)
	if err != nil {
		t.Fatalf("execute docs +script with URL: %v", err)
	}
	if stub.CapturedBody == nil {
		t.Fatal("online parse did not call the document fetch API")
	}

	var envelope struct {
		Data docsScriptParseResult `json:"data"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatalf("decode stdout: %v\n%s", err, stdout)
	}
	if envelope.Data.Profile.BlockCount != 1 || blockCount(envelope.Data.Profile.Blocks, "p") != 1 {
		t.Fatalf("profile = %+v, want one paragraph", envelope.Data.Profile)
	}
}

func TestDocsScriptRejectsContentAndDocTogether(t *testing.T) {
	f, _, _, _ := cmdutil.TestFactory(t, docsTestConfigWithAppID("docs-script-input-conflict"))
	err := mountAndRunDocs(t, DocsScript, []string{
		"+script",
		"--command", docsScriptParse,
		"--content", `<p>local</p>`,
		"--doc", "doxcnScriptConflict",
		"--as", "bot",
	}, f, nil)
	assertValidationContract(t, err, errs.SubtypeInvalidArgument, "", "--content", "--doc")
}

func TestDocsScriptRejectsDocForMarkdownConversion(t *testing.T) {
	f, _, _, _ := cmdutil.TestFactory(t, docsTestConfigWithAppID("docs-script-doc-convert"))
	err := mountAndRunDocs(t, DocsScript, []string{
		"+script",
		"--command", docsScriptMarkdownToXML,
		"--doc", "doxcnScriptConvert",
		"--as", "bot",
	}, f, nil)
	assertValidationContract(t, err, errs.SubtypeInvalidArgument, "--doc")
}

func TestDocsScriptConvertsMarkdownFromStdin(t *testing.T) {
	f, stdout, _, _ := cmdutil.TestFactory(t, docsTestConfigWithAppID("docs-script-markdown"))
	f.IOStreams.In = bytes.NewBufferString("# 标题\n\n- item")

	err := mountAndRunDocs(t, DocsScript, []string{
		"+script",
		"--command", docsScriptMarkdownToXML,
		"--content", "-",
		"--as", "bot",
	}, f, stdout)
	if err != nil {
		t.Fatalf("execute docs +script: %v", err)
	}
	if !strings.Contains(stdout.String(), `<h1>标题</h1><ul><li>item</li></ul>`) {
		t.Fatalf("stdout missing converted XML: %s", stdout)
	}
	var envelope struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatalf("decode stdout: %v\n%s", err, stdout)
	}
	if len(envelope.Data) != 1 || envelope.Data["xml"] == nil {
		t.Fatalf("data = %+v, want only xml", envelope.Data)
	}
}

func TestDocsScriptConvertsMarkdownToOutputFile(t *testing.T) {
	workDir := t.TempDir()
	withDocsWorkingDir(t, workDir)
	f, stdout, _, _ := cmdutil.TestFactory(t, docsTestConfigWithAppID("docs-script-output"))
	wantXML := `<h1>标题</h1><ul><li>item</li></ul>`

	err := mountAndRunDocs(t, DocsScript, []string{
		"+script",
		"--command", docsScriptMarkdownToXML,
		"--content", "# 标题\n\n- item",
		"--output", "draft.xml",
		"--as", "bot",
	}, f, stdout)
	if err != nil {
		t.Fatalf("execute docs +script: %v", err)
	}
	gotXML, err := os.ReadFile("draft.xml")
	if err != nil {
		t.Fatalf("read output XML: %v", err)
	}
	if string(gotXML) != wantXML {
		t.Fatalf("output XML = %q, want %q", gotXML, wantXML)
	}

	var envelope struct {
		Data struct {
			SavedPath string          `json:"saved_path"`
			SizeBytes int64           `json:"size_bytes"`
			XML       json.RawMessage `json:"xml"`
		} `json:"data"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatalf("decode stdout: %v\n%s", err, stdout)
	}
	if envelope.Data.SavedPath != filepath.Join(workDir, "draft.xml") {
		t.Fatalf("saved_path = %q, want %q", envelope.Data.SavedPath, filepath.Join(workDir, "draft.xml"))
	}
	if envelope.Data.SizeBytes != int64(len(wantXML)) {
		t.Fatalf("size_bytes = %d, want %d", envelope.Data.SizeBytes, len(wantXML))
	}
	if envelope.Data.XML != nil {
		t.Fatalf("data.xml should be omitted when --output is used: %s", stdout)
	}
}

func TestDocsScriptCreatesUniqueTempXMLFiles(t *testing.T) {
	workDir := t.TempDir()
	withDocsWorkingDir(t, workDir)
	f, stdout, _, _ := cmdutil.TestFactory(t, docsTestConfigWithAppID("docs-script-temp-xml"))

	create := func() docsScriptTempXMLResult {
		t.Helper()
		stdout.Reset()
		err := mountAndRunDocs(t, DocsScript, []string{
			"+script",
			"--command", docsScriptCreateTempXML,
			"--file-name", "川西",
			"--as", "bot",
		}, f, stdout)
		if err != nil {
			t.Fatalf("execute docs +script: %v", err)
		}
		var envelope struct {
			Data docsScriptTempXMLResult `json:"data"`
		}
		if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
			t.Fatalf("decode stdout: %v\n%s", err, stdout)
		}
		return envelope.Data
	}

	first := create()
	second := create()
	if first.Path == second.Path {
		t.Fatalf("temporary paths are identical: %q", first.Path)
	}
	for _, got := range []docsScriptTempXMLResult{first, second} {
		directory := filepath.Dir(got.Path)
		if filepath.Base(got.Path) != "川西.xml" || filepath.Base(directory) != directory ||
			!strings.HasPrefix(directory, "川西_") || !strings.HasSuffix(directory, "_folder") {
			t.Fatalf("path = %q, want 川西_<random>_folder/川西.xml", got.Path)
		}
		info, err := os.Stat(got.Path)
		if err != nil {
			t.Fatalf("stat temporary XML %q: %v", got.Path, err)
		}
		if info.Size() != 0 {
			t.Fatalf("temporary XML %q size = %d, want 0", got.Path, info.Size())
		}
	}
}

func TestDocsScriptCreateTempXMLRejectsOtherFlags(t *testing.T) {
	tests := []struct {
		name  string
		args  []string
		param string
	}{
		{name: "content", args: []string{"--content", "<p>text</p>"}, param: "--content"},
		{name: "doc", args: []string{"--doc", "doxcnScriptTemp"}, param: "--doc"},
		{name: "output", args: []string{"--output", "draft.xml"}, param: "--output"},
		{name: "overwrite", args: []string{"--overwrite"}, param: "--overwrite"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f, _, _, _ := cmdutil.TestFactory(t, docsTestConfigWithAppID("docs-script-temp-xml-flags"))
			args := []string{"+script", "--command", docsScriptCreateTempXML, "--file-name", "川西", "--as", "bot"}
			args = append(args, test.args...)
			err := mountAndRunDocs(t, DocsScript, args, f, nil)
			if err == nil {
				t.Fatalf("expected %s validation error", test.param)
			}
			problem, ok := errs.ProblemOf(err)
			var validationErr *errs.ValidationError
			if !ok || problem.Category != errs.CategoryValidation || problem.Subtype != errs.SubtypeInvalidArgument ||
				!errors.As(err, &validationErr) || validationErr.Param != test.param {
				t.Fatalf("problem = %+v, validation = %+v, ok=%v", problem, validationErr, ok)
			}
		})
	}
}

func TestDocsScriptCreateTempXMLValidatesFileName(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
	}{
		{name: "missing"},
		{name: "path", fileName: "folder/川西"},
		{name: "windows path", fileName: `folder\川西`},
		{name: "reserved character", fileName: "川西:一"},
		{name: "xml extension included", fileName: "川西.xml"},
		{name: "windows device", fileName: "CON"},
		{name: "surrounding whitespace", fileName: " 川西"},
		{name: "dangerous unicode", fileName: "川\u200b西"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f, _, _, _ := cmdutil.TestFactory(t, docsTestConfigWithAppID("docs-script-temp-xml-file-name"))
			args := []string{"+script", "--command", docsScriptCreateTempXML, "--as", "bot"}
			if test.fileName != "" {
				args = append(args, "--file-name", test.fileName)
			}
			err := mountAndRunDocs(t, DocsScript, args, f, nil)
			if err == nil {
				t.Fatalf("expected --file-name validation error for %q", test.fileName)
			}
			problem, ok := errs.ProblemOf(err)
			var validationErr *errs.ValidationError
			if !ok || problem.Category != errs.CategoryValidation || problem.Subtype != errs.SubtypeInvalidArgument ||
				!errors.As(err, &validationErr) || validationErr.Param != "--file-name" {
				t.Fatalf("problem = %+v, validation = %+v, ok=%v", problem, validationErr, ok)
			}
		})
	}
}

func TestDocsScriptOutputRequiresExplicitOverwrite(t *testing.T) {
	withDocsWorkingDir(t, t.TempDir())
	if err := os.WriteFile("draft.xml", []byte("old"), 0o600); err != nil {
		t.Fatalf("write existing output: %v", err)
	}
	f, stdout, _, _ := cmdutil.TestFactory(t, docsTestConfigWithAppID("docs-script-overwrite"))

	err := mountAndRunDocs(t, DocsScript, []string{
		"+script",
		"--command", docsScriptMarkdownToXML,
		"--content", "# new",
		"--output", "draft.xml",
		"--as", "bot",
	}, f, stdout)
	if err == nil {
		t.Fatal("expected existing output error")
	}
	problem, ok := errs.ProblemOf(err)
	var validationErr *errs.ValidationError
	if !ok || problem.Category != errs.CategoryValidation || problem.Subtype != errs.SubtypeFailedPrecondition ||
		!errors.As(err, &validationErr) || validationErr.Param != "--output" {
		t.Fatalf("problem = %+v, validation = %+v, ok=%v", problem, validationErr, ok)
	}
	got, readErr := os.ReadFile("draft.xml")
	if readErr != nil || string(got) != "old" {
		t.Fatalf("existing output changed: content=%q err=%v", got, readErr)
	}

	err = mountAndRunDocs(t, DocsScript, []string{
		"+script",
		"--command", docsScriptMarkdownToXML,
		"--content", "# new",
		"--output", "draft.xml",
		"--overwrite",
		"--as", "bot",
	}, f, stdout)
	if err != nil {
		t.Fatalf("execute docs +script with --overwrite: %v", err)
	}
	got, readErr = os.ReadFile("draft.xml")
	if readErr != nil || string(got) != "<h1>new</h1>" {
		t.Fatalf("overwritten output = %q, err=%v", got, readErr)
	}
}

func TestDocsScriptRejectsOutputForParse(t *testing.T) {
	f, _, _, _ := cmdutil.TestFactory(t, docsTestConfigWithAppID("docs-script-output-parse"))
	err := mountAndRunDocs(t, DocsScript, []string{
		"+script",
		"--command", docsScriptParse,
		"--content", `<p>text</p>`,
		"--output", "draft.xml",
		"--as", "bot",
	}, f, nil)
	if err == nil {
		t.Fatal("expected --output validation error")
	}
	problem, ok := errs.ProblemOf(err)
	var validationErr *errs.ValidationError
	if !ok || problem.Category != errs.CategoryValidation || problem.Subtype != errs.SubtypeInvalidArgument ||
		!errors.As(err, &validationErr) || validationErr.Param != "--output" {
		t.Fatalf("problem = %+v, validation = %+v, ok=%v", problem, validationErr, ok)
	}
}

func TestDocsScriptRejectsUnsafeOutputPath(t *testing.T) {
	f, _, _, _ := cmdutil.TestFactory(t, docsTestConfigWithAppID("docs-script-output-path"))
	err := mountAndRunDocs(t, DocsScript, []string{
		"+script",
		"--command", docsScriptMarkdownToXML,
		"--content", "# title",
		"--output", filepath.Join(t.TempDir(), "draft.xml"),
		"--as", "bot",
	}, f, nil)
	if err == nil {
		t.Fatal("expected unsafe output path error")
	}
	problem, ok := errs.ProblemOf(err)
	var validationErr *errs.ValidationError
	if !ok || problem.Category != errs.CategoryValidation || problem.Subtype != errs.SubtypeInvalidArgument ||
		!errors.As(err, &validationErr) || validationErr.Param != "--output" {
		t.Fatalf("problem = %+v, validation = %+v, ok=%v", problem, validationErr, ok)
	}
}

func TestDocsScriptDryRunHasNoAPICall(t *testing.T) {
	f, stdout, _, _ := cmdutil.TestFactory(t, docsTestConfigWithAppID("docs-script-dry-run"))

	err := mountAndRunDocs(t, DocsScript, []string{
		"+script",
		"--command", docsScriptParse,
		"--content", `<p>text</p>`,
		"--dry-run",
		"--as", "bot",
	}, f, stdout)
	if err != nil {
		t.Fatalf("execute docs +script dry-run: %v", err)
	}
	var got struct {
		API     []any  `json:"api"`
		Command string `json:"command"`
		Network bool   `json:"network"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode dry-run stdout: %v\n%s", err, stdout)
	}
	if len(got.API) != 0 || got.Command != docsScriptParse || got.Network {
		t.Fatalf("dry-run output = %+v", got)
	}
}

func TestDocsScriptCreateTempXMLDryRunDoesNotWrite(t *testing.T) {
	workDir := t.TempDir()
	withDocsWorkingDir(t, workDir)
	f, stdout, _, _ := cmdutil.TestFactory(t, docsTestConfigWithAppID("docs-script-temp-xml-dry-run"))

	err := mountAndRunDocs(t, DocsScript, []string{
		"+script",
		"--command", docsScriptCreateTempXML,
		"--file-name", "川西",
		"--dry-run",
		"--as", "bot",
	}, f, stdout)
	if err != nil {
		t.Fatalf("execute docs +script dry-run: %v", err)
	}
	var got struct {
		API              []any  `json:"api"`
		Command          string `json:"command"`
		DirectoryPattern string `json:"directory_pattern"`
		FileName         string `json:"file_name"`
		XMLFileName      string `json:"xml_file_name"`
		CreatesFile      bool   `json:"creates_file"`
		Network          bool   `json:"network"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode dry-run stdout: %v\n%s", err, stdout)
	}
	if len(got.API) != 0 || got.Command != docsScriptCreateTempXML ||
		got.DirectoryPattern != "川西_*_folder" || got.FileName != "川西" || got.XMLFileName != "川西.xml" ||
		got.CreatesFile || got.Network {
		t.Fatalf("dry-run output = %+v", got)
	}
	entries, err := os.ReadDir(workDir)
	if err != nil {
		t.Fatalf("read work directory: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("dry-run created files: %+v", entries)
	}
}

func TestDocsScriptOnlineDryRunShowsFetchAPICall(t *testing.T) {
	f, stdout, _, _ := cmdutil.TestFactory(t, docsTestConfigWithAppID("docs-script-online-dry-run"))

	err := mountAndRunDocs(t, DocsScript, []string{
		"+script",
		"--command", docsScriptParse,
		"--doc", "https://example.larksuite.com/docx/doxcnScriptDryRun",
		"--dry-run",
		"--as", "bot",
	}, f, stdout)
	if err != nil {
		t.Fatalf("execute online docs +script dry-run: %v", err)
	}
	var got struct {
		API []struct {
			Method string                 `json:"method"`
			URL    string                 `json:"url"`
			Body   map[string]interface{} `json:"body"`
		} `json:"api"`
		Command    string `json:"command"`
		DocumentID string `json:"document_id"`
		Network    bool   `json:"network"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode dry-run stdout: %v\n%s", err, stdout)
	}
	if len(got.API) != 1 || got.API[0].Method != "POST" ||
		got.API[0].URL != "/open-apis/docs_ai/v1/documents/doxcnScriptDryRun/fetch" {
		t.Fatalf("dry-run API = %+v", got.API)
	}
	if got.API[0].Body["format"] != "xml" {
		t.Fatalf("dry-run body = %+v, want XML fetch", got.API[0].Body)
	}
	if got.Command != docsScriptParse || got.DocumentID != "doxcnScriptDryRun" || !got.Network {
		t.Fatalf("dry-run output = %+v", got)
	}
}

func TestDocsScriptOutputDryRunDoesNotWrite(t *testing.T) {
	withDocsWorkingDir(t, t.TempDir())
	f, stdout, _, _ := cmdutil.TestFactory(t, docsTestConfigWithAppID("docs-script-output-dry-run"))

	err := mountAndRunDocs(t, DocsScript, []string{
		"+script",
		"--command", docsScriptMarkdownToXML,
		"--content", "# title",
		"--output", "draft.xml",
		"--overwrite",
		"--dry-run",
		"--as", "bot",
	}, f, stdout)
	if err != nil {
		t.Fatalf("execute docs +script dry-run: %v", err)
	}
	var got struct {
		API       []any  `json:"api"`
		Command   string `json:"command"`
		Network   bool   `json:"network"`
		Output    string `json:"output"`
		Overwrite bool   `json:"overwrite"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode dry-run stdout: %v\n%s", err, stdout)
	}
	if len(got.API) != 0 || got.Command != docsScriptMarkdownToXML || got.Network || got.Output != "draft.xml" || !got.Overwrite {
		t.Fatalf("dry-run output = %+v", got)
	}
	if _, err := os.Stat("draft.xml"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dry-run created output file: %v", err)
	}
}

func TestDocsScriptReturnsTypedParseError(t *testing.T) {
	f, _, _, _ := cmdutil.TestFactory(t, docsTestConfigWithAppID("docs-script-error"))

	err := mountAndRunDocs(t, DocsScript, []string{
		"+script",
		"--command", docsScriptParse,
		"--content", `<!DOCTYPE document><p>text</p>`,
		"--as", "bot",
	}, f, nil)
	if err == nil {
		t.Fatal("expected parse error")
	}
	problem, ok := errs.ProblemOf(err)
	if !ok || problem.Category != errs.CategoryValidation || problem.Subtype != errs.SubtypeInvalidArgument {
		t.Fatalf("problem = %+v, ok=%v", problem, ok)
	}
	var validationErr *errs.ValidationError
	if !errors.As(err, &validationErr) || validationErr.Param != "--content" {
		t.Fatalf("error = %#v, want --content metadata", err)
	}
}

func TestDocsScriptRejectsMalformedXML(t *testing.T) {
	f, _, _, _ := cmdutil.TestFactory(t, docsTestConfigWithAppID("docs-script-malformed"))

	err := mountAndRunDocs(t, DocsScript, []string{
		"+script",
		"--command", docsScriptParse,
		"--content", `<p>text`,
		"--as", "bot",
	}, f, nil)
	if err == nil {
		t.Fatal("expected malformed XML error")
	}
	problem, ok := errs.ProblemOf(err)
	if !ok || problem.Category != errs.CategoryValidation || problem.Subtype != errs.SubtypeInvalidArgument {
		t.Fatalf("problem = %+v, ok=%v", problem, ok)
	}
}

func TestDocsScriptHelpExamplesAreCrossShellSafe(t *testing.T) {
	cmd := &cobra.Command{Short: "local document parser"}
	installDocsScriptHelp(cmd)
	if strings.Contains(cmd.Example, "cat ") {
		t.Fatalf("help examples require a platform-specific command: %q", cmd.Example)
	}
	if strings.Contains(cmd.Example, "--content @") {
		t.Fatalf("help examples contain an unquoted @file argument: %q", cmd.Example)
	}
	for _, want := range []string{`--command create-temp-xml --file-name "draft"`, `--content "@draft.xml"`, `--content "@draft.md"`, `--output "draft.xml"`} {
		if !strings.Contains(cmd.Example, want) {
			t.Errorf("help examples missing %q: %q", want, cmd.Example)
		}
	}
}

func blockCount(blocks []docxparse.BlockShare, typ string) int {
	for _, block := range blocks {
		if block.Type == typ {
			return block.Count
		}
	}
	return 0
}
