// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package drive

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/httpmock"
)

func TestResolveDriveCopyInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		urlInput  string
		rawInput  string
		docType   string
		wantToken string
		wantType  string
		wantErr   string
		wantParam string
	}{
		{
			name:      "url docx",
			urlInput:  "https://example.larksuite.com/docx/docxCopySource?from=share",
			wantToken: "docxCopySource",
			wantType:  "docx",
		},
		{
			name:      "url base normalizes to bitable",
			urlInput:  "https://example.larksuite.com/base/bitableCopySource",
			wantToken: "bitableCopySource",
			wantType:  "bitable",
		},
		{
			name:      "token flag also accepts url",
			rawInput:  "https://example.larksuite.com/sheets/sheetCopySource",
			wantToken: "sheetCopySource",
			wantType:  "sheet",
		},
		{
			name:      "bare token with type",
			rawInput:  "mindnoteCopySource",
			docType:   "mindnote",
			wantToken: "mindnoteCopySource",
			wantType:  "mindnote",
		},
		{
			name:      "bare token with base alias",
			rawInput:  "bitableCopySource",
			docType:   "base",
			wantToken: "bitableCopySource",
			wantType:  "bitable",
		},
		{
			name:      "url and token mutually exclusive",
			urlInput:  "https://example.larksuite.com/docx/docxCopySource",
			rawInput:  "docxCopySource",
			wantErr:   "mutually exclusive",
			wantParam: "--url",
		},
		{
			name:      "missing input",
			wantErr:   "specify --url or --token",
			wantParam: "--url",
		},
		{
			name:      "bare token needs type",
			rawInput:  "docxCopySource",
			wantErr:   "--type is required",
			wantParam: "--type",
		},
		{
			name:      "type conflicts with url",
			urlInput:  "https://example.larksuite.com/docx/docxCopySource",
			docType:   "sheet",
			wantErr:   "conflicts",
			wantParam: "--type",
		},
		{
			name:      "folder url unsupported as source",
			urlInput:  "https://example.larksuite.com/drive/folder/folderCopySource",
			wantErr:   "unsupported",
			wantParam: "--url",
		},
		{
			name:      "unrecognized url",
			urlInput:  "https://example.larksuite.com/unknown/path",
			wantErr:   "unsupported --url URL",
			wantParam: "--url",
		},
		{
			name:      "token with path fragments",
			rawInput:  "token/with/slash",
			wantErr:   "invalid bare token",
			wantParam: "--token",
		},
		{
			name:      "invalid bare type",
			rawInput:  "someToken",
			docType:   "folder",
			wantErr:   "invalid --type",
			wantParam: "--type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := resolveDriveCopyInput(tt.urlInput, tt.rawInput, tt.docType)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
				}
				assertDriveCopyValidationError(t, err, tt.wantParam)
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Token != tt.wantToken || got.Type != tt.wantType {
				t.Fatalf("got (%q, %q), want (%q, %q)", got.Token, got.Type, tt.wantToken, tt.wantType)
			}
		})
	}
}

func TestResolveDriveCopyInputWikiRedirect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		urlInput  string
		rawInput  string
		docType   string
		wantParam string
	}{
		{
			name:      "wiki url",
			urlInput:  "https://example.larksuite.com/wiki/wikiCopySource",
			wantParam: "--url",
		},
		{
			name:      "wiki url via token flag",
			rawInput:  "https://example.larksuite.com/wiki/wikiCopySource",
			wantParam: "--token",
		},
		{
			name:      "bare token with wiki type",
			rawInput:  "wikiCopySource",
			docType:   "wiki",
			wantParam: "--type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := resolveDriveCopyInput(tt.urlInput, tt.rawInput, tt.docType)
			if err == nil {
				t.Fatal("expected wiki redirect error, got nil")
			}
			assertDriveCopyValidationError(t, err, tt.wantParam)

			var validationErr *errs.ValidationError
			if !errors.As(err, &validationErr) {
				t.Fatalf("expected *errs.ValidationError, got %T", err)
			}
			if !strings.Contains(validationErr.Message, "wiki +node-copy") {
				t.Fatalf("message should redirect to wiki +node-copy, got %q", validationErr.Message)
			}
			if !strings.Contains(validationErr.Hint, "wiki +node-copy --space-id") {
				t.Fatalf("hint should carry the wiki +node-copy command, got %q", validationErr.Hint)
			}
			if !strings.Contains(validationErr.Hint, "--node-token wikiCopySource") {
				t.Fatalf("hint should carry the parsed node token, got %q", validationErr.Hint)
			}
			if !strings.Contains(validationErr.Hint, "wiki +node-get --token wikiCopySource") {
				t.Fatalf("hint should explain how to resolve the space id, got %q", validationErr.Hint)
			}
		})
	}
}

func TestResolveDriveCopyFolderToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     string
		wantToken string
		wantErr   string
	}{
		{
			name:      "bare folder token",
			input:     "folderCopyTarget",
			wantToken: "folderCopyTarget",
		},
		{
			name:      "folder url",
			input:     "https://example.larksuite.com/drive/folder/folderCopyTarget",
			wantToken: "folderCopyTarget",
		},
		{
			name:    "non-folder url",
			input:   "https://example.larksuite.com/docx/docxCopyTarget",
			wantErr: "not a folder",
		},
		{
			name:    "empty input",
			input:   "  ",
			wantErr: "--folder-token",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := resolveDriveCopyFolderToken(tt.input)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
				}
				assertDriveCopyValidationError(t, err, "--folder-token")
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.wantToken {
				t.Fatalf("token = %q, want %q", got, tt.wantToken)
			}
		})
	}
}

func TestParseDriveCopyExtras(t *testing.T) {
	t.Parallel()

	extras, err := parseDriveCopyExtras(nil)
	if err != nil || extras != nil {
		t.Fatalf("empty specs = (%#v, %v), want (nil, nil)", extras, err)
	}

	extras, err = parseDriveCopyExtras([]string{"target_type=docx", "flag=a=b"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []driveCopyExtra{{Key: "target_type", Value: "docx"}, {Key: "flag", Value: "a=b"}}
	if len(extras) != len(want) {
		t.Fatalf("extras = %#v, want %#v", extras, want)
	}
	for i := range want {
		if extras[i] != want[i] {
			t.Fatalf("extras[%d] = %#v, want %#v (order and values must be preserved verbatim)", i, extras[i], want[i])
		}
	}

	for _, bad := range []string{"no-separator", "=docx", "  =docx", "target_type="} {
		_, err := parseDriveCopyExtras([]string{bad})
		if err == nil || !strings.Contains(err.Error(), "invalid --extra") {
			t.Fatalf("spec %q: expected invalid --extra error, got %v", bad, err)
		}
		assertDriveCopyValidationError(t, err, "--extra")
	}
}

func TestBuildDriveCopyBodyExtras(t *testing.T) {
	t.Parallel()

	spec := driveCopySpec{
		Ref:         driveCopyRef{Token: "docCopySource", Type: "doc", SourceFlag: "--url"},
		Name:        "Copied doc",
		FolderToken: "folderCopyTarget",
	}
	if _, ok := buildDriveCopyBody(spec)["extra"]; ok {
		t.Fatal("body should omit extra when no --extra is passed")
	}

	spec.Extras = []driveCopyExtra{{Key: "target_type", Value: "docx"}}
	body := buildDriveCopyBody(spec)
	extras, ok := body["extra"].([]map[string]interface{})
	if !ok || len(extras) != 1 {
		t.Fatalf("body extra = %#v, want 1 key/value entry", body["extra"])
	}
	if extras[0]["key"] != "target_type" || extras[0]["value"] != "docx" {
		t.Fatalf("extra[0] = %#v, want target_type=docx", extras[0])
	}
}

func TestValidateDriveCopySpec(t *testing.T) {
	t.Parallel()

	base := driveCopySpec{
		Ref:         driveCopyRef{Token: "docxCopySource", Type: "docx", SourceFlag: "--url"},
		Name:        "Copy name",
		FolderToken: "folderCopyTarget",
	}

	if err := validateDriveCopySpec(base); err != nil {
		t.Fatalf("unexpected error for valid spec: %v", err)
	}

	empty := base
	empty.Name = ""
	err := validateDriveCopySpec(empty)
	if err == nil || !strings.Contains(err.Error(), "--name must not be empty") {
		t.Fatalf("expected empty-name error, got %v", err)
	}
	assertDriveCopyValidationError(t, err, "--name")

	long := base
	long.Name = strings.Repeat("字", 90) // 270 bytes in UTF-8
	err = validateDriveCopySpec(long)
	if err == nil || !strings.Contains(err.Error(), "exceeds 256 bytes") {
		t.Fatalf("expected name-length error, got %v", err)
	}
	assertDriveCopyValidationError(t, err, "--name")
}

func assertDriveCopyValidationError(t *testing.T, err error, wantParam string) {
	t.Helper()

	var validationErr *errs.ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if validationErr.Category != errs.CategoryValidation {
		t.Fatalf("category = %q, want %q", validationErr.Category, errs.CategoryValidation)
	}
	if validationErr.Subtype != errs.SubtypeInvalidArgument {
		t.Fatalf("subtype = %q, want %q", validationErr.Subtype, errs.SubtypeInvalidArgument)
	}
	if validationErr.Param != wantParam {
		t.Fatalf("param = %q, want %q", validationErr.Param, wantParam)
	}
	if cause := errors.Unwrap(err); cause != nil {
		t.Fatalf("unexpected cause on direct validation error: %v", cause)
	}

	problem, ok := errs.ProblemOf(err)
	if !ok {
		t.Fatalf("expected errs.ProblemOf to recognize typed error: %v", err)
	}
	if problem.Category != errs.CategoryValidation {
		t.Fatalf("problem category = %q, want %q", problem.Category, errs.CategoryValidation)
	}
}

func TestDriveCopyExecuteDocx(t *testing.T) {
	f, stdout, _, reg := cmdutil.TestFactory(t, driveTestConfig())
	copyStub := &httpmock.Stub{
		Method: "POST",
		URL:    "/open-apis/drive/v1/files/docxCopySource/copy",
		Body: map[string]interface{}{
			"code": 0,
			"msg":  "success",
			"data": map[string]interface{}{
				"file": map[string]interface{}{
					"token":        "docxCopyResult",
					"type":         "docx",
					"name":         "Copied doc",
					"url":          "https://example.larksuite.com/docx/docxCopyResult",
					"parent_token": "folderCopyTarget",
				},
			},
		},
	}
	reg.Register(copyStub)

	err := mountAndRunDrive(t, DriveCopy, []string{
		"+copy",
		"--url", "https://example.larksuite.com/docx/docxCopySource",
		"--name", "Copied doc",
		"--folder-token", "https://example.larksuite.com/drive/folder/folderCopyTarget",
		"--as", "user",
	}, f, stdout)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var requestBody map[string]interface{}
	if err := json.Unmarshal(copyStub.CapturedBody, &requestBody); err != nil {
		t.Fatalf("failed to decode captured body: %v\nbody:\n%s", err, string(copyStub.CapturedBody))
	}
	if got := requestBody["name"]; got != "Copied doc" {
		t.Fatalf("body name = %#v, want Copied doc", got)
	}
	if got := requestBody["type"]; got != "docx" {
		t.Fatalf("body type = %#v, want docx", got)
	}
	if got := requestBody["folder_token"]; got != "folderCopyTarget" {
		t.Fatalf("body folder_token = %#v, want folderCopyTarget (parsed from folder URL)", got)
	}

	out := decodeJSONMap(t, stdout.String())
	data := mustMapValue(t, out["data"], "data")
	if got := data["copied"]; got != true {
		t.Fatalf("copied = %#v, want true", got)
	}
	if got := mustStringField(t, data, "file_token", "data.file_token"); got != "docxCopyResult" {
		t.Fatalf("file_token = %q, want docxCopyResult", got)
	}
	if got := mustStringField(t, data, "file_type", "data.file_type"); got != "docx" {
		t.Fatalf("file_type = %q, want docx", got)
	}
	if got := mustStringField(t, data, "url", "data.url"); got != "https://example.larksuite.com/docx/docxCopyResult" {
		t.Fatalf("url = %q, want backend url", got)
	}
	if got := mustStringField(t, data, "source_file_token", "data.source_file_token"); got != "docxCopySource" {
		t.Fatalf("source_file_token = %q, want docxCopySource", got)
	}
}

func TestDriveCopyExecuteBuildsURLFallback(t *testing.T) {
	f, stdout, _, reg := cmdutil.TestFactory(t, driveTestConfig())
	reg.Register(&httpmock.Stub{
		Method: "POST",
		URL:    "/open-apis/drive/v1/files/sheetCopySource/copy",
		Body: map[string]interface{}{
			"code": 0,
			"msg":  "success",
			"data": map[string]interface{}{
				"file": map[string]interface{}{
					"token": "sheetCopyResult",
					"type":  "sheet",
					"name":  "Copied sheet",
				},
			},
		},
	})

	err := mountAndRunDrive(t, DriveCopy, []string{
		"+copy",
		"--token", "sheetCopySource",
		"--type", "sheet",
		"--name", "Copied sheet",
		"--folder-token", "folderCopyTarget",
		"--as", "user",
	}, f, stdout)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	out := decodeJSONMap(t, stdout.String())
	data := mustMapValue(t, out["data"], "data")
	url := mustStringField(t, data, "url", "data.url")
	if !strings.HasSuffix(url, "/sheets/sheetCopyResult") {
		t.Fatalf("url = %q, want built fallback ending in /sheets/sheetCopyResult", url)
	}
}

func TestDriveCopyExecuteAPIError(t *testing.T) {
	f, stdout, _, reg := cmdutil.TestFactory(t, driveTestConfig())
	reg.Register(&httpmock.Stub{
		Method: "POST",
		URL:    "/open-apis/drive/v1/files/docxCopySource/copy",
		Body: map[string]interface{}{
			"code": 1248006,
			"msg":  "no permission",
		},
	})

	err := mountAndRunDrive(t, DriveCopy, []string{
		"+copy",
		"--token", "docxCopySource",
		"--type", "docx",
		"--name", "Copied doc",
		"--folder-token", "folderCopyTarget",
		"--as", "user",
	}, f, stdout)
	if err == nil {
		t.Fatal("expected API error, got nil")
	}
	problem, ok := errs.ProblemOf(err)
	if !ok {
		t.Fatalf("expected typed error, got %T: %v", err, err)
	}
	if problem.Code != 1248006 {
		t.Fatalf("problem code = %d, want 1248006", problem.Code)
	}
}

func TestDriveCopyMountedDryRun(t *testing.T) {
	f, stdout, _, _ := cmdutil.TestFactory(t, driveTestConfig())

	err := mountAndRunDrive(t, DriveCopy, []string{
		"+copy",
		"--url", "https://example.larksuite.com/docx/docxCopySource",
		"--name", "Copied doc",
		"--folder-token", "folderCopyTarget",
		"--extra", "target_type=docx",
		"--dry-run",
		"--as", "user",
	}, f, stdout)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	out := decodeJSONMap(t, stdout.String())
	if got := out["dry_run"]; got != true {
		t.Fatalf("dry_run = %#v, want true\nstdout:\n%s", got, stdout.String())
	}
	data := mustMapValue(t, out["data"], "data")
	apis, ok := data["api"].([]interface{})
	if !ok || len(apis) != 1 {
		t.Fatalf("expected 1 api entry, got %#v\nstdout:\n%s", data["api"], stdout.String())
	}
	call := mustMapValue(t, apis[0], "api.0")
	if got := call["url"]; got != "/open-apis/drive/v1/files/docxCopySource/copy" {
		t.Fatalf("url = %#v, want resolved copy endpoint", got)
	}
	body := mustMapValue(t, call["body"], "api.0.body")
	if got := body["folder_token"]; got != "folderCopyTarget" {
		t.Fatalf("body folder_token = %#v, want folderCopyTarget", got)
	}
	extras, ok := body["extra"].([]interface{})
	if !ok || len(extras) != 1 {
		t.Fatalf("body extra = %#v, want 1 entry", body["extra"])
	}
	extra := mustMapValue(t, extras[0], "api.0.body.extra.0")
	if extra["key"] != "target_type" || extra["value"] != "docx" {
		t.Fatalf("extra[0] = %#v, want target_type=docx", extra)
	}
}

func TestDriveCopyMountedWikiInputFailsValidation(t *testing.T) {
	f, stdout, _, _ := cmdutil.TestFactory(t, driveTestConfig())

	err := mountAndRunDrive(t, DriveCopy, []string{
		"+copy",
		"--url", "https://example.larksuite.com/wiki/wikiCopySource",
		"--name", "Copied wiki",
		"--folder-token", "folderCopyTarget",
		"--as", "user",
	}, f, stdout)
	if err == nil {
		t.Fatal("expected wiki redirect error, got nil")
	}
	assertDriveCopyValidationError(t, err, "--url")

	var validationErr *errs.ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("expected *errs.ValidationError, got %T", err)
	}
	if !strings.Contains(validationErr.Hint, "wiki +node-copy") {
		t.Fatalf("hint should redirect to wiki +node-copy, got %q", validationErr.Hint)
	}
}

func TestDriveCopyMountedFolderAndNameValidation(t *testing.T) {
	f, stdout, _, _ := cmdutil.TestFactory(t, driveTestConfig())

	err := mountAndRunDrive(t, DriveCopy, []string{
		"+copy",
		"--token", "docxCopySource",
		"--type", "docx",
		"--name", "Copied doc",
		"--folder-token", "https://example.larksuite.com/docx/notAFolder",
		"--as", "user",
	}, f, stdout)
	if err == nil || !strings.Contains(err.Error(), "not a folder") {
		t.Fatalf("expected non-folder target error, got %v", err)
	}
	assertDriveCopyValidationError(t, err, "--folder-token")

	err = mountAndRunDrive(t, DriveCopy, []string{
		"+copy",
		"--token", "docxCopySource",
		"--type", "docx",
		"--name", "   ",
		"--folder-token", "folderCopyTarget",
		"--as", "user",
	}, f, stdout)
	if err == nil || !strings.Contains(err.Error(), "--name must not be empty") {
		t.Fatalf("expected whitespace-name error, got %v", err)
	}
	assertDriveCopyValidationError(t, err, "--name")

	err = mountAndRunDrive(t, DriveCopy, []string{
		"+copy",
		"--token", "docxCopySource",
		"--type", "docx",
		"--name", "Copied doc",
		"--folder-token", "folderCopyTarget",
		"--extra", "no-separator",
		"--as", "user",
	}, f, stdout)
	if err == nil || !strings.Contains(err.Error(), "expected format key=value") {
		t.Fatalf("expected malformed --extra error, got %v", err)
	}
	assertDriveCopyValidationError(t, err, "--extra")
}

func TestBuildDriveCopyDryRun(t *testing.T) {
	t.Parallel()

	spec := driveCopySpec{
		Ref:         driveCopyRef{Token: "docxCopySource", Type: "docx", SourceFlag: "--url"},
		Name:        "Copied doc",
		FolderToken: "folderCopyTarget",
	}
	preview := buildDriveCopyDryRun(spec)
	raw, err := json.Marshal(preview)
	if err != nil {
		t.Fatalf("failed to marshal dry-run preview: %v", err)
	}
	payload := decodeJSONMap(t, string(raw))

	apis, ok := payload["api"].([]interface{})
	if !ok || len(apis) != 1 {
		t.Fatalf("expected 1 api entry, got %#v", payload["api"])
	}
	call := mustMapValue(t, apis[0], "api.0")
	if got := call["method"]; got != "POST" {
		t.Fatalf("method = %#v, want POST", got)
	}
	if got := call["url"]; got != "/open-apis/drive/v1/files/docxCopySource/copy" {
		t.Fatalf("url = %#v, want resolved copy endpoint", got)
	}
	body := mustMapValue(t, call["body"], "api.0.body")
	if got := body["type"]; got != "docx" {
		t.Fatalf("body type = %#v, want docx", got)
	}
	if got := body["name"]; got != "Copied doc" {
		t.Fatalf("body name = %#v, want Copied doc", got)
	}
	if got := body["folder_token"]; got != "folderCopyTarget" {
		t.Fatalf("body folder_token = %#v, want folderCopyTarget", got)
	}
	if got := payload["file_token"]; got != "docxCopySource" {
		t.Fatalf("file_token = %#v, want docxCopySource", got)
	}
}
