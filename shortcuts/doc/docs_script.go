// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package doc

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/extension/fileio"
	"github.com/larksuite/cli/internal/charcheck"
	"github.com/larksuite/cli/shortcuts/common"
	"github.com/larksuite/cli/shortcuts/doc/internal/docxparse"
)

const (
	docsScriptParse         = "parse"
	docsScriptMarkdownToXML = "markdown-to-xml"
	docsScriptCreateTempXML = "create-temp-xml"
	docsScriptTempDirSuffix = "_*_folder"
)

var DocsScript = common.Shortcut{
	Service:     "docs",
	Command:     "+script",
	Description: "Create a unique temporary XML file, parse and profile local or online documents, or convert Markdown to LarkOpenCLI XML",
	Risk:        "read",
	AuthTypes:   []string{"user", "bot"},
	Scopes:      []string{},
	ConditionalScopes: []string{
		"docx:document:readonly",
	},
	Flags: []common.Flag{
		{
			Name:     "command",
			Desc:     "local document operation",
			Required: true,
			Enum:     []string{docsScriptParse, docsScriptMarkdownToXML, docsScriptCreateTempXML},
		},
		{
			Name:  "content",
			Desc:  "local content for parse or markdown-to-xml; use @relative-file or - for stdin; mutually exclusive with --doc",
			Input: []string{common.File, common.Stdin},
		},
		{
			Name: "doc",
			Desc: "online document URL or token for --command parse; mutually exclusive with --content",
		},
		{
			Name: "output",
			Desc: "local XML output path for markdown-to-xml; omit to return XML in data.xml",
		},
		{
			Name: "file-name",
			Desc: "portable base name without .xml; create-temp-xml writes <name>_<random>_folder/<name>.xml",
		},
		{
			Name: "overwrite",
			Type: "bool",
			Desc: "overwrite an existing --output file",
		},
	},
	Tips: []string{
		"create-temp-xml atomically creates <file-name>_<random>_folder/<file-name>.xml in the current directory",
		"parse accepts local --content or an online --doc URL/token and returns only the text and block profile",
		"markdown-to-xml converts Markdown to LarkOpenCLI XML",
		"use --output to save converted XML directly and keep stdout compact",
	},
	PostMount: installDocsScriptHelp,
	Validate:  validateDocsScript,
	DryRun:    dryRunDocsScript,
	Execute:   executeDocsScript,
}

type docsScriptParseResult struct {
	Profile docsScriptPublicProfile `json:"profile"`
}

// docsScriptPublicProfile is the stable shortcut response. The parser keeps
// the more detailed breakdown internally so it can be exposed later without
// changing the counting implementation.
type docsScriptPublicProfile struct {
	WordCount  int                    `json:"word_count"`
	CharCount  int                    `json:"char_count"`
	BlockCount int                    `json:"block_count"`
	Blocks     []docxparse.BlockShare `json:"blocks"`
}

type docsScriptMarkdownResult struct {
	XML string `json:"xml"`
}

type docsScriptMarkdownFileResult struct {
	SavedPath string `json:"saved_path"`
	SizeBytes int64  `json:"size_bytes"`
}

type docsScriptTempXMLResult struct {
	Path string `json:"path"`
}

func installDocsScriptHelp(cmd *cobra.Command) {
	installDocsShortcutHelp("+script")(cmd)
	cmd.Example = `  lark-cli docs +script --command create-temp-xml --file-name "draft"
  lark-cli docs +script --command parse --content "@draft.xml"
  lark-cli docs +script --command parse --content "@draft.md"
  lark-cli docs +script --command parse --doc "https://example.larksuite.com/docx/doxcn..."
  lark-cli docs +script --command markdown-to-xml --content "@draft.md" --output "draft.xml"`
}

func validateDocsScript(_ context.Context, runtime *common.RuntimeContext) error {
	content := strings.TrimSpace(runtime.Str("content"))
	doc := strings.TrimSpace(runtime.Str("doc"))
	outputPath := strings.TrimSpace(runtime.Str("output"))
	fileName := strings.TrimSpace(runtime.Str("file-name"))
	if runtime.Str("command") == docsScriptCreateTempXML {
		switch {
		case content != "":
			return errs.NewValidationError(errs.SubtypeInvalidArgument,
				"--content is not supported with --command create-temp-xml").WithParam("--content")
		case doc != "":
			return errs.NewValidationError(errs.SubtypeInvalidArgument,
				"--doc is not supported with --command create-temp-xml").WithParam("--doc")
		case outputPath != "":
			return errs.NewValidationError(errs.SubtypeInvalidArgument,
				"--output is not supported with --command create-temp-xml").WithParam("--output")
		case runtime.Bool("overwrite"):
			return errs.NewValidationError(errs.SubtypeInvalidArgument,
				"--overwrite is not supported with --command create-temp-xml").WithParam("--overwrite")
		case fileName == "":
			return errs.NewValidationError(errs.SubtypeInvalidArgument,
				"--file-name is required with --command create-temp-xml").WithParam("--file-name")
		case runtime.Str("file-name") != fileName:
			return errs.NewValidationError(errs.SubtypeInvalidArgument,
				"--file-name must not start or end with whitespace").WithParam("--file-name")
		default:
			return validateDocsScriptTempXMLFileName(fileName)
		}
	}
	if fileName != "" {
		return errs.NewValidationError(errs.SubtypeInvalidArgument,
			"--file-name is only supported with --command create-temp-xml").WithParam("--file-name")
	}
	if content == "" && doc == "" {
		return errs.NewValidationError(errs.SubtypeInvalidArgument, "one of --content or --doc is required").WithParams(
			errs.InvalidParam{Name: "--content", Reason: "provide local document content"},
			errs.InvalidParam{Name: "--doc", Reason: "provide an online document URL or token"},
		)
	}
	if content != "" && doc != "" {
		return errs.NewValidationError(errs.SubtypeInvalidArgument, "--content and --doc are mutually exclusive").WithParams(
			errs.InvalidParam{Name: "--content", Reason: "mutually exclusive with --doc"},
			errs.InvalidParam{Name: "--doc", Reason: "mutually exclusive with --content"},
		)
	}
	if doc != "" {
		if runtime.Str("command") != docsScriptParse {
			return errs.NewValidationError(errs.SubtypeInvalidArgument,
				"--doc is only supported with --command parse").WithParam("--doc")
		}
		if _, err := parseDocumentRef(doc); err != nil {
			return err
		}
		if err := runtime.EnsureScopes([]string{"docx:document:readonly"}); err != nil {
			return err
		}
	}
	if outputPath == "" {
		if runtime.Bool("overwrite") {
			return errs.NewValidationError(errs.SubtypeInvalidArgument, "--overwrite requires --output").WithParam("--overwrite")
		}
		return nil
	}
	if runtime.Str("command") != docsScriptMarkdownToXML {
		return errs.NewValidationError(errs.SubtypeInvalidArgument,
			"--output is only supported with --command markdown-to-xml").WithParam("--output")
	}
	if _, err := runtime.ResolveSavePath(outputPath); err != nil {
		return errs.NewValidationError(errs.SubtypeInvalidArgument, "unsafe output path: %s", err).
			WithParam("--output").
			WithCause(err)
	}
	return nil
}

func dryRunDocsScript(_ context.Context, runtime *common.RuntimeContext) *common.DryRunAPI {
	if runtime.Str("command") == docsScriptCreateTempXML {
		fileName := strings.TrimSpace(runtime.Str("file-name"))
		return common.NewDryRunAPI().
			Desc("Create a random directory and an empty named XML file inside it; no API call is made").
			Set("command", docsScriptCreateTempXML).
			Set("directory_pattern", docsScriptTempDirectoryPattern(fileName)).
			Set("file_name", fileName).
			Set("xml_file_name", docsScriptXMLFileName(fileName)).
			Set("creates_file", false).
			Set("network", false)
	}
	if doc := strings.TrimSpace(runtime.Str("doc")); doc != "" {
		ref, _ := parseDocumentRef(doc)
		return common.NewDryRunAPI().
			POST("/open-apis/docs_ai/v1/documents/:document_id/fetch").
			Desc("OpenAPI: fetch document for parsing and profiling").
			Body(docsScriptFetchBody(runtime)).
			Set("command", runtime.Str("command")).
			Set("document_id", ref.Token).
			Set("network", true)
	}
	dry := common.NewDryRunAPI().
		Desc("Local LarkOpenCLI document parsing or conversion; no API call is made").
		Set("command", runtime.Str("command")).
		Set("input_bytes", len(runtime.Str("content"))).
		Set("network", false)
	if outputPath := strings.TrimSpace(runtime.Str("output")); outputPath != "" {
		dry.Set("output", outputPath).Set("overwrite", runtime.Bool("overwrite"))
	}
	return dry
}

func executeDocsScript(_ context.Context, runtime *common.RuntimeContext) error {
	command := runtime.Str("command")
	content := runtime.Str("content")
	switch command {
	case docsScriptCreateTempXML:
		return createDocsScriptTempXML(runtime)
	case docsScriptParse:
		inputParam := "--content"
		inputLabel := "--content"
		if strings.TrimSpace(runtime.Str("doc")) != "" {
			var err error
			content, err = fetchDocsScriptContent(runtime)
			if err != nil {
				return err
			}
			inputParam = "--doc"
			inputLabel = "fetched document content"
		}
		profile, err := docxparse.ParseAuto(content)
		if err != nil {
			return errs.NewValidationError(errs.SubtypeInvalidArgument,
				"could not parse %s as LarkOpenCLI XML or Markdown: %s", inputLabel, err).
				WithParam(inputParam).
				WithCause(err)
		}
		runtime.OutFormatRaw(docsScriptParseResult{Profile: docsScriptPublicProfile{
			WordCount:  profile.WordCount,
			CharCount:  profile.CharCount,
			BlockCount: profile.BlockCount,
			Blocks:     profile.Blocks,
		}}, nil, nil)
		return nil
	case docsScriptMarkdownToXML:
		xml, err := docxparse.MarkdownToXML(content)
		if err != nil {
			return errs.NewValidationError(errs.SubtypeInvalidArgument,
				"could not convert --content from Markdown to LarkOpenCLI XML: %s", err).
				WithParam("--content").
				WithCause(err)
		}
		if outputPath := strings.TrimSpace(runtime.Str("output")); outputPath != "" {
			return saveDocsScriptXML(runtime, outputPath, xml)
		}
		runtime.OutFormatRaw(docsScriptMarkdownResult{XML: xml}, nil, nil)
		return nil
	default:
		return errs.NewValidationError(errs.SubtypeInvalidArgument,
			"unsupported --command %q", command).
			WithParam("--command")
	}
}

func createDocsScriptTempXML(runtime *common.RuntimeContext) error {
	creator, ok := runtime.FileIO().(fileio.TempDirFileCreator)
	if !ok {
		return errs.NewValidationError(errs.SubtypeFailedPrecondition,
			"the active file I/O provider does not support temporary file creation").
			WithHint("run this command with the local file I/O provider")
	}
	fileName := strings.TrimSpace(runtime.Str("file-name"))
	path, err := creator.CreateTempDirFile(docsScriptTempDirectoryPattern(fileName), docsScriptXMLFileName(fileName))
	if err != nil {
		return common.WrapSaveErrorTyped(err)
	}
	if _, err := runtime.ResolveSavePath(path); err != nil {
		return errs.NewInternalError(errs.SubtypeFileIO,
			"resolve temporary XML path %s: %s", path, err).
			WithCause(err)
	}
	runtime.Out(docsScriptTempXMLResult{
		Path: path,
	}, nil)
	return nil
}

func validateDocsScriptTempXMLFileName(fileName string) error {
	if fileName != filepath.Base(fileName) || strings.ContainsAny(fileName, "<>:\"/\\|?*\t\r\n") {
		return errs.NewValidationError(errs.SubtypeInvalidArgument,
			"--file-name must be a portable file name without path separators or reserved characters").WithParam("--file-name")
	}
	if err := charcheck.RejectControlChars(fileName, "--file-name"); err != nil {
		return errs.NewValidationError(errs.SubtypeInvalidArgument, "%s", err).
			WithParam("--file-name").
			WithCause(err)
	}
	if strings.HasSuffix(fileName, ".") || strings.HasSuffix(fileName, " ") {
		return errs.NewValidationError(errs.SubtypeInvalidArgument,
			"--file-name must not end with a dot or space").WithParam("--file-name")
	}
	if strings.EqualFold(filepath.Ext(fileName), ".xml") {
		return errs.NewValidationError(errs.SubtypeInvalidArgument,
			"--file-name must omit the .xml extension").WithParam("--file-name")
	}
	base := strings.ToUpper(strings.SplitN(fileName, ".", 2)[0])
	if isWindowsReservedFileName(base) {
		return errs.NewValidationError(errs.SubtypeInvalidArgument,
			"--file-name uses a Windows-reserved device name").WithParam("--file-name")
	}
	return nil
}

func docsScriptTempDirectoryPattern(fileName string) string {
	return fileName + docsScriptTempDirSuffix
}

func docsScriptXMLFileName(fileName string) string {
	return fileName + ".xml"
}

func isWindowsReservedFileName(base string) bool {
	switch base {
	case "CON", "PRN", "AUX", "NUL":
		return true
	}
	if len(base) == 4 && (strings.HasPrefix(base, "COM") || strings.HasPrefix(base, "LPT")) {
		return base[3] >= '1' && base[3] <= '9'
	}
	return false
}

func docsScriptFetchBody(runtime *common.RuntimeContext) map[string]interface{} {
	body := map[string]interface{}{
		"format":      "xml",
		"extra_param": docsFetchExtraParam,
		"export_option": map[string]interface{}{
			"export_block_id":        false,
			"export_style_attrs":     false,
			"export_cite_extra_data": false,
		},
	}
	if lang := resolveFetchLang(runtime); lang != "" {
		body["lang"] = lang
	}
	return body
}

func fetchDocsScriptContent(runtime *common.RuntimeContext) (string, error) {
	ref, _ := parseDocumentRef(runtime.Str("doc"))
	apiPath := fmt.Sprintf("/open-apis/docs_ai/v1/documents/%s/fetch", ref.Token)
	data, err := doDocAPI(runtime, "POST", apiPath, docsScriptFetchBody(runtime))
	if err != nil {
		return "", err
	}
	document, ok := data["document"].(map[string]interface{})
	if !ok || document == nil {
		return "", errs.NewInternalError(errs.SubtypeUnknown,
			"document fetch response for --doc is missing document")
	}
	content, ok := document["content"].(string)
	if !ok {
		return "", errs.NewInternalError(errs.SubtypeUnknown,
			"document fetch response for --doc is missing document.content")
	}
	return content, nil
}

func saveDocsScriptXML(runtime *common.RuntimeContext, outputPath, xml string) error {
	if !runtime.Bool("overwrite") {
		if _, err := runtime.FileIO().Stat(outputPath); err == nil {
			return errs.NewValidationError(errs.SubtypeFailedPrecondition,
				"output file already exists: %s (use --overwrite to replace)", outputPath).
				WithParam("--output")
		} else if !errors.Is(err, fs.ErrNotExist) {
			if errors.Is(err, fileio.ErrPathValidation) {
				return errs.NewValidationError(errs.SubtypeInvalidArgument, "unsafe output path: %s", err).
					WithParam("--output").
					WithCause(err)
			}
			return errs.NewInternalError(errs.SubtypeFileIO,
				"cannot access output path %s: %s", outputPath, err).
				WithCause(err)
		}
	}

	result, err := runtime.FileIO().Save(outputPath, fileio.SaveOptions{
		ContentType:   "application/xml",
		ContentLength: int64(len(xml)),
	}, strings.NewReader(xml))
	if err != nil {
		return common.WrapSaveErrorTyped(err)
	}
	savedPath, err := runtime.ResolveSavePath(outputPath)
	if err != nil {
		return errs.NewInternalError(errs.SubtypeFileIO,
			"resolve saved XML path %s: %s", outputPath, err).
			WithCause(err)
	}
	runtime.Out(docsScriptMarkdownFileResult{
		SavedPath: savedPath,
		SizeBytes: result.Size(),
	}, nil)
	return nil
}
