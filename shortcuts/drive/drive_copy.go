// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package drive

import (
	"context"
	"fmt"
	"strings"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/validate"
	"github.com/larksuite/cli/shortcuts/common"
)

const driveCopyMaxNameBytes = 256

var driveCopyTypes = []string{"doc", "docx", "sheet", "file", "mindnote", "slides", "bitable", "base", "wiki"}

type driveCopyRef struct {
	Token      string
	Type       string
	SourceFlag string
}

type driveCopyExtra struct {
	Key   string
	Value string
}

type driveCopySpec struct {
	Ref         driveCopyRef
	Name        string
	FolderToken string
	Extras      []driveCopyExtra
}

// DriveCopy copies a Drive file into a target folder through the Drive copy
// API, with URL parsing. Wiki inputs are rejected with a redirect to the
// existing `wiki +node-copy` shortcut.
var DriveCopy = common.Shortcut{
	Service:     "drive",
	Command:     "+copy",
	Description: "Copy a doc/docx/sheet/file/mindnote/slides/base(bitable) into a target folder, with URL parsing; wiki inputs are redirected to wiki +node-copy",
	Risk:        "write",
	Scopes:      []string{"docs:document:copy"},
	AuthTypes:   []string{"user", "bot"},
	Flags: []common.Flag{
		{Name: "url", Desc: "recommended: Lark/Feishu document URL (doc/docx/sheet/file/mindnote/slides/base/bitable)"},
		{Name: "token", Desc: "document token or document URL; bare tokens require --type"},
		{Name: "type", Desc: "document type for bare --token; optional for URLs but must match the URL type when provided", Enum: driveCopyTypes},
		{Name: "name", Desc: "name for the copied file, up to 256 bytes", Required: true},
		{Name: "folder-token", Desc: "target folder token or folder URL; the tenant root folder token creates the copy in My Space root", Required: true},
		{Name: "extra", Type: "string_array", Desc: "repeatable key=value pair forwarded verbatim as a custom copy parameter, e.g. --extra target_type=docx to convert a legacy doc into a docx copy"},
	},
	Tips: []string{
		"The source type must match the real file type; the API rejects mismatches.",
		"Use `--extra target_type=docx` with a legacy doc source to create the copy as a new-version docx.",
	},
	Validate: func(ctx context.Context, runtime *common.RuntimeContext) error {
		_, err := readDriveCopySpec(runtime)
		return err
	},
	DryRun: func(ctx context.Context, runtime *common.RuntimeContext) *common.DryRunAPI {
		spec, err := readDriveCopySpec(runtime)
		if err != nil {
			return common.NewDryRunAPI().Set("error", err.Error())
		}
		return buildDriveCopyDryRun(spec)
	},
	Execute: func(ctx context.Context, runtime *common.RuntimeContext) error {
		spec, err := readDriveCopySpec(runtime)
		if err != nil {
			return err
		}

		fmt.Fprintf(runtime.IO().ErrOut, "Copying %s %s to folder %s...\n",
			spec.Ref.Type, common.MaskToken(spec.Ref.Token), common.MaskToken(spec.FolderToken))

		data, err := runtime.CallAPITyped(
			"POST",
			fmt.Sprintf("/open-apis/drive/v1/files/%s/copy", validate.EncodePathSegment(spec.Ref.Token)),
			nil,
			buildDriveCopyBody(spec),
		)
		if err != nil {
			return err
		}

		runtime.Out(buildDriveCopyOutput(runtime, spec, data), nil)
		return nil
	},
}

func readDriveCopySpec(runtime *common.RuntimeContext) (driveCopySpec, error) {
	ref, err := resolveDriveCopyInput(runtime.Str("url"), runtime.Str("token"), runtime.Str("type"))
	if err != nil {
		return driveCopySpec{}, err
	}
	spec := driveCopySpec{
		Ref:  ref,
		Name: strings.TrimSpace(runtime.Str("name")),
	}
	spec.FolderToken, err = resolveDriveCopyFolderToken(runtime.Str("folder-token"))
	if err != nil {
		return driveCopySpec{}, err
	}
	spec.Extras, err = parseDriveCopyExtras(runtime.StrArray("extra"))
	if err != nil {
		return driveCopySpec{}, err
	}
	if err := validateDriveCopySpec(spec); err != nil {
		return driveCopySpec{}, err
	}
	return spec, nil
}

// parseDriveCopyExtras converts repeated `key=value` specs into the API's
// extra parameter shape, preserving order and transcribing values verbatim.
func parseDriveCopyExtras(specs []string) ([]driveCopyExtra, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	extras := make([]driveCopyExtra, 0, len(specs))
	for _, spec := range specs {
		key, value, found := strings.Cut(spec, "=")
		if !found {
			return nil, errs.NewValidationError(errs.SubtypeInvalidArgument, "invalid --extra %q: expected format key=value", spec).WithParam("--extra")
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, errs.NewValidationError(errs.SubtypeInvalidArgument, "invalid --extra %q: key must not be empty", spec).WithParam("--extra")
		}
		if value == "" {
			return nil, errs.NewValidationError(errs.SubtypeInvalidArgument, "invalid --extra %q: value must not be empty", spec).WithParam("--extra")
		}
		extras = append(extras, driveCopyExtra{Key: key, Value: value})
	}
	return extras, nil
}

func validateDriveCopySpec(spec driveCopySpec) error {
	if spec.Name == "" {
		return errs.NewValidationError(errs.SubtypeInvalidArgument, "--name must not be empty or whitespace-only").WithParam("--name")
	}
	if len(spec.Name) > driveCopyMaxNameBytes {
		return errs.NewValidationError(errs.SubtypeInvalidArgument, "--name exceeds %d bytes (got %d)", driveCopyMaxNameBytes, len(spec.Name)).WithParam("--name")
	}
	return nil
}

func resolveDriveCopyInput(urlInput, tokenInput, explicitType string) (driveCopyRef, error) {
	urlInput = strings.TrimSpace(urlInput)
	tokenInput = strings.TrimSpace(tokenInput)
	if urlInput != "" && tokenInput != "" {
		return driveCopyRef{}, errs.NewValidationError(errs.SubtypeInvalidArgument, "--url and --token are mutually exclusive; pass one input only").WithParam("--url")
	}
	if urlInput == "" && tokenInput == "" {
		return driveCopyRef{}, errs.NewValidationError(errs.SubtypeInvalidArgument, "specify --url or --token").WithParam("--url")
	}

	raw := urlInput
	sourceFlag := "--url"
	if raw == "" {
		raw = tokenInput
		sourceFlag = "--token"
	}
	inputType := normalizeDriveCopyType(strings.ToLower(strings.TrimSpace(explicitType)))

	if ref, ok := common.ParseResourceURL(raw); ok {
		refType := normalizeDriveCopyType(ref.Type)
		if inputType != "" && inputType != refType {
			return driveCopyRef{}, errs.NewValidationError(
				errs.SubtypeInvalidArgument,
				"--type %q conflicts with URL path type %q; remove --type or use a matching value",
				inputType,
				refType,
			).WithParam("--type")
		}
		if refType == "wiki" {
			return driveCopyRef{}, driveCopyWikiRedirectError(sourceFlag, ref.Token)
		}
		if !driveCopyTypeSupported(refType) {
			return driveCopyRef{}, errs.NewValidationError(
				errs.SubtypeInvalidArgument,
				"unsupported %s resource type %q; drive copy supports doc, docx, sheet, file, mindnote, slides, and bitable/base",
				sourceFlag,
				refType,
			).WithParam(sourceFlag)
		}
		return driveCopyRef{Token: ref.Token, Type: refType, SourceFlag: sourceFlag}, nil
	}

	if strings.Contains(raw, "://") {
		return driveCopyRef{}, errs.NewValidationError(errs.SubtypeInvalidArgument, "unsupported %s URL %q: use a recognized Lark document URL or pass a bare token with --type", sourceFlag, raw).WithParam(sourceFlag)
	}
	if strings.ContainsAny(raw, "/?#") {
		return driveCopyRef{}, errs.NewValidationError(errs.SubtypeInvalidArgument, "invalid bare token %q: remove path/query fragments or pass a recognized Lark document URL", raw).WithParam(sourceFlag)
	}
	if inputType == "" {
		return driveCopyRef{}, errs.NewValidationError(errs.SubtypeInvalidArgument, "--type is required when %s is a bare token (allowed: doc, docx, sheet, file, mindnote, slides, bitable, base)", sourceFlag).WithParam("--type")
	}
	if inputType == "wiki" {
		return driveCopyRef{}, driveCopyWikiRedirectError("--type", raw)
	}
	if !driveCopyTypeSupported(inputType) {
		return driveCopyRef{}, errs.NewValidationError(errs.SubtypeInvalidArgument, "invalid --type %q; allowed: doc, docx, sheet, file, mindnote, slides, bitable, base", inputType).WithParam("--type")
	}
	return driveCopyRef{Token: raw, Type: inputType, SourceFlag: sourceFlag}, nil
}

// driveCopyWikiRedirectError guides wiki inputs to the dedicated wiki copy
// command instead of the Drive copy API, which cannot place copies in the
// wiki tree.
func driveCopyWikiRedirectError(param, nodeToken string) *errs.ValidationError {
	return errs.NewValidationError(
		errs.SubtypeInvalidArgument,
		"wiki node %q cannot be copied with drive +copy; use wiki +node-copy instead",
		nodeToken,
	).WithParam(param).WithHint(
		"run: lark-cli wiki +node-copy --space-id <space-id> --node-token %s --target-space-id <target-space-id> (or --target-parent-node-token); resolve <space-id> with: lark-cli wiki +node-get --token %s",
		nodeToken,
		nodeToken,
	)
}

func resolveDriveCopyFolderToken(input string) (string, error) {
	input = strings.TrimSpace(input)
	if ref, ok := common.ParseResourceURL(input); ok {
		if ref.Type != "folder" {
			return "", errs.NewValidationError(
				errs.SubtypeInvalidArgument,
				"--folder-token URL resolves to %q, not a folder; pass a folder URL or a folder token",
				ref.Type,
			).WithParam("--folder-token")
		}
		return ref.Token, nil
	}
	if err := validate.ResourceName(input, "--folder-token"); err != nil {
		return "", errs.NewValidationError(errs.SubtypeInvalidArgument, "%s", err).WithParam("--folder-token")
	}
	return input, nil
}

func normalizeDriveCopyType(docType string) string {
	switch strings.TrimSpace(docType) {
	case "base":
		return "bitable"
	default:
		return strings.TrimSpace(docType)
	}
}

func driveCopyTypeSupported(docType string) bool {
	switch normalizeDriveCopyType(docType) {
	case "doc", "docx", "sheet", "file", "mindnote", "slides", "bitable":
		return true
	default:
		return false
	}
}

func buildDriveCopyBody(spec driveCopySpec) map[string]interface{} {
	body := map[string]interface{}{
		"name":         spec.Name,
		"type":         spec.Ref.Type,
		"folder_token": spec.FolderToken,
	}
	if len(spec.Extras) > 0 {
		extras := make([]map[string]interface{}, 0, len(spec.Extras))
		for _, extra := range spec.Extras {
			extras = append(extras, map[string]interface{}{"key": extra.Key, "value": extra.Value})
		}
		body["extra"] = extras
	}
	return body
}

func buildDriveCopyDryRun(spec driveCopySpec) *common.DryRunAPI {
	return common.NewDryRunAPI().
		Desc("1-step request: copy file into target folder").
		POST("/open-apis/drive/v1/files/:file_token/copy").
		Body(buildDriveCopyBody(spec)).
		Set("file_token", spec.Ref.Token)
}

func buildDriveCopyOutput(runtime *common.RuntimeContext, spec driveCopySpec, data map[string]interface{}) map[string]interface{} {
	out := map[string]interface{}{
		"copied":            true,
		"source_file_token": spec.Ref.Token,
		"source_type":       spec.Ref.Type,
		"folder_token":      spec.FolderToken,
	}
	file := common.GetMap(data, "file")
	if token := common.GetString(file, "token"); token != "" {
		out["file_token"] = token
		if url := common.GetString(file, "url"); url != "" {
			out["url"] = url
		} else if built := common.BuildResourceURL(runtime.Config.Brand, common.GetString(file, "type"), token); built != "" {
			out["url"] = built
		}
	}
	if fileType := common.GetString(file, "type"); fileType != "" {
		out["file_type"] = fileType
	}
	if name := common.GetString(file, "name"); name != "" {
		out["name"] = name
	}
	return out
}
