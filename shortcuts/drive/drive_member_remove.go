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

var driveMemberRemoveIDTypes = []string{
	"email", "openid", "openchat", "opendepartmentid",
	"userid", "unionid", "groupid", "wikispaceid",
}

// DriveMemberRemove removes one collaborator/member permission from a Drive resource.
var DriveMemberRemove = common.Shortcut{
	Service:     "drive",
	Command:     "+member-remove",
	Description: "Remove one collaborator/member permission from a Drive document, file, folder, or wiki node",
	Risk:        "high-risk-write",
	Scopes:      []string{"docs:permission.member:delete"},
	AuthTypes:   []string{"user", "bot"},
	Flags: []common.Flag{
		{Name: "token", Desc: "target token or document URL; type is auto-inferred from URL path when omitted", Required: true},
		{Name: "type", Desc: "target resource type; required when --token is a bare token"},
		{Name: "member-id", Desc: "single collaborator ID to remove; comma-separated values are rejected", Required: true},
		{Name: "member-type", Desc: "ID type for --member-id; supported: email|openid|openchat|opendepartmentid|userid|unionid|groupid|wikispaceid", Required: true},
		{Name: "member-kind", Desc: "request body type when --member-type=wikispaceid; one of wiki_space_member|wiki_space_viewer|wiki_space_editor"},
		{Name: "perm-type", Desc: "wiki permission scope; defaults to container; rejected for non-wiki types and wiki-space members"},
	},
	Tips: []string{
		"This command removes exactly one collaborator; run separate commands for multiple members.",
		"Resource type is auto-inferred from URL paths; pass --type when --token is a bare token.",
		"When --member-type=wikispaceid, pass --member-kind wiki_space_member, wiki_space_viewer, or wiki_space_editor.",
		"For ordinary wiki collaborators, --perm-type defaults to container; use single_page to remove only the current-page permission.",
		"Department collaborator removal (--member-type=opendepartmentid) requires --as user; bot identity is not supported.",
		"A successful response confirms that the removal request completed; it does not prove the permission previously existed.",
	},
	Validate: func(ctx context.Context, runtime *common.RuntimeContext) error {
		_, err := readDriveMemberRemoveSpec(runtime)
		return err
	},
	DryRun: func(ctx context.Context, runtime *common.RuntimeContext) *common.DryRunAPI {
		spec, err := readDriveMemberRemoveSpec(runtime)
		if err != nil {
			return common.NewDryRunAPI().Set("error", err.Error())
		}
		return buildDriveMemberRemoveDryRun(spec)
	},
	Execute: func(ctx context.Context, runtime *common.RuntimeContext) error {
		spec, err := readDriveMemberRemoveSpec(runtime)
		if err != nil {
			return err
		}
		return executeDriveMemberRemove(runtime, spec)
	},
}

type driveMemberRemoveSpec struct {
	Token        string
	ResourceType string
	MemberID     string
	MemberType   string
	MemberKind   string
	PermType     string
}

func (spec driveMemberRemoveSpec) APIQueryParams() map[string]interface{} {
	return map[string]interface{}{
		"type":        spec.ResourceType,
		"member_type": spec.MemberType,
	}
}

func (spec driveMemberRemoveSpec) Body() map[string]interface{} {
	body := make(map[string]interface{}, 2)
	if memberKind := driveMemberAddBodyType(spec.MemberType, spec.MemberKind); memberKind != "" {
		body["type"] = memberKind
	}
	if spec.PermType != "" {
		body["perm_type"] = spec.PermType
	}
	return body
}

func readDriveMemberRemoveSpec(runtime *common.RuntimeContext) (driveMemberRemoveSpec, error) {
	token, resourceType, err := resolveDriveMemberAddTarget(runtime.Str("token"), runtime.Str("type"))
	if err != nil {
		return driveMemberRemoveSpec{}, err
	}
	if strings.Contains(token, "/") {
		return driveMemberRemoveSpec{}, errs.NewValidationError(
			errs.SubtypeInvalidArgument,
			"--token must resolve to a single resource token and cannot contain '/'",
		).WithParam("--token")
	}

	memberID := strings.TrimSpace(runtime.Str("member-id"))
	if memberID == "" {
		return driveMemberRemoveSpec{}, errs.NewValidationError(
			errs.SubtypeInvalidArgument,
			"--member-id is required and cannot be blank",
		).WithParam("--member-id")
	}
	if strings.Contains(memberID, "/") {
		return driveMemberRemoveSpec{}, errs.NewValidationError(
			errs.SubtypeInvalidArgument,
			"--member-id must be a single collaborator ID and cannot contain '/'",
		).WithParam("--member-id")
	}
	if strings.Contains(memberID, ",") {
		return driveMemberRemoveSpec{}, errs.NewValidationError(
			errs.SubtypeInvalidArgument,
			"--member-id accepts exactly one collaborator ID; run one +member-remove command per member",
		).WithParam("--member-id")
	}

	memberType, err := resolveDriveMemberRemoveMemberType(memberID, runtime.Str("member-type"))
	if err != nil {
		return driveMemberRemoveSpec{}, err
	}
	if memberType == "wikispaceid" && resourceType != "wiki" {
		return driveMemberRemoveSpec{}, errs.NewValidationError(
			errs.SubtypeInvalidArgument,
			"--member-type=wikispaceid only applies when resource type is wiki; got %q",
			resourceType,
		).WithParam("--member-type")
	}
	memberKind, err := resolveDriveMemberAddMemberKind(memberType, runtime.Str("member-kind"))
	if err != nil {
		return driveMemberRemoveSpec{}, err
	}

	permType, err := normalizeDriveMemberAddEnumValue(runtime.Str("perm-type"), driveMemberAddPermTypes, "--perm-type")
	if err != nil {
		return driveMemberRemoveSpec{}, err
	}
	if resourceType == "wiki" && memberType == "wikispaceid" {
		if runtime.Changed("perm-type") {
			return driveMemberRemoveSpec{}, errs.NewValidationError(
				errs.SubtypeInvalidArgument,
				"--perm-type is not supported when --member-type=wikispaceid; use --member-kind wiki_space_member|wiki_space_viewer|wiki_space_editor",
			).WithParam("--perm-type")
		}
		permType = ""
	} else if resourceType == "wiki" && permType == "" {
		permType = driveMemberAddDefaultPermType(resourceType)
	} else if resourceType != "wiki" && runtime.Changed("perm-type") {
		return driveMemberRemoveSpec{}, errs.NewValidationError(
			errs.SubtypeInvalidArgument,
			"--perm-type only applies when resource type is wiki; got %q",
			resourceType,
		).WithParam("--perm-type")
	} else if resourceType != "wiki" {
		permType = ""
	}

	spec := driveMemberRemoveSpec{
		Token:        token,
		ResourceType: resourceType,
		MemberID:     memberID,
		MemberType:   memberType,
		MemberKind:   memberKind,
		PermType:     permType,
	}
	if runtime.As().IsBot() && spec.MemberType == "opendepartmentid" {
		return driveMemberRemoveSpec{}, errs.NewValidationError(
			errs.SubtypeInvalidArgument,
			"--member-type=opendepartmentid requires --as user; bot identity does not support removing department collaborators",
		).WithParam("--member-type")
	}
	return spec, nil
}

func resolveDriveMemberRemoveMemberType(memberID, explicit string) (string, error) {
	memberType, err := normalizeDriveMemberAddEnumValue(explicit, driveMemberRemoveIDTypes, "--member-type")
	if err != nil {
		return "", err
	}
	if memberType == "" {
		return "", errs.NewValidationError(
			errs.SubtypeInvalidArgument,
			"--member-type is required; accepted values: %s",
			strings.Join(driveMemberRemoveIDTypes, ", "),
		).WithParam("--member-type")
	}

	// User IDs are tenant-defined and may resemble another supported ID format.
	if memberType != "userid" {
		if expected := inferMemberTypeFromID(memberID); expected != "" && expected != memberType {
			return "", errs.NewValidationError(
				errs.SubtypeInvalidArgument,
				"member-id %q prefix implies --member-type %s, but --member-type %s was provided; fix the ID or use the matching member type",
				memberID,
				expected,
				memberType,
			).WithParam("--member-id")
		}
	}
	return memberType, nil
}

func buildDriveMemberRemoveDryRun(spec driveMemberRemoveSpec) *common.DryRunAPI {
	return common.NewDryRunAPI().
		Desc("Remove Drive collaborator/member permission").
		DELETE(driveMemberRemovePath(spec)).
		Params(spec.APIQueryParams()).
		Body(spec.Body())
}

func driveMemberRemovePath(spec driveMemberRemoveSpec) string {
	return fmt.Sprintf(
		"/open-apis/drive/v1/permissions/%s/members/%s",
		validate.EncodePathSegment(spec.Token),
		validate.EncodePathSegment(spec.MemberID),
	)
}

func executeDriveMemberRemove(runtime *common.RuntimeContext, spec driveMemberRemoveSpec) error {
	fmt.Fprintf(
		runtime.IO().ErrOut,
		"Removing Drive member %s (type=%s) from %s %s...\n",
		common.MaskToken(spec.MemberID),
		spec.MemberType,
		spec.ResourceType,
		common.MaskToken(spec.Token),
	)

	if _, err := runtime.CallAPITyped(
		"DELETE",
		driveMemberRemovePath(spec),
		spec.APIQueryParams(),
		spec.Body(),
	); err != nil {
		return err
	}

	out := driveMemberRemoveOutput(spec)

	fmt.Fprintf(runtime.IO().ErrOut, "Removed Drive member %s\n", common.MaskToken(spec.MemberID))
	runtime.Out(out, nil)
	return nil
}

func driveMemberRemoveOutput(spec driveMemberRemoveSpec) map[string]interface{} {
	out := map[string]interface{}{
		"removed":        true,
		"resource_token": spec.Token,
		"resource_type":  spec.ResourceType,
		"member_id":      spec.MemberID,
		"member_type":    spec.MemberType,
	}
	if memberKind := driveMemberAddBodyType(spec.MemberType, spec.MemberKind); memberKind != "" {
		out["member_kind"] = memberKind
	}
	if spec.PermType != "" {
		out["perm_type"] = spec.PermType
	}
	return out
}
