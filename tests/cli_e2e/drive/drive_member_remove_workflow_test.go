// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package drive

import (
	"context"
	"testing"
	"time"

	clie2e "github.com/larksuite/cli/tests/cli_e2e"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestDrive_MemberRemoveWorkflowAsUser(t *testing.T) {
	clie2e.SkipWithoutTenantAccessToken(t)
	clie2e.SkipWithoutUserToken(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	suffix := clie2e.GenerateSuffix()
	folderToken := CreateDriveFolder(t, t, ctx, "lark-cli-e2e-member-remove-"+suffix, "user", "")
	docToken := createMemberRemoveWorkflowDoc(t, ctx, folderToken, suffix)
	botOpenID := getMemberRemoveWorkflowBotOpenID(t, ctx)

	addResult, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args: []string{
			"drive", "+member-add",
			"--token", docToken,
			"--type", "docx",
			"--member-id", botOpenID,
			"--member-type", "openid",
			"--perm", "view",
			"--yes",
		},
		DefaultAs: "user",
	})
	require.NoError(t, err)
	addResult.AssertExitCode(t, 0)
	addResult.AssertStdoutStatus(t, true)
	require.Equal(t, botOpenID, gjson.Get(addResult.Stdout, "data.member_id").String(), "stdout:\n%s", addResult.Stdout)

	removeResult, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args: []string{
			"drive", "+member-remove",
			"--token", docToken,
			"--type", "docx",
			"--member-id", botOpenID,
			"--member-type", "openid",
			"--yes",
		},
		DefaultAs: "user",
	})
	require.NoError(t, err)
	removeResult.AssertExitCode(t, 0)
	removeResult.AssertStdoutStatus(t, true)
	require.True(t, gjson.Get(removeResult.Stdout, "data.removed").Bool(), "stdout:\n%s", removeResult.Stdout)
	require.Equal(t, docToken, gjson.Get(removeResult.Stdout, "data.resource_token").String(), "stdout:\n%s", removeResult.Stdout)
	require.Equal(t, "docx", gjson.Get(removeResult.Stdout, "data.resource_type").String(), "stdout:\n%s", removeResult.Stdout)
	require.Equal(t, botOpenID, gjson.Get(removeResult.Stdout, "data.member_id").String(), "stdout:\n%s", removeResult.Stdout)
	require.Equal(t, "openid", gjson.Get(removeResult.Stdout, "data.member_type").String(), "stdout:\n%s", removeResult.Stdout)
}

func createMemberRemoveWorkflowDoc(t *testing.T, ctx context.Context, folderToken, suffix string) string {
	t.Helper()

	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args: []string{
			"docs", "+create",
			"--parent-token", folderToken,
			"--doc-format", "markdown",
			"--content", "# member-remove-" + suffix + "\n\nTemporary permission workflow fixture.",
		},
		DefaultAs: "user",
	})
	require.NoError(t, err)
	result.AssertExitCode(t, 0)
	result.AssertStdoutStatus(t, true)

	docToken := gjson.Get(result.Stdout, "data.document.document_id").String()
	require.NotEmpty(t, docToken, "stdout:\n%s", result.Stdout)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := clie2e.CleanupContext()
		defer cleanupCancel()

		deleteResult, deleteErr := DeleteDriveResourceAndVerify(cleanupCtx, docToken, "docx", "user")
		clie2e.ReportCleanupFailure(t, "delete member-remove workflow doc "+docToken, deleteResult, deleteErr)
	})
	return docToken
}

func getMemberRemoveWorkflowBotOpenID(t *testing.T, ctx context.Context) string {
	t.Helper()

	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args:      []string{"auth", "status", "--json", "--verify"},
		DefaultAs: "bot",
	})
	require.NoError(t, err)
	result.AssertExitCode(t, 0)
	require.True(t, gjson.Get(result.Stdout, "identities.bot.verified").Bool(), "stdout:\n%s", result.Stdout)

	openID := gjson.Get(result.Stdout, "identities.bot.openId").String()
	require.NotEmpty(t, openID, "stdout:\n%s", result.Stdout)
	return openID
}
