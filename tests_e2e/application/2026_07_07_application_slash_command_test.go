// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package application

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	clie2e "github.com/larksuite/cli/tests/cli_e2e"
)

// TestApplicationSlashCommand_LifecycleWorkflow proves the full self-contained
// CRUD loop from spec §7 as the bot identity:
//
//	create(i18n+icon) → list → update by name → create --force (name clash →
//	PATCH) → duplicate create without --force (deterministic API error) →
//	delete --yes → list confirms removal.
//
// Spec §3 (live-verified) states server-side state is immediate ("list
// reflects it immediately"), so every read-after-write asserts directly
// without retry — the immediacy itself is part of the contract under test.
//
// Identity: spec §3 proves both tat/uat return 200 upstream; the sandbox user
// token has no application:app_slash_command:* scope (OAuth isolation, spec
// §4.0 — the scope is deliberately excluded from `auth login --domain all`),
// so live coverage runs bot-only. See coverage doc, Identity Analysis.
func TestApplicationSlashCommand_LifecycleWorkflow(t *testing.T) {
	parentT := t
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	// Slash command names are server-unique per app; keep the charset
	// conservative (lowercase alnum) since server-side naming rules beyond
	// "must not start with /" are undocumented.
	name := "e2ecmd" + strings.ReplaceAll(clie2e.GenerateSuffix(), "-", "")

	const (
		initialDesc = "e2e initial description"
		updatedDesc = "e2e updated description"
		forcedDesc  = "e2e forced description"
		i18nZh      = "e2e 中文描述"
		i18nEn      = "e2e english description"
	)

	var commandID string
	deleted := false
	scopeSkipReason := ""

	// guardScope implements spec §8 R1: when the sandbox app is not in the
	// scope gray-release, skip with the reason instead of a misleading FAIL.
	// Every non-permission failure falls through to the unconditional asserts.
	guardScope := func(t *testing.T, result *clie2e.Result) {
		t.Helper()
		if skip, reason := isScopePermissionError(result); skip {
			scopeSkipReason = reason
			t.Skip("spec §8 R1: sandbox app lacks application:app_slash_command scope grant; " + reason)
		}
	}
	requireLiveState := func(t *testing.T) {
		t.Helper()
		if scopeSkipReason != "" {
			t.Skip("skipped: earlier step hit scope/permission error; " + scopeSkipReason)
		}
		require.NotEmpty(t, commandID, "create step must have produced a command_id first")
	}

	t.Run("create with i18n and icon", func(t *testing.T) {
		result, err := clie2e.RunCmd(ctx, clie2e.Request{
			Args: []string{
				"application", "+slash-command-create",
				"--command", name,
				"--description", initialDesc,
				"--description-i18n", "zh_cn=" + i18nZh,
				"--description-i18n", "en_us=" + i18nEn,
				"--icon-key", validIconKey,
			},
			DefaultAs: "bot",
		})
		require.NoError(t, err)
		guardScope(t, result)
		result.AssertExitCode(t, 0)
		result.AssertStdoutStatus(t, true)

		data := gjson.Get(result.Stdout, "data")
		require.True(t, data.Exists(), "data must exist, stdout:\n%s", result.Stdout)
		assert.Equal(t, "created", data.Get("action").String(),
			"spec §4.2: fresh create must report action=created, stdout:\n%s", result.Stdout)
		assert.Equal(t, name, data.Get("command").String(), "stdout:\n%s", result.Stdout)

		commandID = data.Get("command_id").String()
		require.NotEmpty(t, commandID, "create must return command_id, stdout:\n%s", result.Stdout)
		registerSlashCommandCleanup(parentT, commandID, &deleted)

		// Spec §3 (live-verified): create returns the full resource object,
		// not just command_id.
		assert.Equal(t, initialDesc, data.Get("description.default_value").String(),
			"stdout:\n%s", result.Stdout)
		assert.Equal(t, i18nZh, data.Get("description.i18n.zh_cn").String(),
			"--description-i18n zh_cn must round-trip, stdout:\n%s", result.Stdout)
		assert.Equal(t, i18nEn, data.Get("description.i18n.en_us").String(),
			"--description-i18n en_us must round-trip, stdout:\n%s", result.Stdout)
		assert.Equal(t, validIconKey, data.Get("icon.icon_key").String(),
			"--icon-key must round-trip, stdout:\n%s", result.Stdout)
		assert.NotEmpty(t, data.Get("create_time").String(),
			"spec §3: create response carries create_time, stdout:\n%s", result.Stdout)

		assert.Contains(t, result.Stdout+result.Stderr, clientCacheHintFragment,
			"spec §4.0: successful writes must surface the client-cache hint, stdout:\n%s\nstderr:\n%s",
			result.Stdout, result.Stderr)
	})

	t.Run("list shows created command immediately", func(t *testing.T) {
		requireLiveState(t)
		result, err := clie2e.RunCmd(ctx, clie2e.Request{
			Args:      []string{"application", "+slash-command-list"},
			DefaultAs: "bot",
		})
		require.NoError(t, err)
		guardScope(t, result)
		result.AssertExitCode(t, 0)
		result.AssertStdoutStatus(t, true)

		items := gjson.Get(result.Stdout, "data.items")
		require.True(t, items.IsArray(), "data.items must be an array, stdout:\n%s", result.Stdout)
		count := gjson.Get(result.Stdout, "data.count")
		require.True(t, count.Exists(), "spec §4.1: data.count is part of the contract, stdout:\n%s", result.Stdout)
		assert.Equal(t, int64(len(items.Array())), count.Int(),
			"data.count must equal len(data.items), stdout:\n%s", result.Stdout)

		item, found := findItemByCommandID(result.Stdout, commandID)
		require.True(t, found,
			"created command %s must appear in list immediately (spec §3: server-side is immediate), stdout:\n%s",
			commandID, result.Stdout)
		assert.Equal(t, name, item.Get("command").String(), "item:\n%s", item.Raw)
		assert.Equal(t, initialDesc, item.Get("description.default_value").String(), "item:\n%s", item.Raw)
		assert.Equal(t, validIconKey, item.Get("icon.icon_key").String(), "item:\n%s", item.Raw)
		assert.NotEmpty(t, item.Get("create_time").String(),
			"spec §3: list items carry create_time (undocumented upstream, live-verified), item:\n%s", item.Raw)
	})

	t.Run("update by name changes description", func(t *testing.T) {
		requireLiveState(t)
		result, err := clie2e.RunCmd(ctx, clie2e.Request{
			Args: []string{
				"application", "+slash-command-update",
				"--command", name,
				"--description", updatedDesc,
			},
			DefaultAs: "bot",
		})
		require.NoError(t, err)
		guardScope(t, result)
		result.AssertExitCode(t, 0)
		result.AssertStdoutStatus(t, true)

		data := gjson.Get(result.Stdout, "data")
		assert.Equal(t, "updated", data.Get("action").String(), "stdout:\n%s", result.Stdout)
		// Name→id resolution must land on the resource we created.
		assert.Equal(t, commandID, data.Get("command_id").String(),
			"--command name resolution must resolve to the created command_id, stdout:\n%s", result.Stdout)
		assert.Equal(t, updatedDesc, data.Get("description.default_value").String(),
			"stdout:\n%s", result.Stdout)
		assert.NotEmpty(t, data.Get("update_time").String(),
			"spec §3: PATCH response carries update_time, stdout:\n%s", result.Stdout)
		// Spec §3 + §5 (live-verified): PATCH replaces sent top-level fields
		// wholesale — sending description without i18n deletes the i18n texts.
		assert.False(t, data.Get("description.i18n.zh_cn").Exists(),
			"PATCH wholesale-replaces description; zh_cn i18n not resent must be gone, stdout:\n%s", result.Stdout)

		assert.Contains(t, result.Stdout+result.Stderr, clientCacheHintFragment,
			"spec §4.0: successful writes must surface the client-cache hint, stderr:\n%s", result.Stderr)
	})

	t.Run("read after update", func(t *testing.T) {
		requireLiveState(t)
		result, err := clie2e.RunCmd(ctx, clie2e.Request{
			Args:      []string{"application", "+slash-command-list"},
			DefaultAs: "bot",
		})
		require.NoError(t, err)
		result.AssertExitCode(t, 0)

		item, found := findItemByCommandID(result.Stdout, commandID)
		require.True(t, found, "command %s must still be listed after update, stdout:\n%s", commandID, result.Stdout)
		assert.Equal(t, updatedDesc, item.Get("description.default_value").String(),
			"server state must reflect the update immediately, item:\n%s", item.Raw)
		// icon was not part of the PATCH body → untouched (field-level partial).
		assert.Equal(t, validIconKey, item.Get("icon.icon_key").String(),
			"spec §3: top-level fields not sent in PATCH are preserved, item:\n%s", item.Raw)
	})

	t.Run("create --force on existing name becomes update", func(t *testing.T) {
		requireLiveState(t)
		result, err := clie2e.RunCmd(ctx, clie2e.Request{
			Args: []string{
				"application", "+slash-command-create",
				"--command", name,
				"--description", forcedDesc,
				"--force",
			},
			DefaultAs: "bot",
		})
		require.NoError(t, err)
		guardScope(t, result)
		result.AssertExitCode(t, 0)
		result.AssertStdoutStatus(t, true)

		data := gjson.Get(result.Stdout, "data")
		assert.Equal(t, "updated", data.Get("action").String(),
			"spec §4.2: --force on a name clash must convert to PATCH and report action=updated, stdout:\n%s",
			result.Stdout)
		assert.Equal(t, commandID, data.Get("command_id").String(),
			"--force must update the existing resource (same command_id), not create a new one, stdout:\n%s",
			result.Stdout)
		assert.Equal(t, forcedDesc, data.Get("description.default_value").String(),
			"stdout:\n%s", result.Stdout)
	})

	t.Run("create duplicate without --force fails with hint", func(t *testing.T) {
		requireLiveState(t)
		result, err := clie2e.RunCmd(ctx, clie2e.Request{
			Args: []string{
				"application", "+slash-command-create",
				"--command", name,
				"--description", "e2e duplicate attempt",
			},
			DefaultAs: "bot",
		})
		require.NoError(t, err)
		result.AssertExitCode(t, 1)

		payload := payloadFromResult(t, result)
		assert.False(t, gjson.Get(payload, "ok").Bool(), "payload:\n%s", payload)
		combined := result.Stdout + result.Stderr
		assert.Contains(t, combined, "already exists",
			"spec §3: 40000000 name-clash message must be preserved, stdout:\n%s\nstderr:\n%s",
			result.Stdout, result.Stderr)
		assert.Contains(t, combined, "--force",
			"spec §4.2: the name-clash hint must point at --force, stdout:\n%s\nstderr:\n%s",
			result.Stdout, result.Stderr)
	})

	t.Run("delete with --yes returns deleted action", func(t *testing.T) {
		requireLiveState(t)
		result, err := clie2e.RunCmd(ctx, clie2e.Request{
			Args:      []string{"application", "+slash-command-delete", "--command-id", commandID},
			DefaultAs: "bot",
			Yes:       true,
		})
		require.NoError(t, err)
		guardScope(t, result)
		result.AssertExitCode(t, 0)
		result.AssertStdoutStatus(t, true)
		deleted = result.ExitCode == 0

		// Spec §4.4: upstream DELETE data is {}, the CLI must still return the
		// resource identity so write ops always echo a resource ID.
		data := gjson.Get(result.Stdout, "data")
		assert.Equal(t, "deleted", data.Get("action").String(), "stdout:\n%s", result.Stdout)
		assert.Equal(t, commandID, data.Get("command_id").String(), "stdout:\n%s", result.Stdout)

		assert.Contains(t, result.Stdout+result.Stderr, clientCacheHintFragment,
			"spec §4.0/§4.4: delete must surface the client-cache hint, stderr:\n%s", result.Stderr)
	})

	t.Run("list confirms deletion", func(t *testing.T) {
		requireLiveState(t)
		result, err := clie2e.RunCmd(ctx, clie2e.Request{
			Args:      []string{"application", "+slash-command-list"},
			DefaultAs: "bot",
		})
		require.NoError(t, err)
		result.AssertExitCode(t, 0)

		_, found := findItemByCommandID(result.Stdout, commandID)
		assert.False(t, found,
			"deleted command %s must disappear from list immediately (spec §3), stdout:\n%s",
			commandID, result.Stdout)
	})
}

// TestApplicationSlashCommandDelete_ConfirmationGate proves the high-risk-write
// framework gate (spec §4.4): without --yes the command must exit 10 with a
// confirmation_required envelope and must not reach the server. Fake app
// credentials guarantee no real call is possible, so the placeholder
// command-id never leaves the process (live-probed on an existing
// high-risk-write command: the gate trips before any network activity).
func TestApplicationSlashCommandDelete_ConfirmationGate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)

	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args:      []string{"application", "+slash-command-delete", "--command-id", "e2eplaceholderid"},
		DefaultAs: "bot",
		Env:       fakeCredsEnv(),
	})
	require.NoError(t, err)
	result.AssertExitCode(t, 10)

	payload := payloadFromResult(t, result)
	assert.False(t, gjson.Get(payload, "ok").Bool(), "payload:\n%s", payload)
	assert.Equal(t, "confirmation", gjson.Get(payload, "error.type").String(), "payload:\n%s", payload)
	assert.Equal(t, "confirmation_required", gjson.Get(payload, "error.subtype").String(), "payload:\n%s", payload)
	assert.Equal(t, "high-risk-write", gjson.Get(payload, "error.risk").String(), "payload:\n%s", payload)
	assert.Contains(t, gjson.Get(payload, "error.hint").String(), "--yes", "payload:\n%s", payload)
	assert.Contains(t, gjson.Get(payload, "error.action").String(), "+slash-command-delete", "payload:\n%s", payload)
}

// TestApplicationSlashCommand_DryRunPreviews proves the request construction
// of all four commands without side effects (spec §7 dry-run E2E): method,
// URL, and — critically — the live-verified body shape where icon sits at the
// top level next to description (spec §3: the official create example nesting
// is a documentation bug) and PATCH bodies stay partial.
func TestApplicationSlashCommand_DryRunPreviews(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)

	const placeholderID = "e2edryruncommandid"

	t.Run("list previews GET", func(t *testing.T) {
		result, err := clie2e.RunCmd(ctx, clie2e.Request{
			Args:      []string{"application", "+slash-command-list", "--dry-run"},
			DefaultAs: "bot",
			Env:       fakeCredsEnv(),
		})
		require.NoError(t, err)
		result.AssertExitCode(t, 0)

		preview := dryRunJSON(t, result)
		assert.Equal(t, "GET", gjson.Get(preview, "api.0.method").String(), "preview:\n%s", preview)
		assert.Equal(t, slashCommandsBasePath, gjson.Get(preview, "api.0.url").String(), "preview:\n%s", preview)
	})

	t.Run("create previews POST body with top-level icon", func(t *testing.T) {
		result, err := clie2e.RunCmd(ctx, clie2e.Request{
			Args: []string{
				"application", "+slash-command-create",
				"--command", "e2edryruncmd",
				"--description", "e2e dry-run description",
				"--description-i18n", "zh_cn=e2e中文",
				"--description-i18n", "en_us=e2e english",
				"--icon-key", validIconKey,
				"--dry-run",
			},
			DefaultAs: "bot",
			Env:       fakeCredsEnv(),
		})
		require.NoError(t, err)
		result.AssertExitCode(t, 0)

		preview := dryRunJSON(t, result)
		assert.Equal(t, "POST", gjson.Get(preview, "api.0.method").String(), "preview:\n%s", preview)
		assert.Equal(t, slashCommandsBasePath, gjson.Get(preview, "api.0.url").String(), "preview:\n%s", preview)

		body := gjson.Get(preview, "api.0.body")
		assert.Equal(t, "e2edryruncmd", body.Get("command").String(), "preview:\n%s", preview)
		assert.Equal(t, "e2e dry-run description", body.Get("description.default_value").String(), "preview:\n%s", preview)
		assert.Equal(t, "e2e中文", body.Get("description.i18n.zh_cn").String(),
			"repeatable --description-i18n must map into description.i18n, preview:\n%s", preview)
		assert.Equal(t, "e2e english", body.Get("description.i18n.en_us").String(), "preview:\n%s", preview)
		assert.Equal(t, validIconKey, body.Get("icon.icon_key").String(),
			"spec §3: icon must be a top-level sibling of description, preview:\n%s", preview)
		assert.False(t, body.Get("description.icon").Exists(),
			"icon must NOT be nested under description (official doc example is wrong per spec §3), preview:\n%s", preview)
	})

	t.Run("update previews partial PATCH body", func(t *testing.T) {
		result, err := clie2e.RunCmd(ctx, clie2e.Request{
			Args: []string{
				"application", "+slash-command-update",
				"--command-id", placeholderID,
				"--description", "e2e dry-run new description",
				"--dry-run",
			},
			DefaultAs: "bot",
			Env:       fakeCredsEnv(),
		})
		require.NoError(t, err)
		result.AssertExitCode(t, 0)

		preview := dryRunJSON(t, result)
		assert.Equal(t, "PATCH", gjson.Get(preview, "api.0.method").String(), "preview:\n%s", preview)
		assert.Equal(t, slashCommandsBasePath+"/"+placeholderID,
			gjson.Get(preview, "api.0.url").String(), "preview:\n%s", preview)

		body := gjson.Get(preview, "api.0.body")
		assert.Equal(t, "e2e dry-run new description", body.Get("description.default_value").String(),
			"preview:\n%s", preview)
		assert.False(t, body.Get("icon").Exists(),
			"spec §4.3: PATCH body must only contain user-provided top-level fields, preview:\n%s", preview)
		assert.False(t, body.Get("command").Exists(),
			"command name is a selector, not a PATCHable body field, preview:\n%s", preview)
	})

	t.Run("delete previews DELETE", func(t *testing.T) {
		result, err := clie2e.RunCmd(ctx, clie2e.Request{
			Args: []string{
				"application", "+slash-command-delete",
				"--command-id", placeholderID,
				"--dry-run",
			},
			DefaultAs: "bot",
			Env:       fakeCredsEnv(),
			Yes:       true,
		})
		require.NoError(t, err)
		result.AssertExitCode(t, 0)

		preview := dryRunJSON(t, result)
		assert.Equal(t, "DELETE", gjson.Get(preview, "api.0.method").String(), "preview:\n%s", preview)
		assert.Equal(t, slashCommandsBasePath+"/"+placeholderID,
			gjson.Get(preview, "api.0.url").String(), "preview:\n%s", preview)
	})
}

// TestApplicationSlashCommand_ValidationErrors proves the deterministic local
// Validate rules from spec §5: every violation must exit 2 with a structured
// validation envelope naming the offending flag. Fake credentials guarantee
// the process cannot fall through to a real API call, so a pass can only come
// from local validation (probed: validation runs before any network activity).
func TestApplicationSlashCommand_ValidationErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)

	cases := []struct {
		name string
		args []string
		// wantFragments are asserted individually against the combined
		// output — each one must appear (no OR fallbacks).
		wantFragments []string
	}{
		{
			name: "create rejects command with leading slash",
			args: []string{
				"application", "+slash-command-create",
				"--command", "/e2egreet", "--description", "e2e desc",
			},
			wantFragments: []string{"must not start with"},
		},
		{
			name: "create requires description",
			args: []string{
				"application", "+slash-command-create",
				"--command", "e2egreetnodesc",
			},
			wantFragments: []string{"description"},
		},
		{
			name: "update rejects both command-id and command",
			args: []string{
				"application", "+slash-command-update",
				"--command-id", "e2eplaceholderid", "--command", "e2egreet",
				"--description", "e2e desc",
			},
			wantFragments: []string{"--command-id", "--command"},
		},
		{
			name: "update requires a target selector",
			args: []string{
				"application", "+slash-command-update",
				"--description", "e2e desc",
			},
			wantFragments: []string{"--command-id"},
		},
		{
			name: "update requires at least one mutable field",
			args: []string{
				"application", "+slash-command-update",
				"--command-id", "e2eplaceholderid",
			},
			wantFragments: []string{"at least one"},
		},
		{
			name: "i18n entry must be lang=text",
			args: []string{
				"application", "+slash-command-create",
				"--command", "e2egreetbadi18n", "--description", "e2e desc",
				"--description-i18n", "zh_cn缺等号",
			},
			wantFragments: []string{"--description-i18n"},
		},
		{
			name: "i18n duplicate language rejected",
			args: []string{
				"application", "+slash-command-create",
				"--command", "e2egreetdupi18n", "--description", "e2e desc",
				"--description-i18n", "zh_cn=one",
				"--description-i18n", "zh_cn=two",
			},
			wantFragments: []string{"zh_cn"},
		},
		{
			name: "update i18n requires description",
			args: []string{
				"application", "+slash-command-update",
				"--command-id", "e2eplaceholderid",
				"--description-i18n", "zh_cn=文案",
			},
			wantFragments: []string{"--description"},
		},
		{
			name: "delete rejects both command-id and command",
			args: []string{
				"application", "+slash-command-delete",
				"--command-id", "e2eplaceholderid", "--command", "e2egreet",
				"--yes",
			},
			wantFragments: []string{"--command-id", "--command"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := clie2e.RunCmd(ctx, clie2e.Request{
				Args:      tc.args,
				DefaultAs: "bot",
				Env:       fakeCredsEnv(),
			})
			require.NoError(t, err)
			result.AssertExitCode(t, 2)

			payload := payloadFromResult(t, result)
			assert.False(t, gjson.Get(payload, "ok").Bool(), "payload:\n%s", payload)
			assert.Equal(t, "validation", gjson.Get(payload, "error.type").String(),
				"spec §5: local Validate failures are structured validation errors, payload:\n%s", payload)

			combined := result.Stdout + result.Stderr
			for _, fragment := range tc.wantFragments {
				assert.Contains(t, combined, fragment,
					"spec §5: validation errors must name the offending flag/rule, stdout:\n%s\nstderr:\n%s",
					result.Stdout, result.Stderr)
			}
		})
	}
}

// TestApplicationDomain_HelpSurface proves the help/catalog contract from
// spec §7: the application domain is registered with all four shortcuts, and
// delete advertises its high-risk-write risk level. Deterministic and local.
func TestApplicationDomain_HelpSurface(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)

	t.Run("domain help lists all four shortcuts", func(t *testing.T) {
		result, err := clie2e.RunCmd(ctx, clie2e.Request{
			Args: []string{"application", "--help"},
			Env:  fakeCredsEnv(),
		})
		require.NoError(t, err)
		result.AssertExitCode(t, 0)

		combined := result.Stdout + result.Stderr
		for _, shortcut := range []string{
			"+slash-command-list",
			"+slash-command-create",
			"+slash-command-update",
			"+slash-command-delete",
		} {
			assert.Contains(t, combined, shortcut,
				"domain help must list %s, stdout:\n%s", shortcut, result.Stdout)
		}
	})

	t.Run("delete help shows high-risk-write risk", func(t *testing.T) {
		result, err := clie2e.RunCmd(ctx, clie2e.Request{
			Args: []string{"application", "+slash-command-delete", "-h"},
			Env:  fakeCredsEnv(),
		})
		require.NoError(t, err)
		result.AssertExitCode(t, 0)

		combined := result.Stdout + result.Stderr
		assert.Contains(t, combined, "high-risk-write",
			"spec §7: command help must display the risk level, stdout:\n%s", result.Stdout)
		assert.Contains(t, combined, "--yes",
			"high-risk-write commands must advertise the framework-injected --yes flag, stdout:\n%s", result.Stdout)
	})
}
