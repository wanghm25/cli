// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

// Package application contains black-box E2E tests for the `application`
// shortcut domain (slash command management for the currently bound app).
//
// Spec: docs/superpowers/specs/2026-07-07-application-slash-command-design.md
package application

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	clie2e "github.com/larksuite/cli/tests/cli_e2e"
)

const (
	// slashCommandsBasePath is the raw OpenAPI path behind every command (spec §3).
	slashCommandsBasePath = "/open-apis/application/v7/app_slash_commands"

	// validIconKey is the server-side default icon key (spec §3, live-verified),
	// so it is guaranteed to be inside the allowed icon_key list.
	validIconKey = "skill_outlined"

	// clientCacheHintFragment is quoted from the mandatory post-write hint in
	// spec §4.0: "note: changes take ~5 minutes to appear in Feishu clients ...".
	// The hint may surface on stderr or as an envelope warning, so callers
	// assert against the combined process output.
	clientCacheHintFragment = "changes take ~5 minutes"
)

// fakeCredsEnv guarantees the child process cannot make an authenticated real
// API call. Used only for local-deterministic scenarios (validation errors,
// --dry-run previews, the high-risk confirmation gate), matching the dry-run
// E2E setup prescribed by spec §7.
func fakeCredsEnv() map[string]string {
	return map[string]string{
		"LARKSUITE_CLI_APP_ID":     "cli_e2e_fake_app_id",
		"LARKSUITE_CLI_APP_SECRET": "e2e-fake-app-secret",
	}
}

// payloadFromResult extracts the structured JSON envelope from a result.
// Success envelopes print to stdout; error envelopes (validation /
// confirmation / api) print to stderr (probed on the live CLI).
func payloadFromResult(t *testing.T, result *clie2e.Result) string {
	t.Helper()
	if payload := extractJSONObject(result.Stdout); payload != "" {
		return payload
	}
	if payload := extractJSONObject(result.Stderr); payload != "" {
		return payload
	}
	require.FailNow(t, "no JSON envelope found in stdout or stderr",
		"stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr)
	return ""
}

func extractJSONObject(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if gjson.Valid(raw) {
		return raw
	}
	start := strings.Index(raw, "{")
	if start < 0 {
		return ""
	}
	payload := raw[start:]
	if !gjson.Valid(payload) {
		return ""
	}
	return payload
}

// dryRunJSON extracts the JSON preview that follows the "=== Dry Run ==="
// banner (live-probed envelope: {"api":[{"method","url","params","body"}]}).
func dryRunJSON(t *testing.T, result *clie2e.Result) string {
	t.Helper()
	require.Contains(t, result.Stderr, "Dry Run",
		"expected dry-run banner on stderr, stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr)
	payload := extractJSONObject(result.Stdout)
	require.NotEmpty(t, payload,
		"dry-run output must contain a JSON preview, stdout:\n%s", result.Stdout)
	return payload
}

// isScopePermissionError reports whether a failed result is unambiguously a
// permission/scope error. Spec §8 R1: the sandbox test app may not be inside
// the application:app_slash_command scope gray-release, and live scenarios
// must skip with a reason instead of producing a misleading FAIL. The match is
// deliberately narrow so every other failure (unknown command, duplicate name,
// bad flags, server errors) still fails the test loudly.
func isScopePermissionError(result *clie2e.Result) (bool, string) {
	if result == nil || result.ExitCode == 0 {
		return false, ""
	}
	payload := extractJSONObject(result.Stdout)
	if payload == "" {
		payload = extractJSONObject(result.Stderr)
	}
	if payload == "" {
		return false, ""
	}
	errType := strings.ToLower(gjson.Get(payload, "error.type").String())
	message := strings.ToLower(gjson.Get(payload, "error.message").String())
	code := gjson.Get(payload, "error.code").Int()

	if code == 99991672 { // token acquired but tenant/app lacks the required scope
		return true, "api code 99991672 (missing scope): " + message
	}
	if strings.Contains(errType, "permission") {
		return true, "error.type=" + errType + ": " + message
	}
	if strings.Contains(message, "permission") || strings.Contains(message, "scope") {
		return true, "permission/scope error: " + message
	}
	return false, ""
}

// findItemByCommandID scans the list envelope's data.items for command_id.
func findItemByCommandID(stdout, commandID string) (gjson.Result, bool) {
	var found gjson.Result
	ok := false
	gjson.Get(stdout, "data.items").ForEach(func(_, item gjson.Result) bool {
		if item.Get("command_id").String() == commandID {
			found = item
			ok = true
			return false
		}
		return true
	})
	return found, ok
}

// registerSlashCommandCleanup deletes the slash command at teardown unless the
// test body already deleted it. Cleanup-only execution never counts as
// coverage; the positive delete proof lives in the workflow test itself.
func registerSlashCommandCleanup(parentT *testing.T, commandID string, deleted *bool) {
	parentT.Cleanup(func() {
		if *deleted {
			return
		}
		ctx, cancel := clie2e.CleanupContext()
		defer cancel()
		result, err := clie2e.RunCmd(ctx, clie2e.Request{
			Args:      []string{"application", "+slash-command-delete", "--command-id", commandID},
			DefaultAs: "bot",
			Yes:       true,
		})
		clie2e.ReportCleanupFailure(parentT, "cleanup slash command "+commandID, result, err)
	})
}
