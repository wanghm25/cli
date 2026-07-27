// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

// Package event contains black-box E2E tests for the `event` domain's
// refined (dynamic) subscription feature.
//
// Spec: docs/superpowers/specs/2026-07-21-oapi-event-subscribe-design.md
//
// Truth-source split this file encodes (spec §0.3):
//   - Phase A (discovery/parse layer) is SDK-agnostic and hits no remote:
//     `event list --json` / `event schema --json` additive fields, the R1
//     bare-refined-base-key rejection, legacy-key freeze, the include-resource
//     -data E-gate (§9), and the template identity gate (§2.8) are all typed
//     errors or additive JSON that a locally-built binary can prove NOW.
//   - Phase B/C/D (management face / runtime / lifecycle) depend on the
//     candidate event-SDK (blkb12/oapi-sdk-go@feat/event_sdk, unreleased) and
//     the open-platform event/v1 endpoint (GA unknown). Real round-trips are
//     best-effort and are guarded behind LARK_CLI_E2E_EVENT_LIVE.
//
// Error-contract note (errs/ERROR_CONTRACT.md, AGENTS.md): typed errors render
// as a JSON envelope on stderr; we branch only on the wire-stable
// error.type / error.subtype / error.param / error.missing_scopes, never on
// error.message. Confirmation uses exit 10 with error.type=="confirmation".
package event

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	clie2e "github.com/larksuite/cli/tests/cli_e2e"
)

const (
	// refinedBaseKey is the sole entry in the mock refined catalog (spec §2.2).
	refinedBaseKey = "im.message.created_v1"

	// legacyKey is an existing (frozen) EventKey; bot-only, exact-match only.
	legacyKey = "im.message.receive_v1"

	// chatIDTemplatePath / ownerMePath are the two mock key templates (spec §2.2):
	// chat-id supports {user,bot}; owner/me is a fixed value, user-only.
	chatIDTemplateSeg = "/chat-id/"
	ownerMePath       = refinedBaseKey + "/owner/me"

	// placeholderChatID is a syntactically-shaped but non-existent chat id used
	// ONLY in local gate/parse tests (R1, E-gate) whose typed rejection fires
	// during ResolveEventKey / the phase gate — before any remote call. It is
	// never used to assert remote behavior, so it does not fake a remote
	// resource (hard-gate #7). Live tests self-construct a real chat instead.
	placeholderChatID = "oc_00000000000000000000000000000000"

	// refinedChatKeyPlaceholder is a materialized refined key over the
	// placeholder chat, for parse/gate-only assertions.
	refinedChatKeyPlaceholder = refinedBaseKey + chatIDTemplateSeg + placeholderChatID
)

// errPayload returns the typed error JSON envelope. Typed errors render to
// stderr (verified on the live CLI); we fall back to stdout defensively.
func errPayload(t *testing.T, result *clie2e.Result) string {
	t.Helper()
	if p := jsonObject(result.Stderr); p != "" {
		return p
	}
	if p := jsonObject(result.Stdout); p != "" {
		return p
	}
	require.FailNow(t, "no JSON error envelope found",
		"stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr)
	return ""
}

func jsonObject(raw string) string {
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

// assertTypedError asserts the wire-stable exit code + error.type (+ optional
// subtype). Returns the parsed error envelope so callers can add field-level
// assertions (param, missing_scopes). Never asserts on error.message.
func assertTypedError(t *testing.T, result *clie2e.Result, wantExit int, wantType, wantSubtype string) string {
	t.Helper()
	result.AssertExitCode(t, wantExit)
	payload := errPayload(t, result)

	assert.Equal(t, wantType, gjson.Get(payload, "error.type").String(),
		"error.type mismatch\nstdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr)
	if wantSubtype != "" {
		assert.Equal(t, wantSubtype, gjson.Get(payload, "error.subtype").String(),
			"error.subtype mismatch\nstdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr)
	}
	return payload
}

// findKey returns the catalog entry for key from an `event list --json` array.
func findKey(stdout, key string) gjson.Result {
	return gjson.Get(stdout, `#(key=="`+key+`")`)
}

// createChat self-constructs a real group chat with the sandbox bot identity
// and returns its chat_id (spec §10.4: chat is bot-constructable). Used only by
// the guarded live workflow. There is no chat-disband CLI command today, so the
// registered cleanup logs the id for the disposable sandbox to reclaim rather
// than leaving a spurious cleanup failure.
func createChat(t *testing.T, parentT *testing.T, ctx context.Context, name string) string {
	t.Helper()
	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args:      []string{"im", "+chat-create", "--name", name, "--chat-mode", "group"},
		DefaultAs: "bot",
	})
	require.NoError(t, err)
	result.AssertExitCode(t, 0)
	result.AssertStdoutStatus(t, true)

	chatID := gjson.Get(result.Stdout, "data.chat_id").String()
	require.NotEmpty(t, chatID, "chat create should return data.chat_id, stdout:\n%s", result.Stdout)

	parentT.Cleanup(func() {
		parentT.Logf("chat %s left for disposable-sandbox teardown (no chat-disband CLI command)", chatID)
	})
	return chatID
}

// deleteSubscription is the cleanup for a live-created subscription: delete is
// high-risk (needs --yes) and this feature's only owned remote resource, so it
// must be reclaimed. not_found is auto-suppressed by ReportCleanupFailure.
func deleteSubscription(parentT *testing.T, subID string) {
	parentT.Helper()
	cleanupCtx, cancel := clie2e.CleanupContext()
	defer cancel()
	result, err := clie2e.RunCmd(cleanupCtx, clie2e.Request{
		Args:      []string{"event", "subscription", "delete", subID},
		DefaultAs: "bot",
		Yes:       true,
	})
	clie2e.ReportCleanupFailure(parentT, "delete event subscription "+subID, result, err)
}
