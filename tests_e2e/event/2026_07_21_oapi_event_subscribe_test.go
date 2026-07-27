// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	clie2e "github.com/larksuite/cli/tests/cli_e2e"
)

// ---------------------------------------------------------------------------
// PROVABLE (backward compatibility) — pass on today's binary and must stay
// green after the feature lands. These lock the "legacy output is frozen"
// invariant (spec §13.1, §13.9, §13.14) and the status read path (§4.6).
// ---------------------------------------------------------------------------

// TestEventList_LegacyKeyFrozen proves `event list --json` keeps a legacy key's
// shape unchanged and never marks it refined. Additive refined fields (§2.5)
// must not leak onto legacy keys.
func TestEventList_LegacyKeyFrozen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	result, err := clie2e.RunCmd(ctx, clie2e.Request{Args: []string{"event", "list", "--json"}})
	require.NoError(t, err)
	result.AssertExitCode(t, 0)

	entry := findKey(result.Stdout, legacyKey)
	require.True(t, entry.Exists(), "legacy key %q must appear in event list, stdout:\n%s", legacyKey, result.Stdout)

	assert.Equal(t, legacyKey, entry.Get("key").String(), "legacy key identity, stdout:\n%s", result.Stdout)
	assert.Equal(t, legacyKey, entry.Get("event_type").String(), "legacy event_type, stdout:\n%s", result.Stdout)
	assert.Contains(t, entry.Get("auth_types").Value(), "bot", "legacy auth_types frozen, stdout:\n%s", result.Stdout)

	// Refined markers must be absent on a legacy key (§13.1 byte-for-byte freeze).
	assert.False(t, entry.Get("refined_subscription").Exists(),
		"legacy key must NOT carry refined_subscription, stdout:\n%s", result.Stdout)
	assert.False(t, entry.Get("key_templates").Exists(),
		"legacy key must NOT carry key_templates, stdout:\n%s", result.Stdout)
}

// TestEventSchema_LegacyKeyFrozen proves `event schema <legacy> --json` stays
// read-risk with no refined additive fields (§2.6/§2.7 apply to refined keys only).
func TestEventSchema_LegacyKeyFrozen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	result, err := clie2e.RunCmd(ctx, clie2e.Request{Args: []string{"event", "schema", legacyKey, "--json"}})
	require.NoError(t, err)
	result.AssertExitCode(t, 0)

	assert.Equal(t, legacyKey, gjson.Get(result.Stdout, "key").String(), "legacy key identity, stdout:\n%s", result.Stdout)
	assert.Contains(t, gjson.Get(result.Stdout, "auth_types").Value(), "bot", "legacy auth_types, stdout:\n%s", result.Stdout)

	assert.False(t, gjson.Get(result.Stdout, "refined_subscription").Exists(),
		"legacy schema must NOT carry refined_subscription, stdout:\n%s", result.Stdout)
	assert.NotEqual(t, "write", gjson.Get(result.Stdout, "risk.effective").String(),
		"legacy schema must NOT disclose write-effective risk, stdout:\n%s", result.Stdout)
}

// TestEventConsume_LegacyKeyWithSuffixRejected proves a legacy key with a path
// suffix is never treated as a refined key: it is a deterministic
// validation/invalid_argument (§2.3 step 2, §13.14). The type/subtype/exit
// contract holds on today's binary (unknown key) and after the resolver lands
// (exact-match-only legacy lookup rejects the suffix).
func TestEventConsume_LegacyKeyWithSuffixRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args: []string{"event", "consume", legacyKey + "/foo/bar", "--max-events", "1", "--timeout", "8s"},
	})
	require.NoError(t, err)

	assertTypedError(t, result, 2, "validation", "invalid_argument")
	assert.Empty(t, result.Stdout, "a rejected key must not stream any NDJSON event, stdout:\n%s", result.Stdout)
}

// TestEventStatus_BackwardCompat proves `event status --json` stays a read-only
// command returning the apps summary even with no running bus (§4.6). Refined
// additive fields only appear with a live refined consumer (covered live).
func TestEventStatus_BackwardCompat(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	result, err := clie2e.RunCmd(ctx, clie2e.Request{Args: []string{"event", "status", "--json", "--current"}})
	require.NoError(t, err)
	result.AssertExitCode(t, 0)

	apps := gjson.Get(result.Stdout, "apps")
	require.True(t, apps.IsArray(), "status must return an apps array, stdout:\n%s", result.Stdout)
	require.NotEmpty(t, apps.Array(), "current profile app must be listed, stdout:\n%s", result.Stdout)
	assert.NotEmpty(t, apps.Array()[0].Get("app_id").String(), "app entry must carry app_id, stdout:\n%s", result.Stdout)
	assert.True(t, apps.Array()[0].Get("running").Exists(), "app entry must carry running flag, stdout:\n%s", result.Stdout)
}

// ---------------------------------------------------------------------------
// EXPECTED-RED — assertion chains are correct; they FAIL on today's binary
// because the feature is unimplemented, and go green after Phase 4. All are
// pure local (parse / discovery / phase-gate); none needs the remote endpoint,
// so they are reliable regardless of endpoint GA status.
// ---------------------------------------------------------------------------

// TestEventList_RefinedKeyAdditive proves the mock catalog exposes the refined
// base key with additive discovery metadata (§2.5, §13.1).
func TestEventList_RefinedKeyAdditive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	result, err := clie2e.RunCmd(ctx, clie2e.Request{Args: []string{"event", "list", "--json"}})
	require.NoError(t, err)
	result.AssertExitCode(t, 0)

	entry := findKey(result.Stdout, refinedBaseKey)
	require.True(t, entry.Exists(),
		"refined base key %q must appear in event list (mock catalog, spec §2.2), stdout:\n%s", refinedBaseKey, result.Stdout)

	assert.True(t, entry.Get("refined_subscription").Bool(),
		"refined key must set refined_subscription=true, stdout:\n%s", result.Stdout)
	assert.Equal(t, "im.message", entry.Get("resource_type").String(),
		"refined key resource_type, stdout:\n%s", result.Stdout)

	templates := entry.Get("key_templates")
	require.True(t, templates.IsArray() && len(templates.Array()) >= 1,
		"refined key must expose key_templates, stdout:\n%s", result.Stdout)
	first := templates.Array()[0]
	assert.NotEmpty(t, first.Get("template").String(), "template must be non-empty, stdout:\n%s", result.Stdout)
	assert.NotEmpty(t, first.Get("selector_key").String(), "selector_key must be non-empty, stdout:\n%s", result.Stdout)
	assert.NotEmpty(t, first.Get("path_segment").String(), "path_segment must be non-empty, stdout:\n%s", result.Stdout)
}

// TestEventSchema_RefinedKeyAdditive proves `event schema <refined> --json`
// discloses write-effective risk, dry-run recommendation, conditional scopes
// and templates (§2.6, §2.7, §13.2).
func TestEventSchema_RefinedKeyAdditive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	result, err := clie2e.RunCmd(ctx, clie2e.Request{Args: []string{"event", "schema", refinedBaseKey, "--json"}})
	require.NoError(t, err)
	result.AssertExitCode(t, 0) // RED today: unknown key -> exit 2.

	assert.True(t, gjson.Get(result.Stdout, "refined_subscription").Bool(),
		"schema must set refined_subscription=true, stdout:\n%s", result.Stdout)
	assert.Equal(t, "write", gjson.Get(result.Stdout, "risk.effective").String(),
		"schema must disclose risk.effective=write, stdout:\n%s", result.Stdout)
	assert.True(t, gjson.Get(result.Stdout, "risk.dry_run_recommended").Bool(),
		"schema must recommend dry-run, stdout:\n%s", result.Stdout)
	assert.True(t, gjson.Get(result.Stdout, "conditional_scopes").Exists(),
		"schema must disclose conditional_scopes (deferred encrypt_key), stdout:\n%s", result.Stdout)
	assert.True(t, gjson.Get(result.Stdout, "key_templates").IsArray(),
		"schema must expose key_templates, stdout:\n%s", result.Stdout)
}

// TestEventConsume_BareRefinedBaseKeyRejected proves R1 (spec §2.3, §13.11):
// a bare refined base key cannot be consumed; it must be a typed
// invalid_argument naming the offending EventKey in error.param.
func TestEventConsume_BareRefinedBaseKeyRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args:      []string{"event", "consume", refinedBaseKey, "--max-events", "1", "--timeout", "8s"},
		DefaultAs: "bot",
	})
	require.NoError(t, err)

	payload := assertTypedError(t, result, 2, "validation", "invalid_argument")
	// RED today: the generic "unknown EventKey" error carries no param; the R1
	// resolver must name the offending key.
	assert.Equal(t, refinedBaseKey, gjson.Get(payload, "error.param").String(),
		"R1 rejection must set error.param to the bare base key, stderr:\n%s", result.Stderr)
	assert.Empty(t, result.Stdout, "a rejected key must not stream any event, stdout:\n%s", result.Stdout)
}

// TestEventConsume_IncludeResourceDataOnLegacyRejected proves a legacy key with
// --include-resource-data is a typed invalid_argument, not a silent no-op
// (spec §4.7 "无声降级即 bug").
func TestEventConsume_IncludeResourceDataOnLegacyRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args:      []string{"event", "consume", legacyKey, "--include-resource-data", "--max-events", "1", "--timeout", "8s"},
		DefaultAs: "bot",
	})
	require.NoError(t, err)

	// RED today: --include-resource-data is an unknown flag (type=unknown_flag).
	assertTypedError(t, result, 2, "validation", "invalid_argument")
}

// TestEventConsume_IncludeResourceDataEGate proves the deferred-encryption gate
// (module E, spec §9, §13.7): a refined consume asking for resource data is a
// typed failed_precondition. --dry-run keeps the path side-effect free; the
// gate fires before any remote call.
func TestEventConsume_IncludeResourceDataEGate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args: []string{
			"event", "consume", refinedChatKeyPlaceholder,
			"--include-resource-data", "--dry-run", "--max-events", "1", "--timeout", "8s",
		},
		DefaultAs: "bot",
	})
	require.NoError(t, err)

	// RED today: unknown flags. After Phase 4: E-gate -> failed_precondition.
	assertTypedError(t, result, 2, "validation", "failed_precondition")
}

// TestEventConsume_OwnerTemplateIdentityGate proves the template identity gate
// (spec §2.8): owner/me is user-only, so resolving to bot is a typed
// failed_precondition — never a silent identity swap.
func TestEventConsume_OwnerTemplateIdentityGate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args:      []string{"event", "consume", ownerMePath, "--max-events", "1", "--timeout", "8s"},
		DefaultAs: "bot",
	})
	require.NoError(t, err)

	// RED today: created_v1 is unknown -> invalid_argument. After Phase 4:
	// key resolves, owner/me template rejects bot -> failed_precondition.
	assertTypedError(t, result, 2, "validation", "failed_precondition")
	assert.Empty(t, result.Stdout, "a gated consume must not stream any event, stdout:\n%s", result.Stdout)
}

// TestEventSubscriptionCreate_BareRefinedBaseKeyRejected proves R1 on the
// management face (spec §2.3, §11.5, §13.11): create with a bare base key is a
// typed invalid_argument naming the key, with no remote call (--dry-run).
func TestEventSubscriptionCreate_BareRefinedBaseKeyRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args:      []string{"event", "subscription", "create", refinedBaseKey, "--dry-run"},
		DefaultAs: "bot",
	})
	require.NoError(t, err)

	// RED today: `subscription` is an unknown_subcommand (no subtype/param).
	payload := assertTypedError(t, result, 2, "validation", "invalid_argument")
	assert.Equal(t, refinedBaseKey, gjson.Get(payload, "error.param").String(),
		"R1 rejection must set error.param to the bare base key, stderr:\n%s", result.Stderr)
}

// TestEventSubscriptionCreate_IncludeResourceDataEGate proves the deferred-
// encryption gate on the management face (spec §9, §13.7): create with
// --include-resource-data is a typed failed_precondition. --dry-run keeps it
// side-effect free; the gate fires before any remote write.
func TestEventSubscriptionCreate_IncludeResourceDataEGate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	result, err := clie2e.RunCmd(ctx, clie2e.Request{
		Args: []string{
			"event", "subscription", "create", refinedChatKeyPlaceholder,
			"--include-resource-data", "--dry-run",
		},
		DefaultAs: "bot",
	})
	require.NoError(t, err)

	// RED today: unknown_subcommand. After Phase 4: E-gate -> failed_precondition.
	assertTypedError(t, result, 2, "validation", "failed_precondition")
}

// ---------------------------------------------------------------------------
// BLOCKED (guarded live) — the management-face round-trip depends on the
// unreleased event-SDK and the non-GA event/v1 endpoint (spec §0.3). Opt in
// with LARK_CLI_E2E_EVENT_LIVE=1 in a sandbox that has both plus the
// event:subscription:read+write scopes. The chat is self-constructed; the
// subscription is created and torn down by this test.
// ---------------------------------------------------------------------------

// TestEventSubscriptionWorkflow_Live exercises create -> get (read-after-write)
// -> update confirmation gate -> update (--yes) -> renew -> list, with delete as
// cleanup. Covers spec §3 (management CRUD), §3.7 (update needs exit-10
// confirmation), §13.13 (confirmation contract).
func TestEventSubscriptionWorkflow_Live(t *testing.T) {
	if os.Getenv("LARK_CLI_E2E_EVENT_LIVE") == "" {
		t.Skip("FIXTURE: set LARK_CLI_E2E_EVENT_LIVE=1 to run the live subscription workflow " +
			"(requires the released event-SDK, a GA event/v1 endpoint, and event:subscription:read+write scopes)")
	}

	parentT := t
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	suffix := clie2e.GenerateSuffix()

	chatID := createChat(t, parentT, ctx, "lark-cli-e2e-event-"+suffix)
	refinedKey := refinedBaseKey + chatIDTemplateSeg + chatID

	var subID string
	t.Run("create refined subscription", func(t *testing.T) {
		result, err := clie2e.RunCmd(ctx, clie2e.Request{
			Args:      []string{"event", "subscription", "create", refinedKey, "--json"},
			DefaultAs: "bot",
		})
		require.NoError(t, err)
		result.AssertExitCode(t, 0)
		result.AssertStdoutStatus(t, true)

		subID = gjson.Get(result.Stdout, "data.remote_subscription_id").String()
		require.NotEmpty(t, subID, "create must return data.remote_subscription_id (spec §3.3), stdout:\n%s", result.Stdout)
		parentT.Cleanup(func() { deleteSubscription(parentT, subID) })
	})
	require.NotEmpty(t, subID, "cannot continue workflow without a subscription id")

	t.Run("get reflects the created subscription", func(t *testing.T) {
		result, err := clie2e.RunCmdWithRetry(ctx, clie2e.Request{
			Args:      []string{"event", "subscription", "get", subID, "--json"},
			DefaultAs: "bot",
		}, clie2e.RetryOptions{
			ShouldRetry: func(r *clie2e.Result) bool { return r == nil || r.ExitCode != 0 },
		})
		require.NoError(t, err)
		result.AssertExitCode(t, 0)
		assert.Equal(t, subID, gjson.Get(result.Stdout, "data.remote_subscription_id").String(),
			"get must return the same subscription id, stdout:\n%s", result.Stdout)
	})

	t.Run("update without --yes requires confirmation", func(t *testing.T) {
		result, err := clie2e.RunCmd(ctx, clie2e.Request{
			Args:      []string{"event", "subscription", "update", subID, "--include-resource-data=false"},
			DefaultAs: "bot",
		})
		require.NoError(t, err)
		// High-risk-write confirmation: category confirmation, exit 10 (§3.7, §13.13).
		assertTypedError(t, result, 10, "confirmation", "")
	})

	t.Run("update with --yes proceeds", func(t *testing.T) {
		result, err := clie2e.RunCmd(ctx, clie2e.Request{
			Args:      []string{"event", "subscription", "update", subID, "--include-resource-data=false", "--json"},
			DefaultAs: "bot",
			Yes:       true,
		})
		require.NoError(t, err)
		result.AssertExitCode(t, 0)
		result.AssertStdoutStatus(t, true)
	})

	t.Run("renew extends the subscription without confirmation", func(t *testing.T) {
		result, err := clie2e.RunCmd(ctx, clie2e.Request{
			Args:      []string{"event", "subscription", "renew", subID, "--json"},
			DefaultAs: "bot",
		})
		require.NoError(t, err)
		result.AssertExitCode(t, 0)
		result.AssertStdoutStatus(t, true)
	})

	t.Run("list includes the created subscription", func(t *testing.T) {
		result, err := clie2e.RunCmd(ctx, clie2e.Request{
			Args:      []string{"event", "subscription", "list", "--json"},
			DefaultAs: "bot",
		})
		require.NoError(t, err)
		result.AssertExitCode(t, 0)

		found := false
		gjson.Get(result.Stdout, "data.items").ForEach(func(_, item gjson.Result) bool {
			if item.Get("remote_subscription_id").String() == subID {
				found = true
				return false
			}
			return true
		})
		assert.True(t, found, "list must include the created subscription %s, stdout:\n%s", subID, result.Stdout)
	})
}
