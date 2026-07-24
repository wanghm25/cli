// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/credential"

	_ "github.com/larksuite/cli/events" // registers the real catalog: im.message.created_v1 (refined base) + im.message.receive_v1 (legacy)
)

func TestParseParams(t *testing.T) {
	cases := []struct {
		name       string
		in         []string
		want       map[string]string
		wantSentry error
		wantEcho   string
	}{
		{
			name: "empty input",
			in:   nil,
			want: map[string]string{},
		},
		{
			name: "single key=value",
			in:   []string{"mailbox=user@example.com"},
			want: map[string]string{"mailbox": "user@example.com"},
		},
		{
			name: "multiple pairs",
			in:   []string{"a=1", "b=2", "c=3"},
			want: map[string]string{"a": "1", "b": "2", "c": "3"},
		},
		{
			name: "value containing = is kept intact",
			in:   []string{"filter=foo=bar"},
			want: map[string]string{"filter": "foo=bar"},
		},
		{
			name: "empty value allowed",
			in:   []string{"key="},
			want: map[string]string{"key": ""},
		},
		{
			name: "duplicate key — last wins",
			in:   []string{"k=1", "k=2"},
			want: map[string]string{"k": "2"},
		},
		{
			name:       "missing = separator",
			in:         []string{"mailbox"},
			wantSentry: errInvalidParamFormat,
			wantEcho:   `"mailbox"`,
		},
		{
			name:       "leading = (empty key)",
			in:         []string{"=value"},
			wantSentry: errInvalidParamFormat,
			wantEcho:   `"=value"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseParams(tc.in)
			if tc.wantSentry != nil {
				if err == nil {
					t.Fatalf("want error wrapping %v, got nil", tc.wantSentry)
				}
				if !errors.Is(err, tc.wantSentry) {
					t.Fatalf("want errors.Is(err, %v), got %q", tc.wantSentry, err.Error())
				}
				if tc.wantEcho != "" && !strings.Contains(err.Error(), tc.wantEcho) {
					t.Errorf("err %q should echo %q so user sees the bad input", err.Error(), tc.wantEcho)
				}
				assertInvalidArgumentParam(t, err, "--param")
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("len = %d, want %d; got=%v", len(got), len(tc.want), got)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("key %q: got %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

// emptyTokenResolver resolves to a result that carries no token.
type emptyTokenResolver struct{}

func (emptyTokenResolver) ResolveToken(_ context.Context, _ credential.TokenSpec) (*credential.TokenResult, error) {
	return &credential.TokenResult{}, nil
}

// failingTokenResolver fails outright with an untyped error.
type failingTokenResolver struct{}

func (failingTokenResolver) ResolveToken(_ context.Context, _ credential.TokenSpec) (*credential.TokenResult, error) {
	return nil, errors.New("backend unavailable")
}

func factoryWithResolver(r credential.DefaultTokenResolver) *cmdutil.Factory {
	return &cmdutil.Factory{Credential: credential.NewCredentialProvider(nil, nil, r, nil)}
}

func TestResolveTenantToken_EmptyTokenResult(t *testing.T) {
	_, err := resolveTenantToken(context.Background(), factoryWithResolver(emptyTokenResolver{}), "cli_x")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	p, ok := errs.ProblemOf(err)
	if !ok {
		t.Fatalf("expected typed errs error, got %T: %v", err, err)
	}
	if p.Category != errs.CategoryAuthentication || p.Subtype != errs.SubtypeTokenMissing {
		t.Errorf("problem = %s/%s, want %s/%s", p.Category, p.Subtype,
			errs.CategoryAuthentication, errs.SubtypeTokenMissing)
	}
	var malformed *credential.MalformedTokenResultError
	if !errors.As(err, &malformed) {
		t.Error("empty-token failure should preserve the credential-layer cause")
	}
}

func TestResolveTenantToken_ResolverFailure(t *testing.T) {
	_, err := resolveTenantToken(context.Background(), factoryWithResolver(failingTokenResolver{}), "cli_x")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	p, ok := errs.ProblemOf(err)
	if !ok {
		t.Fatalf("expected typed errs error, got %T: %v", err, err)
	}
	if p.Category != errs.CategoryAuthentication || p.Subtype != errs.SubtypeTokenMissing {
		t.Errorf("problem = %s/%s, want %s/%s", p.Category, p.Subtype,
			errs.CategoryAuthentication, errs.SubtypeTokenMissing)
	}
	if errors.Unwrap(err) == nil {
		t.Error("resolver failure should preserve its cause")
	}
}

// assertInvalidArgumentParam verifies err is a typed validation error with
// subtype invalid_argument naming the given flag in its param field. Returns
// the typed error so callers can additionally inspect Message/Hint.
func assertInvalidArgumentParam(t *testing.T, err error, param string) *errs.ValidationError {
	t.Helper()
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeInvalidArgument {
		t.Errorf("subtype = %s, want %s", ve.Subtype, errs.SubtypeInvalidArgument)
	}
	if ve.Param != param {
		t.Errorf("param = %q, want %q", ve.Param, param)
	}
	return ve
}

// newRefinedConsumeTestFactory builds a Factory for exercising the full
// runConsume entry (via NewCmdConsume + cmd.Execute()) against the REAL
// global EventKey registry (im.message.created_v1 refined base +
// im.message.receive_v1 legacy — both registered by the blank import above,
// from events/refined/refined_keys_mock.json and events/im/register.go).
//
// It also plants the same safety net as
// TestBusCommandLoggerSetupFailureIsTypedFileIO (cmd/event/bus_test.go):
// LARKSUITE_CLI_CONFIG_DIR points at a temp dir whose "events" entry is a
// regular file, not a directory. This is defense in depth, not a workaround
// for the behavior under test: if the consume-entry gate ever regresses and
// lets a call fall through toward the real bus (EnsureBus -> forkBus),
// forkBus's vfs.MkdirAll fails deterministically on that blocked path
// BEFORE forkBus ever reaches os.Executable()/exec.Command — so a
// regression surfaces here as a typed internal error inside this
// in-process test, never as a forked child process. (Under `go test`,
// os.Executable() resolves to the compiled test binary, not lark-cli; the
// blocked path guarantees runConsume can never reach that call.)
func newRefinedConsumeTestFactory(t *testing.T) *cmdutil.Factory {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("LARKSUITE_CLI_CONFIG_DIR", dir)
	if err := os.WriteFile(filepath.Join(dir, "events"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}

	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{
		AppID: "cli_consume_test", AppSecret: "secret", Brand: core.BrandFeishu,
	})
	return f
}

// newStrictBotOnlyRefinedConsumeTestFactory mirrors newRefinedConsumeTestFactory
// but additionally configures a bot-only strict-mode account
// (SupportedIdentities:2, bitflag 1=user/2=bot — internal/core/config.go) —
// the same setup cmd/event/subscription/subscription_test.go's
// TestResolveEffectiveIdentity_StrictModeRejectsCrossIdentity uses to lock
// f.CheckStrictMode's rejection of a cross-identity --as for the sibling
// `event subscription` write commands. Reused here to lock the equivalent
// fix on runRefinedConsume's own write path (review finding I1).
func newStrictBotOnlyRefinedConsumeTestFactory(t *testing.T) *cmdutil.Factory {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("LARKSUITE_CLI_CONFIG_DIR", dir)
	if err := os.WriteFile(filepath.Join(dir, "events"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}

	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{
		AppID: "cli_consume_test", AppSecret: "secret", Brand: core.BrandFeishu,
		SupportedIdentities: 2, // bot only
	})
	return f
}

// newConsumeCmd wires cmd.Execute()'s own error/usage banner to io.Discard —
// the returned error (what every test below asserts on) is unaffected;
// this only keeps `go test -v` output free of cobra's usage dump for
// expected-error cases.
func newConsumeCmd(f *cmdutil.Factory, args ...string) *cobra.Command {
	cmd := NewCmdConsume(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs(args)
	return cmd
}

// TestRunConsume_BareRefinedBaseKeyRejected locks bare-refined-base-key rejection at the
// consume entry: a refined-subscription base key with no template segment
// (im.message.created_v1) must be rejected — typed invalid_argument, hint
// pointing at `event schema` — before any identity resolution or bus
// activity, never silently treated as an ordinary consumable key.
func TestRunConsume_BareRefinedBaseKeyRejected(t *testing.T) {
	f := newRefinedConsumeTestFactory(t)
	err := newConsumeCmd(f, "im.message.created_v1").Execute()

	ve := assertInvalidArgumentParam(t, err, "event_key")
	if !strings.Contains(ve.Hint, "event schema") {
		t.Errorf("Hint = %q, want it to point at `event schema`", ve.Hint)
	}
}

// TestRunConsume_RefinedMaterializedKey_DrivesRealRefinedChain_FailsSafelyOffline
// is the counterpart to the removed
// TestRunConsume_RefinedMaterializedKey_RuntimeNotYetAvailable: a fully
// materialized refined EventKey is no longer rejected by a
// hardcoded "not yet available" stub — the fork seam now routes it into the
// real consume.RunRefined chain (ProbeBusEligibility -> PlanRemoteSubscription
// -> ...; internal/event/consume/refined_test.go covers that chain's own
// ordering/dry-run/error-contract behavior directly, with injected fakes).
//
// This Factory (newRefinedConsumeTestFactory) registers no httpmock stubs at
// all, so the chain's first REAL network call — PlanRemoteSubscription's
// List, reached only after identity resolution and
// apiClient/SubscriptionClient construction succeeded and
// ProbeBusEligibility passed — fails deterministically and offline with a
// typed error. That failure is the useful assertion here: it proves the
// fork seam reaches all the way into the real chain (not a stub) while
// never forking a bus or reaching Apply (the only remote write) — both
// later stages than Plan, never reached in this run.
func TestRunConsume_RefinedMaterializedKey_DrivesRealRefinedChain_FailsSafelyOffline(t *testing.T) {
	f := newRefinedConsumeTestFactory(t)
	err := newConsumeCmd(f, "im.message.created_v1/chat-id/oc_9f3b1c2d8a").Execute()

	if err == nil {
		t.Fatal("expected an error (this test's Factory registers no HTTP stubs), got nil")
	}
	if _, ok := errs.ProblemOf(err); !ok {
		t.Fatalf("expected a typed errs.* error even from a deep chain failure, got %T: %v", err, err)
	}
	// Reached PlanRemoteSubscription's real List call — proof the fork seam
	// no longer returns a hardcoded stub error, and proof it failed before
	// ever reaching a write (Apply) or forking a bus (StartOrConnectBus).
	if !strings.Contains(err.Error(), "/open-apis/event/v1/subscriptions") {
		t.Errorf("expected the failure to come from PlanRemoteSubscription's List call, got: %v", err)
	}
}

// TestRunConsume_OwnerMeTemplate_AsBot_RejectedByTemplateAuthTypesBeforePlanApply
// is the REQUIRED write-safety case: the shipped
// catalog's im.message.created_v1/owner/me template declares
// auth_types:["user"] (events/refined/refined_keys_mock.json) -- narrower
// than the base key's ["user","bot"] -- so `--as bot` must be rejected with
// the typed error BEFORE the refined chain ever reaches
// PlanRemoteSubscription/ApplyRemoteSubscriptionPlan. This Factory
// (newRefinedConsumeTestFactory) registers zero HTTP stubs, so if the
// rejection did NOT happen up front and the chain instead reached Plan's
// real List call, the error would mention "/open-apis/event/v1/subscriptions"
// (exactly as
// TestRunConsume_RefinedMaterializedKey_DrivesRealRefinedChain_FailsSafelyOffline
// demonstrates for the chat-id template) -- proving the apply seam was never
// reached is therefore equivalent to proving that substring is absent.
func TestRunConsume_OwnerMeTemplate_AsBot_RejectedByTemplateAuthTypesBeforePlanApply(t *testing.T) {
	f := newRefinedConsumeTestFactory(t)
	err := newConsumeCmd(f, "im.message.created_v1/owner/me", "--as", "bot").Execute()

	if err == nil {
		t.Fatal("expected an error for --as bot on the user-only owner/me template, got nil")
	}
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	if ve.Param != "--as" {
		t.Errorf("Param = %q, want --as", ve.Param)
	}
	if strings.Contains(err.Error(), "/open-apis/event/v1/subscriptions") {
		t.Errorf("template AuthTypes must reject BEFORE any Plan/Apply network call (apply seam must never be reached), got: %v", err)
	}
}

// TestRunConsume_OwnerMeTemplate_AsUser_PassesTemplateCheck_ProceedsToRealChain
// is Fix 1's non-regression counterpart: a user identity satisfies BOTH
// AuthTypes tiers (base key AND the owner/me template) and so must still
// proceed all the way into the real refined chain, exactly like
// TestRunConsume_RefinedMaterializedKey_DrivesRealRefinedChain_FailsSafelyOffline's
// chat-id case -- failing only once it reaches PlanRemoteSubscription's real
// (stub-less) List call, never earlier.
func TestRunConsume_OwnerMeTemplate_AsUser_PassesTemplateCheck_ProceedsToRealChain(t *testing.T) {
	f := newRefinedConsumeTestFactory(t)
	err := newConsumeCmd(f, "im.message.created_v1/owner/me", "--as", "user").Execute()

	if err == nil {
		t.Fatal("expected an error (this Factory registers no HTTP stubs), got nil")
	}
	if _, ok := errs.ProblemOf(err); !ok {
		t.Fatalf("expected a typed errs.* error even from a deep chain failure, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "/open-apis/event/v1/subscriptions") {
		t.Errorf("expected --as user on owner/me to reach PlanRemoteSubscription's real List call, got: %v", err)
	}
}

// ---- runRefinedConsume strict-mode write-safety (review finding I1) ----
//
// resolveIdentity (shared by both the legacy and refined branches) calls
// f.ResolveAs + f.CheckIdentity but never f.CheckStrictMode. Every sibling
// --as write command DOES gate strict mode right after ResolveAs —
// cmd/event/subscription/subscription.go's resolveEffectiveIdentity (whose
// own fix is locked by TestResolveEffectiveIdentity_StrictModeRejectsCrossIdentity
// in cmd/event/subscription/subscription_test.go), cmd/api/api.go's apiRun,
// cmd/service/service.go's serviceMethodRun, and cmd/whoami/whoami.go's
// whoamiRun. Because Factory.ResolveAs deliberately preserves an explicit
// --as through strict mode (see its own doc comment: "so CheckStrictMode can
// reject incompatible requests"), omitting CheckStrictMode lets an explicit
// --as of the disallowed type slip through whenever it also happens to
// satisfy the EventKey's/template's own AuthTypes whitelist — and on a
// refined key that reaches PlanRemoteSubscription/ApplyRemoteSubscriptionPlan,
// this means a remote create/reuse/reactivate of a Subscription under the
// wrong authority. The fix adds f.CheckStrictMode right after
// resolveIdentity returns in runRefinedConsume, before CheckTemplateAuthTypes
// or any client/subClient construction — i.e. before any remote write.

// TestRunRefinedConsume_StrictModeBotOnly_AsUser_RejectedBeforePlanApply is
// this task's REQUIRED write-safety case: im.message.created_v1's chat-id
// template declares auth_types:["user","bot"] at BOTH the base-key and
// template tier (events/refined/refined_keys_mock.json), so --as user
// otherwise passes both of resolveIdentity's CheckIdentity and
// runRefinedConsume's own CheckTemplateAuthTypes on a bot-only strict-mode
// account — f.CheckStrictMode is the only gate that can catch it. This
// Factory (newStrictBotOnlyRefinedConsumeTestFactory) registers zero HTTP
// stubs, so if the rejection did NOT happen up front, the chain would
// instead reach PlanRemoteSubscription's real List call and fail with
// "/open-apis/event/v1/subscriptions" in the error (exactly as
// TestRunConsume_RefinedMaterializedKey_DrivesRealRefinedChain_FailsSafelyOffline
// demonstrates) — proving the write seam (Plan/Apply/RunRefined) was never
// reached is therefore equivalent to proving that substring is absent.
func TestRunRefinedConsume_StrictModeBotOnly_AsUser_RejectedBeforePlanApply(t *testing.T) {
	f := newStrictBotOnlyRefinedConsumeTestFactory(t)
	err := newConsumeCmd(f, "im.message.created_v1/chat-id/oc_9f3b1c2d8a", "--as", "user").Execute()

	if err == nil {
		t.Fatal("expected an error for --as user under strict mode bot, got nil")
	}
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeInvalidArgument {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeInvalidArgument)
	}
	if !strings.Contains(err.Error(), "strict mode") {
		t.Errorf("expected the strict-mode error wording (f.CheckStrictMode), got: %v", err)
	}
	if strings.Contains(err.Error(), "/open-apis/event/v1/subscriptions") {
		t.Errorf("strict-mode check must reject BEFORE any Plan/Apply network call (write seam must never be reached), got: %v", err)
	}
}

// TestRunRefinedConsume_StrictModeBotOnly_AsBot_PassesStrictModeCheck_ProceedsToRealChain
// is the non-regression counterpart: --as bot matches the bot-only
// strict-mode account, so it must pass f.CheckStrictMode (as well as both
// AuthTypes tiers) and proceed all the way into the real refined chain,
// exactly like
// TestRunConsume_RefinedMaterializedKey_DrivesRealRefinedChain_FailsSafelyOffline
// — failing only once it reaches PlanRemoteSubscription's real (stub-less)
// List call, never earlier. This locks that the new check does not
// misfire on an ALLOWED identity, even when strict mode is active.
func TestRunRefinedConsume_StrictModeBotOnly_AsBot_PassesStrictModeCheck_ProceedsToRealChain(t *testing.T) {
	f := newStrictBotOnlyRefinedConsumeTestFactory(t)
	err := newConsumeCmd(f, "im.message.created_v1/chat-id/oc_9f3b1c2d8a", "--as", "bot").Execute()

	if err == nil {
		t.Fatal("expected an error (this Factory registers no HTTP stubs), got nil")
	}
	if _, ok := errs.ProblemOf(err); !ok {
		t.Fatalf("expected a typed errs.* error even from a deep chain failure, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "/open-apis/event/v1/subscriptions") {
		t.Errorf("expected --as bot on a bot-only strict-mode account to reach PlanRemoteSubscription's real List call, got: %v", err)
	}
}

// TestRunConsume_LegacyKeyWithSuffixRejected locks that a legacy EventKey
// rejects any "/"-suffix (exact match only — legacy keys never enter the
// refined split path) with a message distinct from
// "unknown EventKey" — contrast
// TestRunConsume_UnknownEventKeyContractPreserved, which must keep the old
// wording for a genuinely unregistered base.
func TestRunConsume_LegacyKeyWithSuffixRejected(t *testing.T) {
	f := newRefinedConsumeTestFactory(t)
	err := newConsumeCmd(f, "im.message.receive_v1/foo").Execute()

	ve := assertInvalidArgumentParam(t, err, "event_key")
	if strings.Contains(ve.Message, "unknown EventKey") {
		t.Errorf("legacy+suffix must not be reported as an unknown key: %q", ve.Message)
	}
	if !strings.Contains(ve.Hint, "exact match") {
		t.Errorf("Hint = %q, want guidance that legacy keys only accept an exact match", ve.Hint)
	}
}

// TestRunConsume_UnknownEventKeyContractPreserved is the cmd/event/consume.go
// in-process counterpart of the committed regression
// tests/cli_e2e/event/event_consume_error_test.go
// (TestEventConsumeUnknownKeyRegression). ResolveEventKey's own "unknown"
// wording ("unknown EventKey %q (base %q is not registered)") differs from
// the pre-existing contract ("unknown EventKey: <key>"), so runConsume must
// map the unknown-base case back onto the established unknownEventKeyErr
// message/hint rather than surfacing ResolveEventKey's message verbatim.
func TestRunConsume_UnknownEventKeyContractPreserved(t *testing.T) {
	f := newRefinedConsumeTestFactory(t)
	err := newConsumeCmd(f, "bogus.key").Execute()

	p, ok := errs.ProblemOf(err)
	if !ok {
		t.Fatalf("expected typed errs error, got %T: %v", err, err)
	}
	if p.Category != errs.CategoryValidation || p.Subtype != errs.SubtypeInvalidArgument {
		t.Errorf("problem = %s/%s, want %s/%s", p.Category, p.Subtype,
			errs.CategoryValidation, errs.SubtypeInvalidArgument)
	}
	if !strings.Contains(p.Message, "unknown EventKey: bogus.key") {
		t.Errorf("message = %q, want it to contain %q", p.Message, "unknown EventKey: bogus.key")
	}
	if !strings.Contains(p.Hint, "event list") {
		t.Errorf("hint = %q, want it to mention `event list`", p.Hint)
	}
}

// ---- legacy --dry-run must be a true no-op (never starts a bus) ----

// TestRunConsume_LegacyKey_DryRun_NoOp_ExitsZero_NeverStartsBus locks that
// --dry-run against a LEGACY (non-refined) EventKey behaves exactly as
// --help promises: a no-op, because a legacy key has no remote-Subscription
// plan for a dry-run to preview at all. Before this fix, the legacy branch
// of runConsume ignored o.dryRun entirely and drove the ordinary
// consume.Run path (starting a bus). This reuses the same blocked-bus-fork
// environment as newRefinedConsumeTestFactory (its "events" config dir is a
// regular file, not a directory):
// TestRunConsume_OrdinaryKey_NoIncludeResourceDataFlag_UnaffectedNonRegression
// above proves that WITHOUT --dry-run, this exact legacy key/identity
// reaches that blocked path and fails with a typed InternalError -- so
// err == nil here is proof --dry-run exits before ever reaching
// EnsureBus/forkBus, not merely proof that no error happened to occur. It
// also locks the printed no-op message and that stdout stays completely
// empty (no NDJSON), matching a real refined --dry-run's own
// zero-stdout contract.
func TestRunConsume_LegacyKey_DryRun_NoOp_ExitsZero_NeverStartsBus(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("LARKSUITE_CLI_CONFIG_DIR", dir)
	if err := os.WriteFile(filepath.Join(dir, "events"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	f, stdoutBuf, stderrBuf, _ := cmdutil.TestFactory(t, &core.CliConfig{
		AppID: "cli_consume_test", AppSecret: "secret", Brand: core.BrandFeishu,
	})

	err := newConsumeCmd(f, "im.message.receive_v1", "--as", "bot", "--dry-run").Execute()
	if err != nil {
		t.Fatalf("legacy --dry-run must be a no-op that exits 0, got error: %v", err)
	}
	if !strings.Contains(stderrBuf.String(), "nothing to preview") {
		t.Errorf("expected a no-op message on stderr, got: %q", stderrBuf.String())
	}
	if stdoutBuf.Len() != 0 {
		t.Errorf("legacy --dry-run must never write to stdout, got: %q", stdoutBuf.String())
	}
}

// ---- --include-resource-data flag behavior ----
//
// `event consume` originally shipped with NO --include-resource-data flag at
// all: internal/event/consume/refined.go hardcodes IncludeResourceData(false)
// for its own remote write, so passing --include-resource-data on the CLI
// produced a generic cobra "unknown flag" error instead of a typed
// rejection (silent degradation is a bug — the caller must get an explicit, typed
// explanation of the deferral, not a parse error indistinguishable from a
// typo). The tests below lock the flag's behavior: it exists, defaults to
// false (no behavior change), and true is SUPPORTED on a refined key
// (it creates an ENCRYPTED subscription; the former
// resource_data_encryption_deferred gate is retired) while staying a typed
// invalid_argument on an ordinary key (the flag has no remote-Subscription
// concept to apply to there).

// TestNewCmdConsume_HasIncludeResourceDataFlag is the cheapest possible
// regression guard for the underlying bug this gap-fill closes: before this
// change, cmd.Flags().Lookup("include-resource-data") was nil and cobra
// itself rejected the flag pre-RunE with an untyped "unknown flag" error,
// never reaching any of runConsume's own typed-error paths.
func TestNewCmdConsume_HasIncludeResourceDataFlag(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdConsume(f)
	if cmd.Flags().Lookup("include-resource-data") == nil {
		t.Error("NewCmdConsume missing --include-resource-data flag")
	}
}

// TestRunConsume_RefinedKey_IncludeResourceDataTrue_AsBot_RejectedRequiresUser
// locks that resource data is a user-only capability: --include-resource-data=true
// on a bot (here auto→bot, since this Factory has no identity hint and default
// auto resolves to bot) is a typed invalid_argument fired BEFORE any remote
// call — it must never reach PlanRemoteSubscription's List. The USER path
// staying un-gated and reaching Plan is covered by
// TestRunRefinedConsume_IncludeResourceDataTrue_EncryptKeyScopePresent_PassesPreflight_ReachesPlan.
func TestRunConsume_RefinedKey_IncludeResourceDataTrue_AsBot_RejectedRequiresUser(t *testing.T) {
	f := newRefinedConsumeTestFactory(t)
	err := newConsumeCmd(f, "im.message.created_v1/chat-id/oc_9f3b1c2d8a", "--include-resource-data=true").Execute()

	ve := assertInvalidArgumentParam(t, err, "--include-resource-data")
	if !strings.Contains(ve.Message, "--as user") {
		t.Errorf("Message = %q, want it to require --as user", ve.Message)
	}
	if strings.Contains(err.Error(), "resource_data_encryption_deferred") {
		t.Fatalf("the old E-deferred gate must be retired, got: %v", err)
	}
	// The user-only gate rejects before any remote List call.
	if strings.Contains(err.Error(), "/open-apis/event/v1/subscriptions") {
		t.Errorf("--include-resource-data=true + bot must reject BEFORE any remote List call, got: %v", err)
	}
}

// TestRunConsume_RefinedKey_IncludeResourceDataFalse_PassesGate_NonRegression
// mirrors cmd/event/subscription/create_test.go's
// TestRunCreate_IncludeResourceDataFalse_PassesEGate: explicitly passing
// --include-resource-data=false (not just omitting it) must never trip the
// new gate — the refined chain proceeds all the way to
// PlanRemoteSubscription's real List call exactly as it did before this
// gap-fill.
func TestRunConsume_RefinedKey_IncludeResourceDataFalse_PassesGate_NonRegression(t *testing.T) {
	f := newRefinedConsumeTestFactory(t)
	err := newConsumeCmd(f, "im.message.created_v1/chat-id/oc_9f3b1c2d8a", "--include-resource-data=false").Execute()

	if err == nil {
		t.Fatal("expected an error (this Factory registers no HTTP stubs), got nil")
	}
	var ve *errs.ValidationError
	if errors.As(err, &ve) && ve.Subtype == errs.SubtypeFailedPrecondition && ve.Param == "--include-resource-data" {
		t.Fatalf("--include-resource-data=false must not trip the gate, got: %v", err)
	}
	if !strings.Contains(err.Error(), "/open-apis/event/v1/subscriptions") {
		t.Errorf("--include-resource-data=false must still proceed to PlanRemoteSubscription's real List call, got: %v", err)
	}
}

// TestRunConsume_OrdinaryKey_IncludeResourceDataTrue_TypedInvalidArgument
// locks that passing --include-resource-data to an ordinary Key is a typed
// invalid_argument rejection: the flag only ever controls a
// refined key's remote Subscription (see `event subscription
// create/update --include-resource-data`); passing true against an
// ORDINARY (legacy) EventKey is a caller mistake, not a not-yet-supported
// capability, so it is a DIFFERENT typed subtype (invalid_argument) than
// the refined case's failed_precondition above — and it must fire before
// ANY side effect, not just before a refined-only remote call: this test
// passes no --as at all even though im.message.receive_v1 requires --as
// bot (events/im/register.go), proving the gate runs before identity
// resolution too.
func TestRunConsume_OrdinaryKey_IncludeResourceDataTrue_TypedInvalidArgument(t *testing.T) {
	f := newRefinedConsumeTestFactory(t)
	err := newConsumeCmd(f, "im.message.receive_v1", "--include-resource-data=true").Execute()

	ve := assertInvalidArgumentParam(t, err, "--include-resource-data")
	if !strings.Contains(ve.Message, "im.message.receive_v1") {
		t.Errorf("Message = %q, want it to name the rejected EventKey", ve.Message)
	}
	if strings.Contains(err.Error(), "resource_data_encryption_deferred") {
		t.Errorf("an ordinary key's rejection must not claim the encryption-deferred reason (that gate is refined-key-only), got: %v", err)
	}
}

// TestRunConsume_OrdinaryKey_NoIncludeResourceDataFlag_UnaffectedNonRegression
// proves plain `event consume im.message.receive_v1 --as bot` (the flag
// never passed at all — the pre-gap-fill baseline) still behaves exactly
// as before: it must proceed past the new gate, past identity resolution,
// all the way down into consume.Run -> EnsureBus -> forkBus, which fails
// deterministically (and fast — no real network, no real bus, no real
// fork survives) against newRefinedConsumeTestFactory's blocked
// LARKSUITE_CLI_CONFIG_DIR/events safety net. A typed InternalError from
// that unrelated, much-later failure point — never an
// --include-resource-data validation error — is proof the gate did not
// misfire on the default (flag-omitted) path.
func TestRunConsume_OrdinaryKey_NoIncludeResourceDataFlag_UnaffectedNonRegression(t *testing.T) {
	f := newRefinedConsumeTestFactory(t)
	err := newConsumeCmd(f, "im.message.receive_v1", "--as", "bot").Execute()

	if err == nil {
		t.Fatal("expected an error (blocked bus-fork path / no HTTP stubs in this Factory), got nil")
	}
	var ve *errs.ValidationError
	if errors.As(err, &ve) && ve.Param == "--include-resource-data" {
		t.Fatalf("omitting --include-resource-data must not trip the new gate, got: %v", err)
	}
	if _, ok := errs.ProblemOf(err); !ok {
		t.Fatalf("expected a typed errs.* error even from the unrelated bus-fork failure, got %T: %v", err, err)
	}
}

// ---- --include-resource-data=true refined encrypt_key:read scope preflight ----
//
// Before this fix, event:encrypt_key:read was only ever checked implicitly
// at RunRefined's post-Apply/HelloV2 prewarm stage -- so a missing scope
// meant the encrypted remote Subscription was ALREADY created (refined
// cleanup is nil: never auto-deleted) by the time the gap was discovered.
// These tests drive runConsume's real refined entry (via NewCmdConsume +
// cmd.Execute()) with a Credential whose ResolveToken result carries a
// CONFIRMED (non-empty) scope string, proving the new local preflight
// rejects before ever reaching PlanRemoteSubscription's real List call
// (which this Factory's zero registered HTTP stubs would otherwise fail
// with an error naming "/open-apis/event/v1/subscriptions" -- exactly as
// TestRunConsume_RefinedKey_IncludeResourceDataTrue_UnGated_ReachesPlan
// demonstrates when scopes are NOT locally known).

// fixedScopeTokenResolver resolves a fixed token whose Scopes is a
// CONFIRMED (non-empty) value a test controls directly -- as opposed to
// cmdutil.TestFactory's own (unexported) testDefaultToken, which always
// returns Scopes=="" (unknown -- every local scope precheck in this CLI
// treats that as a best-effort skip, not "missing").
type fixedScopeTokenResolver struct{ scopes string }

func (r fixedScopeTokenResolver) ResolveToken(_ context.Context, _ credential.TokenSpec) (*credential.TokenResult, error) {
	return &credential.TokenResult{Token: "test-user-token", Scopes: r.scopes}, nil
}

// newRefinedConsumeTestFactoryWithUserScopes mirrors newRefinedConsumeTestFactory
// (same blocked-bus-fork safety net) but additionally swaps in a Credential
// whose ResolveToken result carries the given (confirmed, non-empty)
// scopes, so a test can exercise the local encrypt_key:read preflight's
// confirmed-missing branch instead of its unknown-scopes/best-effort-skip
// branch. nil defaultAcct/httpClient mirrors this file's own
// factoryWithResolver (used by TestResolveTenantToken_* above): an explicit
// --as short-circuits resolveIdentity before it ever touches Credential,
// and Factory.CheckStrictMode treats an unresolvable account as
// strict-mode-off rather than an error, so neither is needed here.
func newRefinedConsumeTestFactoryWithUserScopes(t *testing.T, scopes string) *cmdutil.Factory {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("LARKSUITE_CLI_CONFIG_DIR", dir)
	if err := os.WriteFile(filepath.Join(dir, "events"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{
		AppID: "cli_consume_test", AppSecret: "secret", Brand: core.BrandFeishu,
	})
	f.Credential = credential.NewCredentialProvider(nil, nil, fixedScopeTokenResolver{scopes: scopes}, nil)
	return f
}

// TestRunRefinedConsume_IncludeResourceDataTrue_MissingEncryptKeyScope_RejectedBeforePlanApply
// is this task's REQUIRED case: a user token confirmed to hold
// event:subscription:{read,write} but NOT event:encrypt_key:read must be
// rejected as a typed missing_scope error before any Plan/Apply network
// call, when --include-resource-data=true.
func TestRunRefinedConsume_IncludeResourceDataTrue_MissingEncryptKeyScope_RejectedBeforePlanApply(t *testing.T) {
	f := newRefinedConsumeTestFactoryWithUserScopes(t, "event:subscription:read event:subscription:write")
	err := newConsumeCmd(f, "im.message.created_v1/chat-id/oc_9f3b1c2d8a", "--as", "user", "--include-resource-data=true").Execute()

	if err == nil {
		t.Fatal("expected a missing-scope error, got nil")
	}
	var permErr *errs.PermissionError
	if !errors.As(err, &permErr) {
		t.Fatalf("expected *errs.PermissionError, got %T: %v", err, err)
	}
	if permErr.Category != errs.CategoryAuthorization || permErr.Subtype != errs.SubtypeMissingScope {
		t.Errorf("problem = %s/%s, want %s/%s", permErr.Category, permErr.Subtype,
			errs.CategoryAuthorization, errs.SubtypeMissingScope)
	}
	if len(permErr.MissingScopes) != 1 || permErr.MissingScopes[0] != "event:encrypt_key:read" {
		t.Errorf("MissingScopes = %v, want [event:encrypt_key:read]", permErr.MissingScopes)
	}
	if strings.Contains(err.Error(), "/open-apis/event/v1/subscriptions") {
		t.Errorf("the encrypt_key:read scope preflight must reject BEFORE any Plan/Apply network call, got: %v", err)
	}
}

// TestRunRefinedConsume_IncludeResourceDataTrue_EncryptKeyScopePresent_PassesPreflight_ReachesPlan
// is the non-regression counterpart: holding event:encrypt_key:read (on top
// of the usual subscription scopes) must pass the new preflight and
// proceed all the way into the real refined chain, exactly like
// TestRunConsume_RefinedMaterializedKey_DrivesRealRefinedChain_FailsSafelyOffline
// -- failing only once it reaches PlanRemoteSubscription's real (stub-less)
// List call, never earlier. This locks that the new gate does not misfire
// on a token that already holds the scope it checks for.
func TestRunRefinedConsume_IncludeResourceDataTrue_EncryptKeyScopePresent_PassesPreflight_ReachesPlan(t *testing.T) {
	f := newRefinedConsumeTestFactoryWithUserScopes(t, "event:subscription:read event:subscription:write event:encrypt_key:read")
	err := newConsumeCmd(f, "im.message.created_v1/chat-id/oc_9f3b1c2d8a", "--as", "user", "--include-resource-data=true").Execute()

	if err == nil {
		t.Fatal("expected an error (this Factory registers no HTTP stubs), got nil")
	}
	var permErr *errs.PermissionError
	if errors.As(err, &permErr) && permErr.Subtype == errs.SubtypeMissingScope {
		t.Fatalf("holding event:encrypt_key:read must not trip the preflight, got: %v", err)
	}
	if !strings.Contains(err.Error(), "/open-apis/event/v1/subscriptions") {
		t.Errorf("expected the preflight to pass and reach PlanRemoteSubscription's real List call, got: %v", err)
	}
}

func TestSanitizeOutputDir(t *testing.T) {
	cases := []struct {
		name       string
		in         string
		wantSentry error
	}{
		{
			name: "relative path accepted",
			in:   "./output",
		},
		{
			name: "nested relative path accepted",
			in:   "events/today",
		},
		{
			name:       "tilde rejected explicitly",
			in:         "~/events",
			wantSentry: errOutputDirTilde,
		},
		{
			name:       "parent escape rejected",
			in:         "../outside",
			wantSentry: errOutputDirUnsafe,
		},
		{
			name:       "absolute path rejected",
			in:         "/tmp/events",
			wantSentry: errOutputDirUnsafe,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sanitizeOutputDir(tc.in)
			if tc.wantSentry != nil {
				if err == nil {
					t.Fatalf("want error wrapping %v, got nil (path=%q)", tc.wantSentry, got)
				}
				if !errors.Is(err, tc.wantSentry) {
					t.Fatalf("want errors.Is(err, %v), got %q", tc.wantSentry, err.Error())
				}
				assertInvalidArgumentParam(t, err, "--output-dir")
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got == "" {
				t.Errorf("expected non-empty safe path, got %q", got)
			}
		})
	}
}
