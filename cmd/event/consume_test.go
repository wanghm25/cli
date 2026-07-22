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

// TestRunConsume_BareRefinedBaseKeyRejected locks design spec §2.3 R1 at the
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
// is Task 15b's counterpart to the removed
// TestRunConsume_RefinedMaterializedKey_RuntimeNotYetAvailable: a fully
// materialized refined EventKey (R1 satisfied) is no longer rejected by a
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

// TestRunConsume_LegacyKeyWithSuffixRejected locks that a legacy EventKey
// rejects any "/"-suffix (exact match only — legacy keys never enter the
// refined split path, design spec §2.3 step 1) with a message distinct from
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
