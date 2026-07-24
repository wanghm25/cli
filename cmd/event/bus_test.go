// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/auth"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
)

// --- resolveUAT must be bound to the requested userOpenID ---
//
// verifyUATBelongsToUser rejects a UAT that provably belongs to another user
// (the profile-switch race). When the keychain holds the requested user's own
// token it compares directly (the fast path); when nothing is stored (e.g. an
// extension credential provider) it positively proves the token's open_id and
// fails CLOSED unless that open_id matches — never accepting an unverifiable
// token. The keychain lookup and the positive verifier are both stubbed so
// these exercise the pure decision logic without touching the OS keychain or a
// live user_info call.

func TestVerifyUATBelongsToUser(t *testing.T) {
	cases := []struct {
		name         string
		userOpenID   string
		token        string
		stored       *auth.StoredUAToken // what getStoredUAToken returns for (appID,userOpenID)
		provedOpenID string              // what proveOpenID returns on the no-stored path
		proveErr     error               // proveOpenID failure on the no-stored path
		noVerifier   bool                // pass a nil proveOpenID (nothing can prove ownership)
		wantErr      bool
		wantSentinel error // optional: assert errors.Is on the rejection
	}{
		{
			name:       "matching stored token is accepted",
			userOpenID: "ou_alice",
			token:      "tok-alice",
			stored:     &auth.StoredUAToken{AppId: "app1", UserOpenId: "ou_alice", AccessToken: "tok-alice"},
			wantErr:    false,
		},
		{
			name:         "wrong-user stored token is rejected (profile-switch race)",
			userOpenID:   "ou_alice",
			token:        "tok-bob", // resolved another user's token
			stored:       &auth.StoredUAToken{AppId: "app1", UserOpenId: "ou_alice", AccessToken: "tok-alice"},
			wantErr:      true,
			wantSentinel: errUATUserMismatch,
		},
		{
			name:         "no stored token + proven matching open_id is accepted (extension provider path)",
			userOpenID:   "ou_alice",
			token:        "tok-from-extension",
			stored:       nil,
			provedOpenID: "ou_alice",
			wantErr:      false,
		},
		{
			name:         "no stored token + proven DIFFERENT open_id is rejected",
			userOpenID:   "ou_alice",
			token:        "tok-bob",
			stored:       nil,
			provedOpenID: "ou_bob",
			wantErr:      true,
			wantSentinel: errUATUserMismatch,
		},
		{
			name:         "no stored token + verification failure is rejected (fail-closed)",
			userOpenID:   "ou_alice",
			token:        "tok-unknown",
			stored:       nil,
			proveErr:     errors.New("user_info API returned HTTP 401"),
			wantErr:      true,
			wantSentinel: errUATUnverifiable,
		},
		{
			name:         "no stored token + no way to verify is rejected (fail-closed)",
			userOpenID:   "ou_alice",
			token:        "tok-unknown",
			stored:       nil,
			noVerifier:   true,
			wantErr:      true,
			wantSentinel: errUATUnverifiable,
		},
		{
			name:       "empty userOpenID skips verification",
			userOpenID: "",
			token:      "tok-anything",
			stored:     nil,
			wantErr:    false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			orig := getStoredUAToken
			t.Cleanup(func() { getStoredUAToken = orig })
			getStoredUAToken = func(appID, userOpenID string) *auth.StoredUAToken {
				if appID != "app1" || userOpenID != tc.userOpenID {
					return nil
				}
				return tc.stored
			}

			var proveCalls int
			var proveOpenID func(context.Context, string) (string, error)
			if !tc.noVerifier {
				proveOpenID = func(_ context.Context, token string) (string, error) {
					proveCalls++
					if token != tc.token {
						t.Errorf("proveOpenID token = %q, want %q", token, tc.token)
					}
					return tc.provedOpenID, tc.proveErr
				}
			}

			err := verifyUATBelongsToUser(context.Background(), "app1", tc.userOpenID, tc.token, proveOpenID)
			if tc.wantErr && err == nil {
				t.Fatal("verifyUATBelongsToUser = nil, want a rejection")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("verifyUATBelongsToUser = %v, want nil", err)
			}
			if tc.wantSentinel != nil && !errors.Is(err, tc.wantSentinel) {
				t.Errorf("verifyUATBelongsToUser err = %v, want errors.Is(%v)", err, tc.wantSentinel)
			}
			// The stored fast path must NOT call the positive verifier (extra
			// API calls are confined to the no-stored path).
			if tc.stored != nil && proveCalls != 0 {
				t.Errorf("proveOpenID calls = %d on the stored fast path, want 0", proveCalls)
			}
		})
	}
}

// The hidden `event _bus` daemon command must exit with a typed file_io error
// when its log directory cannot be created (the error is only visible in the
// forked process's captured stderr / bus.log).
func TestBusCommandLoggerSetupFailureIsTypedFileIO(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("LARKSUITE_CLI_CONFIG_DIR", dir)
	// Block the events/ root with a regular file so MkdirAll fails.
	if err := os.WriteFile(filepath.Join(dir, "events"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}

	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{
		AppID: "cli_bus_test", AppSecret: "secret", Brand: core.BrandFeishu,
	})
	cmd := NewCmdBus(f)
	cmd.SetArgs([]string{})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected logger setup error")
	}
	p, ok := errs.ProblemOf(err)
	if !ok {
		t.Fatalf("expected typed errs error, got %T: %v", err, err)
	}
	if p.Category != errs.CategoryInternal || p.Subtype != errs.SubtypeFileIO {
		t.Errorf("problem = %s/%s, want %s/%s", p.Category, p.Subtype,
			errs.CategoryInternal, errs.SubtypeFileIO)
	}
}
