// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/auth"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
)

// --- R2 #5/#6/#10: resolveUAT must be bound to the requested userOpenID ---
//
// verifyUATBelongsToUser rejects a UAT that provably belongs to another user
// (the profile-switch race), while never false-rejecting when the requested
// user's token is unknown (extension credential providers keep nothing in the
// keychain). The keychain lookup is stubbed via getStoredUAToken so these
// exercise the pure decision logic without touching the OS keychain.

func TestVerifyUATBelongsToUser(t *testing.T) {
	cases := []struct {
		name       string
		userOpenID string
		token      string
		stored     *auth.StoredUAToken // what getStoredUAToken returns for (appID,userOpenID)
		wantErr    bool
	}{
		{
			name:       "matching stored token is accepted",
			userOpenID: "ou_alice",
			token:      "tok-alice",
			stored:     &auth.StoredUAToken{AppId: "app1", UserOpenId: "ou_alice", AccessToken: "tok-alice"},
			wantErr:    false,
		},
		{
			name:       "wrong-user token is rejected (profile-switch race)",
			userOpenID: "ou_alice",
			token:      "tok-bob", // resolved another user's token
			stored:     &auth.StoredUAToken{AppId: "app1", UserOpenId: "ou_alice", AccessToken: "tok-alice"},
			wantErr:    true,
		},
		{
			name:       "no stored token for the user is NOT rejected (extension provider path)",
			userOpenID: "ou_alice",
			token:      "tok-from-extension",
			stored:     nil,
			wantErr:    false,
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

			err := verifyUATBelongsToUser("app1", tc.userOpenID, tc.token)
			if tc.wantErr && err == nil {
				t.Fatal("verifyUATBelongsToUser = nil, want a mismatch rejection")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("verifyUATBelongsToUser = %v, want nil", err)
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
