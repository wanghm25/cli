// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"testing"

	"github.com/spf13/cobra"

	"github.com/larksuite/cli/internal/cmdutil"
)

const (
	riskTestRefinedKey = "im.message.created_v1/chat-id/oc_9f3b1c2d8a"
	riskTestLegacyKey  = "im.message.receive_v1"
)

// TestConsumeArgAwareRisk locks the arg-aware risk derivation that drives the
// pre-startup pruning gate and the --help "Risk:" line: a resolvable refined key
// is write, a resolvable legacy key is read, and any invocation whose EventKey
// cannot be identified/resolved falls back (ok=false) so the caller keeps the
// conservative static "write" — the derivation never under-states a write.
func TestConsumeArgAwareRisk(t *testing.T) {
	cases := []struct {
		name   string
		args   []string
		want   string
		wantOK bool
	}{
		// A refined descriptor implies a remote Create/Reuse/Reactivate -> write.
		{"refined key -> write", []string{"event", "consume", riskTestRefinedKey, "--as", "bot"}, "write", true},
		{"refined dry-run stays write", []string{"event", "consume", riskTestRefinedKey, "--dry-run"}, "write", true},
		// A legacy consume only attaches to delivery (never a remote write) -> read.
		{"legacy key -> read", []string{"event", "consume", riskTestLegacyKey, "--as", "bot"}, "read", true},
		{"legacy dry-run -> read", []string{"event", "consume", riskTestLegacyKey, "--dry-run"}, "read", true},
		// The sole global (--profile) before the subcommand must not be mistaken
		// for the key, and a "consume"-valued profile before "event" is ignored.
		{"global --profile does not eat the key", []string{"--profile", "foo", "event", "consume", riskTestLegacyKey}, "read", true},
		{"stray consume before event ignored", []string{"--profile", "consume", "event", "consume", riskTestLegacyKey}, "read", true},
		// SAFETY: a value flag whose value resembles a legacy key must NOT be read
		// as the positional and downgrade a refined (write) invocation to read.
		{"value flag value is not the key", []string{"event", "consume", riskTestRefinedKey, "--jq", riskTestLegacyKey}, "write", true},
		// --help must NOT abort the refinement: a legacy consume WITH a key and
		// --help/-h still resolves to read, so `event consume <legacy> --help`
		// shows "Risk: read" and is not pruned under a max_risk:read policy.
		// (Before the fix, pflag special-cased an undefined help flag and returned
		// ErrHelp, so the parse failed and the fallback kept the static "write".)
		{"legacy key with --help -> read", []string{"event", "consume", riskTestLegacyKey, "--help"}, "read", true},
		{"legacy key with -h -> read", []string{"event", "consume", riskTestLegacyKey, "-h"}, "read", true},
		{"refined key with --help stays write", []string{"event", "consume", riskTestRefinedKey, "--help"}, "write", true},
		// Conservative fallback paths: keep the static "write".
		{"not a consume invocation", []string{"event", "status"}, "", false},
		{"no positional (help)", []string{"event", "consume", "--help"}, "", false},
		{"unresolvable key", []string{"event", "consume", "not.a.real.key"}, "", false},
		{"two positionals", []string{"event", "consume", riskTestLegacyKey, "extra"}, "", false},
		{"empty args", nil, "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := consumeArgAwareRisk(c.args)
			if ok != c.wantOK {
				t.Fatalf("ok = %v, want %v (level=%q)", ok, c.wantOK, got)
			}
			if ok && got != c.want {
				t.Errorf("risk = %q, want %q", got, c.want)
			}
		})
	}
}

// TestApplyArgAwareConsumeRisk_RefinesAnnotation proves the annotation the
// framework reads is refined in place: a legacy key downgrades the static
// "write" to "read"; a refined key and an unresolved key both leave it at the
// conservative "write".
func TestApplyArgAwareConsumeRisk_RefinesAnnotation(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"legacy downgrades to read", []string{"event", "consume", riskTestLegacyKey}, "read"},
		{"refined stays write", []string{"event", "consume", riskTestRefinedKey}, "write"},
		{"unresolved stays write (fallback)", []string{"event", "status"}, "write"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cmd := &cobra.Command{Use: "consume"}
			cmdutil.SetRisk(cmd, "write") // the command's conservative static default
			applyArgAwareConsumeRisk(cmd, c.args)
			level, ok := cmdutil.GetRisk(cmd)
			if !ok || level != c.want {
				t.Errorf("risk = %q (ok=%v), want %q", level, ok, c.want)
			}
		})
	}
}
