// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package consume

import (
	"strings"
	"testing"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/event"
	subown "github.com/larksuite/cli/internal/event/subscription"
)

// TestRefinedHints_CarryOwningProfileAndIdentity locks that refined consume's
// recovery hints render the OWNING profile (--profile) + identity (--as) through
// event.CommandContext, so a suggested recovery/retry command targets the same
// app + identity the consumer bootstrapped as.
func TestRefinedHints_CarryOwningProfileAndIdentity(t *testing.T) {
	ctx := event.CommandContext{Profile: "B", Identity: core.AsBot}
	resolved := event.ResolvedEventKey{MaterializedKey: "im.message.created_v1/chat-id/oc_x"}

	// The apply-ok retry command must carry --profile B --as bot.
	retry := applyOkRecoveryHint(resolved, ctx, "sub_1", true)
	for _, want := range []string{"--profile B", "--as bot", "event consume"} {
		if !strings.Contains(retry, want) {
			t.Errorf("applyOkRecoveryHint = %q, missing %q", retry, want)
		}
	}

	// The conflict recovery command (get) must carry --profile B --as bot.
	confErr := refinedConflictError(resolved, ctx, subown.SubscriptionPlan{Action: subown.ActionBlock})
	p, ok := errs.ProblemOf(confErr)
	if !ok {
		t.Fatalf("refinedConflictError did not carry a Problem: %v", confErr)
	}
	for _, want := range []string{"--profile B", "--as bot", "event subscription get"} {
		if !strings.Contains(p.Hint, want) {
			t.Errorf("refinedConflictError hint = %q, missing %q", p.Hint, want)
		}
	}
}

// TestRefinedHints_NoProfile_OmitsProfileFlag locks that with no owning profile
// (empty CommandContext.Profile) the hint stays a bare `lark-cli ...` with no
// --profile — the default-profile case is unchanged.
func TestRefinedHints_NoProfile_OmitsProfileFlag(t *testing.T) {
	ctx := event.CommandContext{Identity: core.AsUser} // no Profile
	resolved := event.ResolvedEventKey{MaterializedKey: "k"}
	retry := applyOkRecoveryHint(resolved, ctx, "sub_1", false)
	if strings.Contains(retry, "--profile") {
		t.Errorf("applyOkRecoveryHint = %q, want no --profile when profile is unset", retry)
	}
	if !strings.Contains(retry, "lark-cli event consume") || !strings.Contains(retry, "--as user") {
		t.Errorf("applyOkRecoveryHint = %q, want a bare lark-cli consume with --as user", retry)
	}
}
