// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package model

import "testing"

// TestOwnerRef_Fields locks the value-object shape that later PRs (Gateway /
// Receipt) construct and read, so a field rename or removal fails loudly here.
func TestOwnerRef_Fields(t *testing.T) {
	ref := OwnerRef{
		AppID:      "cli_app",
		Identity:   "user",
		UserOpenID: "ou_abc",
		Profile:    "Alice (diagnostics only)",
	}
	if ref.AppID != "cli_app" || ref.Identity != "user" || ref.UserOpenID != "ou_abc" {
		t.Errorf("OwnerRef identity fields not preserved: %+v", ref)
	}
	if ref.Profile != "Alice (diagnostics only)" {
		t.Errorf("Profile = %q, want the diagnostics label", ref.Profile)
	}
}
