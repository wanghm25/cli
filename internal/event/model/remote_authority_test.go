// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package model

import "testing"

func TestRemoteAuthority_String(t *testing.T) {
	tests := []struct {
		name string
		a    RemoteAuthority
		want string
	}{
		{"zero", RemoteAuthority{}, ""},
		{"user with open_id", RemoteAuthority{Type: "user", OpenID: "ou_xxx"}, "user:ou_xxx"},
		{"user without open_id", RemoteAuthority{Type: "user"}, "user"},
		{"app", RemoteAuthority{Type: "app", AppID: "cli_xxx"}, "app"},
		{"unrecognized type passes through", RemoteAuthority{Type: "service"}, "service"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.String(); got != tc.want {
				t.Errorf("RemoteAuthority(%+v).String() = %q, want %q", tc.a, got, tc.want)
			}
		})
	}
}

func TestRemoteAuthority_AsToken(t *testing.T) {
	tests := []struct {
		name string
		a    RemoteAuthority
		want string
	}{
		{"zero", RemoteAuthority{}, ""},
		// The open_id is NOT embedded — it round-trips through `--as user`.
		{"user with open_id -> user", RemoteAuthority{Type: "user", OpenID: "ou_xxx"}, "user"},
		{"user without open_id -> user", RemoteAuthority{Type: "user"}, "user"},
		// The remote "app" tier maps to the CLI's `--as bot` vocabulary.
		{"app -> bot", RemoteAuthority{Type: "app", AppID: "cli_xxx"}, "bot"},
		{"unrecognized type passes through", RemoteAuthority{Type: "service"}, "service"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.AsToken(); got != tc.want {
				t.Errorf("RemoteAuthority(%+v).AsToken() = %q, want %q", tc.a, got, tc.want)
			}
		})
	}
}

func TestRemoteAuthority_IsZero(t *testing.T) {
	if !(RemoteAuthority{}).IsZero() {
		t.Error("zero RemoteAuthority.IsZero() = false, want true")
	}
	if (RemoteAuthority{Type: "app"}).IsZero() {
		t.Error("non-zero RemoteAuthority.IsZero() = true, want false")
	}
}
