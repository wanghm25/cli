// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package session

import (
	"errors"
	"testing"

	"github.com/larksuite/cli/internal/event/model"
)

func TestOwnerMatchesCurrent(t *testing.T) {
	cur := CurrentIdentity{AppID: "app1", UserOpenID: "ou_alice", Profile: "work"}
	cases := []struct {
		name  string
		owner model.OwnerRef
		want  bool
	}{
		{"exact match", model.OwnerRef{AppID: "app1", UserOpenID: "ou_alice"}, true},
		{"profile is irrelevant", model.OwnerRef{AppID: "app1", UserOpenID: "ou_alice", Profile: "personal"}, true},
		{"different user", model.OwnerRef{AppID: "app1", UserOpenID: "ou_bob"}, false},
		{"different app", model.OwnerRef{AppID: "app2", UserOpenID: "ou_alice"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := OwnerMatchesCurrent(tc.owner, cur); got != tc.want {
				t.Errorf("OwnerMatchesCurrent(%+v, %+v) = %v, want %v", tc.owner, cur, got, tc.want)
			}
		})
	}
}

func TestGate_FailClosedPolicy(t *testing.T) {
	cur := CurrentIdentity{AppID: "app1", UserOpenID: "ou_alice"}
	user := model.OwnerRef{AppID: "app1", UserOpenID: "ou_alice"}
	otherUser := model.OwnerRef{AppID: "app1", UserOpenID: "ou_bob"}
	bot := model.OwnerRef{AppID: "app1", UserOpenID: ""}

	if got := Gate(user, cur, nil); got != AdmitDeliver {
		t.Errorf("owner==current: Gate = %v, want AdmitDeliver", got)
	}
	if got := Gate(otherUser, cur, nil); got != AdmitStale {
		t.Errorf("owner!=current: Gate = %v, want AdmitStale (fail closed)", got)
	}
	if got := Gate(user, CurrentIdentity{}, errors.New("config unreadable")); got != AdmitUnresolved {
		t.Errorf("resolve error: Gate = %v, want AdmitUnresolved (fail closed)", got)
	}
	// A bot/legacy owner is NEVER identity-gated — even a resolve error must not
	// change its outcome (it never reaches that branch).
	if got := Gate(bot, cur, nil); got != AdmitDeliver {
		t.Errorf("bot owner: Gate = %v, want AdmitDeliver (never gated)", got)
	}
	if got := Gate(bot, CurrentIdentity{}, errors.New("unresolved")); got != AdmitDeliver {
		t.Errorf("bot owner under resolve error: Gate = %v, want AdmitDeliver (never gated)", got)
	}
}
