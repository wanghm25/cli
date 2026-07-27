// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package imcontract

import (
	"slices"
	"testing"
	"time"
)

func TestWriteRegistryCoverage(t *testing.T) {
	counts := map[StrategyKind]int{}
	total := 0
	for _, contract := range All() {
		if contract.Strategy.Kind.IsWrite() {
			counts[contract.Strategy.Kind]++
			total++
		}
	}
	if total != 36 {
		t.Fatalf("write contracts = %d, want 36", total)
	}
	want := map[StrategyKind]int{
		AuthoritativeAckKind:           9,
		RequiredResultKind:             12,
		BatchPartialKind:               11,
		RequiredResultBatchPartialKind: 1,
		ResponseSetAssertionKind:       2,
		ExemptionKind:                  1,
	}
	for kind, n := range want {
		if counts[kind] != n {
			t.Errorf("%s = %d, want %d", kind, counts[kind], n)
		}
	}
	if err := ValidateRegistry(time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	wantKeys := []ContractKey{
		"im +chat-create", "im +chat-update", "im +feed-shortcut-create",
		"im +feed-shortcut-remove", "im +flag-cancel", "im +flag-create",
		"im +messages-reply", "im +messages-send",
		"im chat.managers add_managers", "im chat.managers delete_managers",
		"im chat.members create", "im chat.members delete",
		"im chat.moderation update", "im chat.nickname delete",
		"im chat.nickname update", "im chat.user_setting batch_update",
		"im chats create", "im chats link", "im chats update",
		"im feed.groups batch_add_item", "im feed.groups batch_remove_item",
		"im feed.groups create", "im feed.groups delete", "im feed.groups update",
		"im images create", "im messages delete", "im messages forward",
		"im messages merge_forward", "im messages urgent_app",
		"im messages urgent_phone", "im messages urgent_sms", "im pins create",
		"im pins delete", "im reactions create", "im reactions delete",
		"im threads forward",
	}
	gotKeys := make([]ContractKey, 0, len(All()))
	for _, c := range All() {
		gotKeys = append(gotKeys, c.Key)
	}
	if !slices.Equal(gotKeys, wantKeys) {
		t.Fatalf("write registry keys differ:\ngot  %v\nwant %v", gotKeys, wantKeys)
	}
}

func TestModerationExemption(t *testing.T) {
	c, ok := Lookup("im chat.moderation update")
	if !ok || c.Exemption == nil {
		t.Fatal("moderation exemption missing")
	}
	if c.Exemption.Owner != "IM backend" || c.Exemption.Expiry != "2026-10-25" {
		t.Fatalf("unexpected exemption: %#v", c.Exemption)
	}
}
