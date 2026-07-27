// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package model

import (
	"errors"
	"testing"
)

func TestNewRemoteSubscriptionID_RejectsEmpty(t *testing.T) {
	id, err := NewRemoteSubscriptionID("")
	if !errors.Is(err, ErrEmptyRemoteSubscriptionID) {
		t.Fatalf("empty input error = %v, want ErrEmptyRemoteSubscriptionID", err)
	}
	if !id.IsZero() {
		t.Errorf("failed construction must yield the zero (invalid) id, got %q", id)
	}
}

func TestNewRemoteSubscriptionID_AcceptsNonEmpty(t *testing.T) {
	id, err := NewRemoteSubscriptionID("sub_123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id.IsZero() {
		t.Error("a non-empty id must not report IsZero")
	}
	if id.String() != "sub_123" {
		t.Errorf("String() = %q, want %q", id.String(), "sub_123")
	}
}

func TestRemoteSubscriptionID_ZeroValueIsInvalid(t *testing.T) {
	var id RemoteSubscriptionID
	if !id.IsZero() {
		t.Error("the zero value must be invalid (IsZero)")
	}
}
