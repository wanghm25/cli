// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package session

import "testing"

func TestSourceEpoch_AdvanceAndIsCurrent(t *testing.T) {
	var e SourceEpoch
	if e.Current() != 0 {
		t.Fatalf("zero value Current() = %d, want 0", e.Current())
	}
	first := e.Advance()
	if first != 1 || !e.IsCurrent(first) {
		t.Fatalf("Advance() = %d (IsCurrent=%v), want 1/true", first, e.IsCurrent(first))
	}
	second := e.Advance()
	if second != 2 {
		t.Fatalf("second Advance() = %d, want 2", second)
	}
	// A result captured under the old epoch is no longer current once a newer
	// generation has advanced past it.
	if e.IsCurrent(first) {
		t.Error("first epoch must not be current after a newer Advance()")
	}
	if !e.IsCurrent(second) {
		t.Error("second epoch must be current")
	}
}
