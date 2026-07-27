// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"context"
	"testing"

	"github.com/larksuite/cli/internal/event/bus/lifecycle"
	"github.com/larksuite/cli/internal/event/protocol"
)

// ---- E6: encrypt_key removal on deleted_v1 ----

func TestSubscriptionLifecycleAction_Deleted_RemovesEncryptKey(t *testing.T) {
	hub := NewHub()
	c := newConnWithRemoteSub(t, 1, "sub-del")
	hub.RegisterAndIsFirst(c)

	var removed []string
	action := lifecycle.NewSubscriptionAction(hub.lifecycleRegistry(), discardTestLogger())
	action.SetEncryptKeyRemover(func(subID string) { removed = append(removed, subID) })

	le := lifecycle.LifecycleEvent{EventType: lifecycle.LifecycleEventTypeDeleted, EventID: "evt-del", RemoteSubscriptionID: "sub-del"}
	if err := action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(removed) != 1 || removed[0] != "sub-del" {
		t.Errorf("encrypt-key remover called with %v, want [sub-del]", removed)
	}
	// The existing deleted_v1 behavior is preserved (subscription dimension).
	if c.SubscriptionDegradedReason() != lifecycle.ReasonRemoteSubscriptionDeleted {
		t.Errorf("SubscriptionDegradedReason = %q, want %q", c.SubscriptionDegradedReason(), lifecycle.ReasonRemoteSubscriptionDeleted)
	}
}

func TestSubscriptionLifecycleAction_Deleted_NilRemover_NoPanic(t *testing.T) {
	hub := NewHub()
	action := lifecycle.NewSubscriptionAction(hub.lifecycleRegistry(), discardTestLogger()) // no remover wired
	le := lifecycle.LifecycleEvent{EventType: lifecycle.LifecycleEventTypeDeleted, EventID: "evt", RemoteSubscriptionID: "sub-1"}
	if err := action.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle with nil remover must not fail: %v", err)
	}
}

// TestNewBus_WiresEncryptKeyRemover_DeletedReleasesKey proves the full wiring:
// NewBus connects the lifecycle action's deleted_v1 path to the provider's
// Remove, so a deleted_v1 evicts the cached key from the SDK provider.
func TestNewBus_WiresEncryptKeyRemover_DeletedReleasesKey(t *testing.T) {
	b := NewBus("test-app", "test-secret", "", nil, discardTestLogger())
	// Seed a cached key, then confirm a deleted_v1 evicts it.
	b.encryptKeyProvider.static.Set("sub-gone", "CACHED")
	if _, ok := b.encryptKeyProvider.static.EncryptKey(context.Background(), "sub-gone"); !ok {
		t.Fatal("precondition: key should be cached")
	}
	le := lifecycle.LifecycleEvent{EventType: lifecycle.LifecycleEventTypeDeleted, EventID: "evt", RemoteSubscriptionID: "sub-gone"}
	if err := b.lifecycleAction.Handle(context.Background(), le); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if _, ok := b.encryptKeyProvider.static.EncryptKey(context.Background(), "sub-gone"); ok {
		t.Errorf("deleted_v1 must evict the cached encrypt_key from the provider")
	}
}

// ---- E6: decrypt-failure counting + degrade ----

func TestConn_RecordDecryptFailure_CountsThenDegrades(t *testing.T) {
	c := newConnWithRemoteSub(t, 1, "sub-1")

	// Below the threshold: counted + decrypt_state set, but NOT yet degraded.
	for i := int64(1); i < decryptFailDegradeThreshold; i++ {
		c.RecordDecryptFailure()
		if c.DecryptFailCount() != i {
			t.Errorf("DecryptFailCount = %d, want %d", c.DecryptFailCount(), i)
		}
		if c.DecryptState() != decryptStateFailed {
			t.Errorf("DecryptState = %q, want decrypt_failed", c.DecryptState())
		}
		if c.DecryptionDegradedReason() != "" {
			t.Errorf("consumer degraded too early at count=%d (single fails only count)", i)
		}
	}
	// Crossing the threshold degrades it.
	c.RecordDecryptFailure()
	if c.DecryptFailCount() != decryptFailDegradeThreshold {
		t.Errorf("DecryptFailCount = %d, want %d", c.DecryptFailCount(), decryptFailDegradeThreshold)
	}
	if c.DecryptionDegradedReason() != decryptStateFailed {
		t.Errorf("DecryptionDegradedReason = %q, want decrypt_failed after persistent failures", c.DecryptionDegradedReason())
	}
	if c.LastDecryptErrorClass() != decryptStateFailed {
		t.Errorf("LastDecryptErrorClass = %q, want decrypt_failed", c.LastDecryptErrorClass())
	}
	if c.LastDecryptErrorTime().IsZero() {
		t.Errorf("LastDecryptErrorTime must be stamped")
	}
}

func TestBus_OnDecryptFailure_RecordsOnMatchedConsumers(t *testing.T) {
	b := NewBus("test-app", "test-secret", "", nil, discardTestLogger())
	c := newConnWithRemoteSub(t, 1, "sub-x")
	b.hub.RegisterAndIsFirst(c)

	b.onDecryptFailure("sub-x")
	if c.DecryptFailCount() != 1 {
		t.Errorf("DecryptFailCount = %d, want 1", c.DecryptFailCount())
	}
	if c.DecryptState() != decryptStateFailed {
		t.Errorf("DecryptState = %q, want decrypt_failed", c.DecryptState())
	}
	// An unknown / empty subscription id is a harmless no-op (never panics).
	b.onDecryptFailure("sub-unknown")
	b.onDecryptFailure("")
}

// ---- E7: Hub.Consumers() populates decrypt observability ----

func TestHubConsumers_PopulatesDecryptObservability(t *testing.T) {
	hub := NewHub()

	// A healthy encrypted consumer (key available).
	ok := newConnWithRemoteSub(t, 1, "sub-ok")
	ok.SetDecryptState(decryptStateDecrypted)
	hub.RegisterAndIsFirst(ok)

	// A failing encrypted consumer.
	fail := newConnWithRemoteSub(t, 2, "sub-fail")
	fail.RecordDecryptFailure()
	hub.RegisterAndIsFirst(fail)

	// A plaintext consumer (no decrypt state at all).
	plain := newConnWithRemoteSub(t, 3, "sub-plain")
	hub.RegisterAndIsFirst(plain)

	byPID := map[int]protocol.ConsumerInfo{}
	for _, ci := range hub.Consumers() {
		byPID[ci.PID] = ci
	}

	if got := byPID[1]; got.DecryptState != decryptStateDecrypted || got.ResourceData != "decrypted" {
		t.Errorf("healthy consumer: decrypt_state=%q resource_data=%q, want decrypted/decrypted", got.DecryptState, got.ResourceData)
	}
	if got := byPID[2]; got.DecryptState != decryptStateFailed || got.ResourceData != "unavailable" {
		t.Errorf("failing consumer: decrypt_state=%q resource_data=%q, want decrypt_failed/unavailable", got.DecryptState, got.ResourceData)
	} else if got.LastDecryptError == nil || got.LastDecryptError.Count != 1 || got.LastDecryptError.Class != decryptStateFailed {
		t.Errorf("failing consumer last_decrypt_error = %+v, want class=decrypt_failed count=1", got.LastDecryptError)
	}
	if got := byPID[3]; got.DecryptState != "" || got.ResourceData != "" || got.LastDecryptError != nil {
		t.Errorf("plaintext consumer must carry no decrypt fields, got %+v", got)
	}
}
