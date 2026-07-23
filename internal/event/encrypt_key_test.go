// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"encoding/base64"
	"testing"
)

// ---- newEncryptKey (CLI-generated, CSPRNG, in-memory only, no
// --encrypt-key flag) ----

// TestNewEncryptKey_ReturnsNonEmptyString locks the most basic contract:
// callers (cmd/event/subscription/create.go's encrypted-create path) must
// never receive an empty key to inject into
// PayloadOptionsEncryptBuilder.EncryptKey.
func TestNewEncryptKey_ReturnsNonEmptyString(t *testing.T) {
	key, err := newEncryptKey()
	if err != nil {
		t.Fatalf("newEncryptKey: unexpected error: %v", err)
	}
	if key == "" {
		t.Fatal("newEncryptKey returned an empty string")
	}
}

// TestNewEncryptKey_DecodesTo32BytesOfEntropy locks the entropy/format
// contract from the task brief: "crypto/rand ≥32 bytes → base64-encode to a
// string". The SDK hashes whatever string is supplied
// (sha256.Sum256([]byte(encrypt_key)), verified in the 4c77bba clone's
// event/event.go) to derive the actual AES-256 key, so the only thing that
// matters about encrypt_key's shape is that it carries at least 32 bytes
// (256 bits) of real entropy before encoding — this test decodes the
// standard-base64 result back to bytes and checks the byte length directly,
// rather than just the string length, so it cannot be fooled by a
// non-base64 or short-but-padded string.
func TestNewEncryptKey_DecodesTo32BytesOfEntropy(t *testing.T) {
	key, err := newEncryptKey()
	if err != nil {
		t.Fatalf("newEncryptKey: unexpected error: %v", err)
	}
	raw, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		t.Fatalf("newEncryptKey result is not valid standard base64: %v (key=%q)", err, key)
	}
	if len(raw) < 32 {
		t.Errorf("decoded key length = %d bytes, want >= 32 (256 bits of CSPRNG entropy)", len(raw))
	}
}

// TestNewEncryptKey_DistinctAcrossCalls locks that this is a genuine
// CSPRNG-backed generator, not a fixed/static string: two independent calls
// must never collide (a fresh per-subscription
// key every Create).
func TestNewEncryptKey_DistinctAcrossCalls(t *testing.T) {
	const attempts = 8
	seen := make(map[string]bool, attempts)
	for i := 0; i < attempts; i++ {
		key, err := newEncryptKey()
		if err != nil {
			t.Fatalf("newEncryptKey: unexpected error on call %d: %v", i, err)
		}
		if seen[key] {
			t.Fatalf("newEncryptKey produced a duplicate value on call %d: %q", i, key)
		}
		seen[key] = true
	}
}

// TestNewEncryptKey_ExactlyRequestedByteCount locks that the encoded string
// always corresponds to exactly encryptKeyRandomBytes of CSPRNG output (not
// just "at least"), so a future accidental truncation is caught.
func TestNewEncryptKey_ExactlyRequestedByteCount(t *testing.T) {
	key, err := newEncryptKey()
	if err != nil {
		t.Fatalf("newEncryptKey: unexpected error: %v", err)
	}
	raw, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		t.Fatalf("newEncryptKey result is not valid standard base64: %v", err)
	}
	if len(raw) != encryptKeyRandomBytes {
		t.Errorf("decoded key length = %d bytes, want exactly %d", len(raw), encryptKeyRandomBytes)
	}
}

// ---- NewEncryptKey (exported cross-package entry point) ----

// TestNewEncryptKey_ExportedWrapper_ProducesSameShapeResult locks that the
// exported entry point cmd/event/subscription/create.go actually calls
// (package `subscription` cannot see the unexported newEncryptKey) delegates
// to the same generator and result shape — mirrors this package's own
// NewSubscriptionClient/newSubscriptionClient split (NewSubscriptionClient
// is the production/cross-package entry point; the lowercase name is the
// test-level/internal core).
func TestNewEncryptKey_ExportedWrapper_ProducesSameShapeResult(t *testing.T) {
	key, err := NewEncryptKey()
	if err != nil {
		t.Fatalf("NewEncryptKey: unexpected error: %v", err)
	}
	raw, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		t.Fatalf("NewEncryptKey result is not valid standard base64: %v", err)
	}
	if len(raw) != encryptKeyRandomBytes {
		t.Errorf("decoded key length = %d bytes, want exactly %d", len(raw), encryptKeyRandomBytes)
	}
}
