// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"crypto/rand"
	"encoding/base64"

	"github.com/larksuite/cli/errs"
)

// encryptKeyRandomBytes is the number of OS-CSPRNG bytes read to build one
// fresh per-subscription encrypt_key (design spec §4.7 "密钥创建与来源";
// task-E-design-note.md's task E2). The candidate SDK's EventDecrypt derives
// the actual AES-256 key via `sha256.Sum256([]byte(encrypt_key))` (verified
// in the 4c77bba clone's event/event.go) — so encrypt_key itself has no
// fixed platform format, only entropy matters, and the SDK never decodes it
// back to raw bytes. 32 bytes (256 bits) matches the AES-256 key size the
// hash produces and is this repo's own bar for a generated secret; there is
// no reason to exceed it since the hash saturates at 256 bits regardless of
// how much more input entropy is supplied.
const encryptKeyRandomBytes = 32

// newEncryptKey generates one fresh, high-entropy per-subscription
// encrypt_key using the OS CSPRNG (crypto/rand — never math/rand, and never
// derived from any user/CLI-supplied input): spec §4.7 is explicit that the
// CLI does not accept `--encrypt-key`; a bare refined base key over resource
// data cannot ship "bring your own key" and this whole module's fail-closed
// posture depends on every encrypted Subscription's key being CLI-generated
// and never observed by a human or a script.
//
// The raw bytes are standard-base64-encoded purely to obtain a safe,
// printable Go string for PayloadOptionsEncryptBuilder.EncryptKey — see the
// encryptKeyRandomBytes doc comment above for why the encoding choice itself
// carries no cryptographic significance. The raw random buffer is zeroed
// before returning (best-effort defense in depth; the resulting Go string is
// a separate heap allocation Go cannot zero in place without unsafe, so this
// does not eliminate all traces, only the one buffer this function fully
// controls).
//
// RED LINE (task-E-design-note.md's "RED LINES" + this task's own): the
// caller must never log, persist, print, or return this value beyond the
// single Create call it is injected into — see
// cmd/event/subscription/create.go's doCreateSubscription (the sole
// production call site) and this package's own redaction-oriented tests in
// cmd/event/subscription/create_test.go.
func newEncryptKey() (string, error) {
	buf := make([]byte, encryptKeyRandomBytes)
	if _, err := rand.Read(buf); err != nil {
		// rand.Read only fails if the OS CSPRNG itself is unavailable/broken
		// (not a case any caller can meaningfully retry around) — surfaced as
		// a typed internal error rather than a bare error so it still carries
		// this CLI's usual error envelope; the underlying OS error message is
		// passed through (it never contains key material — rand.Read has not
		// produced any bytes worth protecting when it errors).
		return "", errs.NewInternalError(errs.SubtypeUnknown,
			"failed to generate a subscription encrypt_key from the OS CSPRNG: %v", err)
	}
	key := base64.StdEncoding.EncodeToString(buf)
	for i := range buf {
		buf[i] = 0
	}
	return key, nil
}

// NewEncryptKey is newEncryptKey's exported, cross-package entry point —
// cmd/event/subscription/create.go (package `subscription`) cannot see the
// unexported name, the same reason NewSubscriptionClient exists alongside
// this file's sibling newSubscriptionClient (subscription_client.go). Tests
// exercise the unexported newEncryptKey directly (this package) since that
// is where the actual generation logic lives; NewEncryptKey is a pure
// pass-through with no behavior of its own to diverge.
func NewEncryptKey() (string, error) {
	return newEncryptKey()
}
