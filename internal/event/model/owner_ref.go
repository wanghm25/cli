// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package model

// OwnerRef is an immutable descriptor of the identity that owns a subscription
// or a consumer. It is a value object: two OwnerRefs with the same fields are
// interchangeable, and holders must treat it as read-only. Profile is
// display/diagnostics only and never participates in identity or equality.
type OwnerRef struct {
	// AppID is the Lark app the owner acts as.
	AppID string
	// Identity is the acting identity tier, "user" or "bot".
	Identity string
	// UserOpenID is the open_id of the acting user; empty for a bot identity.
	UserOpenID string
	// Profile is a human-readable label for logs and diagnostics only.
	Profile string
}
