// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package model

import "errors"

// RemoteSubscriptionID identifies a remote Lark Subscription record (the
// management-side subscription_id). Its zero value is invalid on purpose:
// callers construct one through NewRemoteSubscriptionID, which rejects the
// empty string, so the non-empty-ID invariant is enforced once at construction
// and every later holder (the Gateway/Receipt in subsequent PRs) can rely on
// it without re-checking.
type RemoteSubscriptionID string

// ErrEmptyRemoteSubscriptionID is returned by NewRemoteSubscriptionID when its
// input is empty.
var ErrEmptyRemoteSubscriptionID = errors.New("remote subscription id must not be empty")

// NewRemoteSubscriptionID returns id as a RemoteSubscriptionID, or
// ErrEmptyRemoteSubscriptionID if id is empty.
func NewRemoteSubscriptionID(id string) (RemoteSubscriptionID, error) {
	if id == "" {
		return "", ErrEmptyRemoteSubscriptionID
	}
	return RemoteSubscriptionID(id), nil
}

// String returns the underlying id.
func (id RemoteSubscriptionID) String() string { return string(id) }

// IsZero reports whether id is the invalid zero value.
func (id RemoteSubscriptionID) IsZero() bool { return id == "" }
