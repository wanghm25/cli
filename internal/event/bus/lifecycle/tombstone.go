// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package lifecycle

import (
	"sync"
	"time"
)

// TombstoneTTL bounds how long a deleted remote_subscription_id is remembered,
// purely in-memory, so a late/out-of-order activated_v1 or updated_v1 for the
// SAME id arriving shortly after a deleted_v1 cannot resurrect it. A package
// var (not const) so tests shrink it; deliberately a fixed default rather than
// env-configurable — mirrors ExecutorWorkers/ExecutorSlots's own "control-plane
// housekeeping, not a scalable data path" rationale.
var TombstoneTTL = 10 * time.Minute

// tombstoneStore is a TTL in-memory map[remote_subscription_id]expiry. Purely
// in-memory: a bus restart clears it entirely, exactly like the executor's own
// pending/busy maps — there is nothing to persist or recover across a process
// boundary here (the no-persistence rule applies equally to this bookkeeping).
type tombstoneStore struct {
	mu     sync.Mutex
	expiry map[string]time.Time
}

func newTombstoneStore() *tombstoneStore {
	return &tombstoneStore{expiry: make(map[string]time.Time)}
}

// mark records/refreshes a live tombstone for remoteSubID (called on every
// deleted_v1, hit or miss — both table columns keep a TTL in-memory tombstone).
func (t *tombstoneStore) mark(remoteSubID string) {
	if remoteSubID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.expiry[remoteSubID] = time.Now().Add(TombstoneTTL)
}

// isLive reports whether remoteSubID currently has an unexpired tombstone,
// lazily evicting an expired entry it happens to find (bounded map growth
// without a separate background sweep).
func (t *tombstoneStore) isLive(remoteSubID string) bool {
	if remoteSubID == "" {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	exp, ok := t.expiry[remoteSubID]
	if !ok {
		return false
	}
	if time.Now().After(exp) {
		delete(t.expiry, remoteSubID)
		return false
	}
	return true
}
