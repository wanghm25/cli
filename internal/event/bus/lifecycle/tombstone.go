// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package lifecycle

import (
	"sync"
	"time"
)

// TombstoneTTL bounds how long a TERMINAL phase (deleted / expired) is
// remembered for one remote_subscription_id, purely in-memory, so a
// late/out-of-order activated_v1 or updated_v1 arriving shortly after cannot
// resurrect it. A package var (not const) so tests shrink it; deliberately a
// fixed default rather than env-configurable — mirrors ExecutorWorkers/
// ExecutorSlots's own "control-plane housekeeping, not a scalable data path"
// rationale. (Named Tombstone* for continuity: a terminal phase IS the
// tombstone.)
var TombstoneTTL = 10 * time.Minute

// phaseEntry is one remote_subscription_id's last reduced Phase plus its
// expiry.
type phaseEntry struct {
	phase  Phase
	expiry time.Time
}

// phaseStore is the reducer's per-remote_subscription_id state: the last Phase
// each id folded into, TTL-bounded and purely in-memory (a bus restart clears
// it entirely, exactly like the executor's own pending/busy maps — nothing to
// persist or recover across a process boundary). It is the source of truth for
// terminal-state priority: once an id's phase is terminal (deleted/expired), a
// later activated/updated for it is dropped rather than allowed to revive it.
type phaseStore struct {
	mu      sync.Mutex
	entries map[string]phaseEntry
}

func newPhaseStore() *phaseStore {
	return &phaseStore{entries: make(map[string]phaseEntry)}
}

// record stores/refreshes id's phase with a fresh TTL. A resurrecting event
// that the reducer DROPS is never recorded, so a terminal phase's TTL is
// measured from the terminal event, not extended by the dropped resurrection.
func (s *phaseStore) record(id string, phase Phase) {
	if id == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[id] = phaseEntry{phase: phase, expiry: time.Now().Add(TombstoneTTL)}
}

// phaseOf returns id's current phase, or PhaseUnknown when it is unknown or its
// TTL has elapsed (lazily evicting the expired entry — bounded map growth
// without a separate sweep).
func (s *phaseStore) phaseOf(id string) Phase {
	if id == "" {
		return PhaseUnknown
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[id]
	if !ok {
		return PhaseUnknown
	}
	if time.Now().After(e.expiry) {
		delete(s.entries, id)
		return PhaseUnknown
	}
	return e.phase
}
