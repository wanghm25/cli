// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package session

import "sync/atomic"

// SourceEpoch is a monotonic counter identifying the current WS source
// generation. Each time the source connection becomes ready under a NEW
// connection_id (the first ready, or a reconnect that rotated it), the epoch
// advances. A per-consumer BindUser captures the epoch it started under; if the
// epoch has advanced by the time that (possibly slow) bind completes, a newer
// generation has superseded it and its result must be dropped rather than
// overwrite the newer generation's session state — otherwise a slow bind from a
// dead connection could clobber a fresh binding.
//
// The zero value is a usable epoch 0 ("no generation ready yet").
type SourceEpoch struct {
	v atomic.Uint64
}

// Advance bumps to the next generation and returns it. Call when the source
// becomes ready under a connection_id different from the current one.
func (e *SourceEpoch) Advance() uint64 { return e.v.Add(1) }

// Current returns the current generation (0 = none ready yet).
func (e *SourceEpoch) Current() uint64 { return e.v.Load() }

// IsCurrent reports whether epoch is still the live generation — i.e. no newer
// source-ready has advanced past it since it was captured.
func (e *SourceEpoch) IsCurrent(epoch uint64) bool { return e.v.Load() == epoch }
