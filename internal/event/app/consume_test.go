// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"errors"
	"testing"

	"github.com/larksuite/cli/internal/core"
	event "github.com/larksuite/cli/internal/event"
)

// TestDescribeConsume_Classification locks the three-way setup classification
// that replaces the command's inline isRefined/dry-run branching.
func TestDescribeConsume_Classification(t *testing.T) {
	refined := event.ResolvedEventKey{IsRefined: true}
	legacy := event.ResolvedEventKey{IsRefined: false}

	cases := []struct {
		name     string
		resolved event.ResolvedEventKey
		dryRun   bool
		want     SetupKind
	}{
		{"refined real run", refined, false, SetupRemoteSubscription},
		{"refined dry-run stays remote-subscription", refined, true, SetupRemoteSubscription},
		{"legacy real run", legacy, false, SetupLegacy},
		{"legacy dry-run is no-setup", legacy, true, SetupNone},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := DescribeConsume(c.resolved, c.dryRun).Kind; got != c.want {
				t.Errorf("Kind = %v, want %v", got, c.want)
			}
		})
	}
}

// TestInvocationDescriptor_Risk locks the argument-aware risk policy: only a
// remote-subscription setup is write; legacy and no-setup are read.
func TestInvocationDescriptor_Risk(t *testing.T) {
	cases := []struct {
		kind SetupKind
		want string
	}{
		{SetupRemoteSubscription, core.RiskWrite},
		{SetupLegacy, core.RiskRead},
		{SetupNone, core.RiskRead},
	}
	for _, c := range cases {
		if got := (InvocationDescriptor{Kind: c.kind}).Risk(); got != c.want {
			t.Errorf("Risk(%v) = %q, want %q", c.kind, got, c.want)
		}
	}
}

// recordingSetup records which strategy ConsumeUseCase.Run dispatched to and
// returns a canned error, so a test asserts both the routing and error
// propagation.
type recordingSetup struct {
	called string
	err    error
}

func (r *recordingSetup) RunRemoteSubscription(context.Context) error {
	r.called = "remote"
	return r.err
}
func (r *recordingSetup) RunLegacy(context.Context) error  { r.called = "legacy"; return r.err }
func (r *recordingSetup) RunNoSetup(context.Context) error { r.called = "none"; return r.err }

// TestConsumeUseCase_Run_Dispatch locks that each SetupKind routes to its
// matching strategy and the strategy's error propagates unchanged.
func TestConsumeUseCase_Run_Dispatch(t *testing.T) {
	sentinel := errors.New("boom")
	cases := []struct {
		kind SetupKind
		want string
	}{
		{SetupRemoteSubscription, "remote"},
		{SetupLegacy, "legacy"},
		{SetupNone, "none"},
	}
	for _, c := range cases {
		t.Run(c.want, func(t *testing.T) {
			setup := &recordingSetup{err: sentinel}
			err := NewConsumeUseCase(setup).Run(context.Background(), InvocationDescriptor{Kind: c.kind})
			if setup.called != c.want {
				t.Errorf("dispatched to %q, want %q", setup.called, c.want)
			}
			if !errors.Is(err, sentinel) {
				t.Errorf("err = %v, want the strategy error propagated", err)
			}
		})
	}
}
