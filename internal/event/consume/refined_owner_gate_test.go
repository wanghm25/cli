// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package consume

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/event/model"
	"github.com/larksuite/cli/internal/event/protocol"
	"github.com/larksuite/cli/internal/event/session"
	subown "github.com/larksuite/cli/internal/event/subscription"
)

// ---- owner==current gate BEFORE the remote write (fail closed, zero write) ----

// TestRefinedOwnerGate_Decision locks the gate's DECISION: it reuses the shared
// session.Gate policy unchanged (a user owner admits only when it matches the
// freshly resolved current identity; a bot/legacy owner is never gated; an
// unresolved current fails closed). Only WHERE consume consults it moved.
func TestRefinedOwnerGate_Decision(t *testing.T) {
	resolved := refinedFixture()
	userOwner := model.OwnerRef{AppID: "cli_x", UserOpenID: "ou_owner", Profile: "owner-profile"}
	botOwner := model.OwnerRef{AppID: "cli_x", UserOpenID: "", Profile: "bot-profile"}

	admit := func(app, user string) func() (session.CurrentIdentity, error) {
		return func() (session.CurrentIdentity, error) {
			return session.CurrentIdentity{AppID: app, UserOpenID: user}, nil
		}
	}
	failResolve := func() (session.CurrentIdentity, error) {
		return session.CurrentIdentity{}, errors.New("config unreadable")
	}

	t.Run("user owner matches current -> admits (nil)", func(t *testing.T) {
		if err := refinedOwnerGate(resolved, core.AsUser, userOwner, admit("cli_x", "ou_owner")); err != nil {
			t.Fatalf("owner==current must admit, got %v", err)
		}
	})

	t.Run("user owner != current -> fail closed (stale)", func(t *testing.T) {
		err := refinedOwnerGate(resolved, core.AsUser, userOwner, admit("cli_x", "ou_active"))
		var ve *errs.ValidationError
		if !errors.As(err, &ve) {
			t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
		}
		if ve.Subtype != errs.SubtypeFailedPrecondition {
			t.Errorf("subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
		}
		if !strings.Contains(ve.Error(), "owner-profile") {
			t.Errorf("Error() = %q, want it to name the selected owner", ve.Error())
		}
	})

	t.Run("bot owner is never identity-gated -> admits even when current differs", func(t *testing.T) {
		if err := refinedOwnerGate(resolved, core.AsBot, botOwner, admit("cli_x", "ou_whoever")); err != nil {
			t.Fatalf("a bot/legacy owner must never be identity-gated, got %v", err)
		}
	})

	t.Run("current unresolved -> fail closed (unresolved)", func(t *testing.T) {
		err := refinedOwnerGate(resolved, core.AsUser, userOwner, failResolve)
		var ve *errs.ValidationError
		if !errors.As(err, &ve) {
			t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
		}
		if ve.Subtype != errs.SubtypeFailedPrecondition {
			t.Errorf("subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
		}
	})
}

// TestRunRefinedChain_OwnerNotCurrent_FailsClosedBeforeApply_ZeroRemoteWrites is
// the #2 guard: a refined consume whose owner is NOT the active identity (a
// non-active-user --profile) fails closed with a typed error BEFORE Apply, so the
// real subscription.Controller over a fake gateway records ZERO Create/Reactivate.
// The remote write no longer precedes the gate.
func TestRunRefinedChain_OwnerNotCurrent_FailsClosedBeforeApply_ZeroRemoteWrites(t *testing.T) {
	var applyCalled, startBusCalled, helloCalled bool
	gw := &fakeGateway{} // if Apply were ever reached it would write; it must not be
	controller := subown.NewController(gw)
	resolved := refinedFixture()
	owner := model.OwnerRef{AppID: "cli_x", UserOpenID: "ou_owner", Profile: "owner-profile"}
	// current is a DIFFERENT user than the selected owner (owner != current).
	staleCurrent := func() (session.CurrentIdentity, error) {
		return session.CurrentIdentity{AppID: "cli_x", UserOpenID: "ou_active"}, nil
	}
	opts := RefinedOptions{ErrOut: io.Discard, Out: io.Discard, Identity: core.AsUser, OwnerRef: owner, Controller: controller}

	deps := refinedDeps{
		probe: func(context.Context) error { return nil },
		plan: func(context.Context) (subown.SubscriptionPlan, error) {
			// A writable plan that WOULD Create — the gate must stop it first.
			return subown.SubscriptionPlan{Action: subown.ActionCreate}, nil
		},
		gate: func(ctx context.Context) error {
			return refinedOwnerGate(resolved, opts.Identity, opts.OwnerRef, staleCurrent)
		},
		apply: func(ctx context.Context, plan subown.SubscriptionPlan) (string, bool, error) {
			applyCalled = true
			receipt, err := controller.Apply(ctx, plan, subown.ConsumeBootstrap, refinedRequest(opts, "im.message.created_v1", "im.message?chat_id=oc_aaa"))
			if err != nil {
				return "", false, err
			}
			return receipt.RemoteID.String(), receipt.CreatedByAttempt, nil
		},
		startBus: func(context.Context) (net.Conn, error) { startBusCalled = true; return nil, nil },
		hello: func(context.Context, net.Conn, string) (*protocol.HelloAck, *bufio.Reader, error) {
			helloCalled = true
			return nil, nil, nil
		},
	}

	err := runRefinedChain(context.Background(), resolved, opts, deps)
	if err == nil {
		t.Fatal("expected a typed fail-closed error when owner != current")
	}
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	if applyCalled || startBusCalled || helloCalled {
		t.Error("the owner gate must short-circuit before apply/startBus/hello")
	}
	if gw.createCalls != 0 || gw.reactivateCalls != 0 {
		t.Errorf("ZERO remote writes required before the gate admits (create=%d reactivate=%d)", gw.createCalls, gw.reactivateCalls)
	}
}
