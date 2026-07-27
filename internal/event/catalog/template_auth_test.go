// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package catalog

import (
	"errors"
	"strings"
	"testing"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/core"
)

// resolveOwnerMeForAuthCheck / resolveChatIDForAuthCheck give
// TestCheckTemplateAuthTypes_* a real matched ResolvedEventKey.Template
// (registerResolveFixtures, resolve_test.go) rather than a hand-built one --
// CheckTemplateAuthTypes is the actual tier-2 write-safety gate shared
// by both `event subscription create` (cmd/event/subscription/create.go)
// and the refined `event consume` startup chain
// (cmd/event/consume.go:runRefinedConsume), so it must be exercised against
// the same fixture shape those callers see.
func resolveOwnerMeForAuthCheck(t *testing.T) ResolvedEventKey {
	t.Helper()
	registerResolveFixtures(t)
	r, err := ResolveEventKey("im.message.created_v1/owner/me")
	if err != nil {
		t.Fatalf("ResolveEventKey: unexpected error: %v", err)
	}
	return r
}

func resolveChatIDForAuthCheck(t *testing.T) ResolvedEventKey {
	t.Helper()
	registerResolveFixtures(t)
	r, err := ResolveEventKey("im.message.created_v1/chat-id/oc_aaa")
	if err != nil {
		t.Fatalf("ResolveEventKey: unexpected error: %v", err)
	}
	return r
}

// TestCheckTemplateAuthTypes_EmptyAuthTypes_ReturnsNil locks the "empty =
// no additional restriction" convention (mirrors KeyDefinition.AuthTypes'
// own doc comment) for a hand-built Template that declares no AuthTypes at
// all -- never reachable through the real owner/me fixture (which always
// sets AuthTypes), so this constructs its own minimal ResolvedEventKey.
func TestCheckTemplateAuthTypes_EmptyAuthTypes_ReturnsNil(t *testing.T) {
	resolved := ResolvedEventKey{
		MaterializedKey: "some.key_v1/id/abc",
		Template:        &KeyTemplate{Template: "some.key_v1/id/{id}", AuthTypes: nil},
	}
	if err := CheckTemplateAuthTypes(core.AsBot, resolved); err != nil {
		t.Errorf("empty Template.AuthTypes must mean no restriction, got: %v", err)
	}
	if err := CheckTemplateAuthTypes(core.AsUser, resolved); err != nil {
		t.Errorf("empty Template.AuthTypes must mean no restriction, got: %v", err)
	}
}

// TestCheckTemplateAuthTypes_IdentityIsMember_ReturnsNil covers the
// chat-id template, which accepts both user and bot.
func TestCheckTemplateAuthTypes_IdentityIsMember_ReturnsNil(t *testing.T) {
	resolved := resolveChatIDForAuthCheck(t)
	for _, id := range []core.Identity{core.AsUser, core.AsBot} {
		if err := CheckTemplateAuthTypes(id, resolved); err != nil {
			t.Errorf("--as %s: unexpected error on chat-id template: %v", id, err)
		}
	}
}

// TestCheckTemplateAuthTypes_IdentityNotMember_ReturnsTypedFailedPrecondition
// is the REQUIRED write-safety case: the owner/me
// template only supports user, so a bot identity must be rejected with the
// typed error -- Subtype failed_precondition, Param "--as", Hint naming
// the allowed identity -- BEFORE any caller ever reaches a remote write.
func TestCheckTemplateAuthTypes_IdentityNotMember_ReturnsTypedFailedPrecondition(t *testing.T) {
	resolved := resolveOwnerMeForAuthCheck(t)

	err := CheckTemplateAuthTypes(core.AsBot, resolved)
	if err == nil {
		t.Fatal("expected an error for --as bot on the user-only owner/me template, got nil")
	}
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	if ve.Param != "--as" {
		t.Errorf("Param = %q, want --as", ve.Param)
	}
	if !strings.Contains(ve.Hint, "user") {
		t.Errorf("Hint = %q, want it to name the allowed identity (user)", ve.Hint)
	}
}

// TestCheckTemplateAuthTypes_OwnerMeTemplate_AllowsUser is the positive
// counterpart: the one identity owner/me does allow must never be rejected.
func TestCheckTemplateAuthTypes_OwnerMeTemplate_AllowsUser(t *testing.T) {
	resolved := resolveOwnerMeForAuthCheck(t)
	if err := CheckTemplateAuthTypes(core.AsUser, resolved); err != nil {
		t.Errorf("unexpected error for --as user on owner/me: %v", err)
	}
}
