// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"slices"
	"strings"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/core"
)

// CheckTemplateAuthTypes enforces the second, stricter
// identity tier for a refined EventKey with a matched KeyTemplate: even when
// the resolved identity is one the whole base key accepts (the caller's own,
// separate check against ResolvedEventKey.Definition.AuthTypes — e.g.
// cmd/event/consume.go's resolveIdentity / cmd/event/subscription/create.go's
// tier-1 check), the SPECIFIC matched template may accept a narrower set
// (e.g. "owner/me" is user-only even though its base key
// im.message.created_v1 allows user+bot). Empty Template.AuthTypes means "no
// additional restriction" (mirrors KeyDefinition.AuthTypes' own "empty = no
// identity required" convention). Violated -> typed failed_precondition
// (never a silent identity switch — AGENTS.md "no silent downgrade"),
// naming the allowed identities so the caller can retry explicitly with
// --as.
//
// EXPORTED and shared by both refined write paths
// that must reject a template-narrowed identity BEFORE ever reaching a
// remote write: `event subscription create`
// (cmd/event/subscription/create.go, tier 2) and the refined
// `event consume` startup chain (cmd/event/consume.go's runRefinedConsume,
// called after resolveIdentity and BEFORE consume.RunRefined's
// Plan/Apply). Originated in create.go; moved here so the check is
// identical, not independently maintained, across both call sites.
func CheckTemplateAuthTypes(identity core.Identity, resolved ResolvedEventKey) error {
	allowed := resolved.Template.AuthTypes
	if len(allowed) == 0 {
		return nil
	}
	if slices.Contains(allowed, string(identity)) {
		return nil
	}
	return errs.NewValidationError(errs.SubtypeFailedPrecondition,
		"EventKey template %s only supports identity: %s; resolved identity is %q",
		resolved.Template.Template, strings.Join(allowed, ", "), identity).
		WithParam("--as").
		WithHint("retry with --as %s", strings.Join(allowed, " or "))
}
