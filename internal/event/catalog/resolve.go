// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package catalog

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/larksuite/cli/errs"
)

// ResolvedEventKey is the result of resolving a caller-supplied EventKey
// string: either a legacy key (looked up as-is, IsRefined false) or a
// refined-subscription key materialized from a base key + template +
// selector value. Authority is deliberately not part of
// this shape — the caller binds it from the resolved --as identity;
// ResolveEventKey never touches identity.
type ResolvedEventKey struct {
	// Definition is the single registered base KeyDefinition: the legacy key
	// itself, or the refined-subscription base key (never a synthetic
	// per-instance definition).
	Definition *KeyDefinition
	// IsRefined is true when input materialized a refined-subscription
	// template; false for a legacy exact-match key.
	IsRefined bool
	// MaterializedKey is the canonical full key: the legacy input verbatim,
	// or "<base>/<path-segment>/<canonical-escaped-value>" for a refined key.
	MaterializedKey string
	// Template is the matched KeyTemplate; nil for a legacy key.
	Template *KeyTemplate
	// SelectorKey is the OAPI selector key (e.g. "chat_id"); empty for legacy.
	SelectorKey string
	// SelectorValue is the decoded (unescaped) selector value (e.g.
	// "oc_xxx"); empty for legacy.
	SelectorValue string
	// TargetResource is "<ResourceType>?<SelectorKey>=<query-escaped-value>";
	// empty for legacy.
	TargetResource string
}

// paramEventKey is the Param every invalid_argument produced by
// ResolveEventKey carries. The whole function operates on a single
// positional <EventKey> argument (as shown in `event consume <EventKey>` /
// `event subscription create <refined EventKey>`), so — per
// errs/ERROR_CONTRACT.md's "Validation parameters" section ("for positional
// arguments, use the canonical name without dashes") — Param is the
// canonical snake_case name for that argument, not its bracketed usage
// placeholder.
const paramEventKey = "event_key"

// ResolveEventKey resolves a caller-supplied EventKey string to either a
// legacy KeyDefinition (exact registry match) or a materialized
// refined-subscription key (base + template + selector value). It is a pure
// function: no registry mutation, no identity/authority binding
// (that is the caller's job once it has resolved --as). Lookup itself
// stays frozen and exact-match-only; this is the only refined-aware entry
// point.
func ResolveEventKey(input string) (ResolvedEventKey, error) {
	// Step 1: exact match first. Legacy keys accept ONLY an exact match —
	// they never enter the split path below, even when input happens to
	// look like "<legacy-key>/something". A refined base key hit here (no
	// template segment) is R1: it may only be used with `event
	// list`/`event schema`, never consume/create.
	if def, ok := Lookup(input); ok {
		if !def.RefinedSubscription {
			return ResolvedEventKey{Definition: def, IsRefined: false, MaterializedKey: input}, nil
		}
		return ResolvedEventKey{}, bareRefinedBaseError(input)
	}

	// Step 2: split only after the exact match fails. The split base MUST
	// itself resolve to a refined definition — a legacy key with a suffix
	// (e.g. "im.message.receive_v1/foo/bar") and an unregistered base both
	// fail here, before any template matching is attempted.
	parts := strings.SplitN(input, "/", 3)
	base := parts[0]

	baseDef, ok := Lookup(base)
	if !ok {
		return ResolvedEventKey{}, errs.NewValidationError(errs.SubtypeInvalidArgument,
			"unknown EventKey %q (base %q is not registered)", input, base).
			WithParam(paramEventKey).
			WithHint("run `lark-cli event list` to see available keys")
	}
	if !baseDef.RefinedSubscription {
		return ResolvedEventKey{}, errs.NewValidationError(errs.SubtypeInvalidArgument,
			"legacy EventKey %q does not accept a path suffix", input).
			WithParam(paramEventKey).
			WithHint("legacy EventKey %s only accepts an exact match; run it as `%s` with no suffix", base, base)
	}
	if len(parts) < 3 {
		// Refined base key with a "/" but no complete <path-segment>/<value>
		// pair (e.g. "base", "base/", or "base/seg" with nothing after it).
		// Still not materialized — same guidance as the fully bare base key.
		return ResolvedEventKey{}, bareRefinedBaseError(base)
	}

	seg, rawValue := parts[1], parts[2]

	tmpl := matchTemplate(baseDef.KeyTemplates, seg)
	if tmpl == nil {
		return ResolvedEventKey{}, errs.NewValidationError(errs.SubtypeInvalidArgument,
			"EventKey %s has no template for path segment %q", base, seg).
			WithParam(paramEventKey).
			WithHint("available path segments for %s: %s; run `lark-cli event schema %s --json` for the full templates",
				base, strings.Join(templatePathSegments(baseDef.KeyTemplates), ", "), base)
	}

	// Fixed-value templates (e.g. owner/me) compare the raw extracted value
	// verbatim, before URL decoding — the fixed value itself is a plain
	// literal, never percent-encoded, so this ordering deliberately matches
	// before URL decoding/normalization.
	if tmpl.FixedValue != "" && rawValue != tmpl.FixedValue {
		return ResolvedEventKey{}, errs.NewValidationError(errs.SubtypeInvalidArgument,
			"EventKey %s/%s requires the fixed value %q; got %q", base, seg, tmpl.FixedValue, rawValue).
			WithParam(paramEventKey).
			WithHint("use the fixed value: `%s/%s/%s`", base, seg, tmpl.FixedValue)
	}

	decoded, badValueReason := decodeSelectorValue(rawValue)
	if badValueReason != "" {
		return ResolvedEventKey{}, errs.NewValidationError(errs.SubtypeInvalidArgument,
			"EventKey %s/%s value %q is invalid: %s", base, seg, rawValue, badValueReason).
			WithParam(paramEventKey).
			WithHint("percent-encode any literal '%%' or '/' inside the selector value (e.g. a literal '/' as %%2F); see `lark-cli event schema %s --json`", base)
	}
	if decoded == "" {
		return ResolvedEventKey{}, errs.NewValidationError(errs.SubtypeInvalidArgument,
			"EventKey %s/%s requires a non-empty selector value", base, seg).
			WithParam(paramEventKey).
			WithHint("pass a value, e.g. `%s`", tmpl.Example)
	}

	return ResolvedEventKey{
		Definition:      baseDef,
		IsRefined:       true,
		MaterializedKey: base + "/" + seg + "/" + url.PathEscape(decoded),
		Template:        tmpl,
		SelectorKey:     tmpl.SelectorKey,
		SelectorValue:   decoded,
		TargetResource:  baseDef.ResourceType + "?" + tmpl.SelectorKey + "=" + url.QueryEscape(decoded),
	}, nil
}

// bareRefinedBaseError implements the bare-base-key rule: a refined-subscription base key with
// no (complete) template segment can only be used with `event
// list`/`event schema`; consume/create must receive a materialized key such
// as one of its KeyTemplates' Example.
func bareRefinedBaseError(base string) error {
	return errs.NewValidationError(errs.SubtypeInvalidArgument,
		"EventKey %q is a refined-subscription base key and cannot be consumed or created directly", base).
		WithParam(paramEventKey).
		WithHint("run `lark-cli event schema %s --json` to see the available templates, then pass a materialized key such as one of key_templates[].example", base)
}

// matchTemplate finds the KeyTemplate whose PathSegment exactly matches seg.
// Returns a pointer into the definition's own KeyTemplates slice (never a
// copy) so the caller can inspect e.g. AuthTypes on the stored value.
//
// RegisterKey (frozen, an earlier task) does not enforce PathSegment
// uniqueness across one base key's templates, so a malformed catalog could
// register two templates sharing a PathSegment; this deterministically
// returns the first declared match rather than behaving ambiguously.
func matchTemplate(templates []KeyTemplate, seg string) *KeyTemplate {
	for i := range templates {
		if templates[i].PathSegment == seg {
			return &templates[i]
		}
	}
	return nil
}

func templatePathSegments(templates []KeyTemplate) []string {
	segs := make([]string, len(templates))
	for i, t := range templates {
		segs[i] = t.PathSegment
	}
	return segs
}

// decodeSelectorValue applies the frozen URL discipline:
// url.PathUnescape EXACTLY ONCE on raw, rejecting a malformed %
// escape and rejecting a literal (unescaped) '/' in raw — which would
// otherwise silently change how the EventKey is segmented instead of being
// rejected outright. A '/' produced BY decoding (e.g. raw "%2F") is fine and
// intentional: it is how a literal '/' travels safely inside one path
// segment. On success reason is "" and decoded is the logical selector
// value; on failure decoded is "" and reason explains why. This returns a
// plain string reason rather than an error because it is a private,
// in-package helper — every path that surfaces to the caller wraps the
// reason into a typed *errs.ValidationError itself.
func decodeSelectorValue(raw string) (decoded string, reason string) {
	if strings.Contains(raw, "/") {
		return "", "contains an unescaped '/' that would change how the EventKey is segmented"
	}
	decoded, err := url.PathUnescape(raw)
	if err != nil {
		return "", fmt.Sprintf("invalid percent-escape (%s)", err)
	}
	return decoded, ""
}
