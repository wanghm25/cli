// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package catalog

import (
	"net/url"
	"strings"
)

// ReverseResolve reconstructs the canonical EventKey from a remote
// subscription's event_type + target_resource — the inverse of the
// TargetResource / MaterializedKey that ResolveEventKey produces. ok is false
// when the pair cannot be reconstructed to exactly one registered EventKey: an
// explicit "unavailable" that a caller surfaces (e.g. status/list leaving the
// event_key blank) instead of fabricating a key. ReverseResolve never guesses —
// an ambiguous or unmatched pair is always ok=false.
//
// A legacy subscription carries an empty target_resource and reverses to its
// own Key when exactly one non-refined definition declares that event_type. A
// refined subscription carries "<resource_type>?<selector_key>=<value>" and
// reverses to "<base>/<path-segment>/<PathEscape(value)>" — byte-for-byte the
// MaterializedKey ResolveEventKey would build — when exactly one refined
// definition and template match the event_type + resource_type + selector.
func ReverseResolve(eventType, targetResource string) (eventKey string, ok bool) {
	if strings.TrimSpace(targetResource) == "" {
		return reverseLegacy(eventType)
	}
	return reverseRefined(eventType, targetResource)
}

// reverseLegacy maps an event_type with no target_resource back to a legacy
// EventKey. It succeeds only when exactly one non-refined definition declares
// the event_type, so an event_type shared by several keys is reported
// unavailable rather than resolved arbitrarily.
func reverseLegacy(eventType string) (string, bool) {
	var match string
	count := 0
	for _, def := range ListAll() {
		if def.EventType == eventType && !def.RefinedSubscription {
			match = def.Key
			count++
		}
	}
	if count != 1 {
		return "", false
	}
	return match, true
}

// reverseRefined maps an event_type + target_resource back to a materialized
// refined EventKey. It succeeds only when exactly one (refined definition,
// template) pair matches the event_type, resource_type, and selector key — and,
// for a fixed-value template, the selector value.
func reverseRefined(eventType, targetResource string) (string, bool) {
	resourceType, selectorKey, selectorValue, ok := parseSingleSelectorTarget(targetResource)
	if !ok || selectorValue == "" {
		return "", false
	}

	var match string
	count := 0
	for _, def := range ListAll() {
		if def.EventType != eventType || !def.RefinedSubscription || def.ResourceType != resourceType {
			continue
		}
		for i := range def.KeyTemplates {
			tmpl := def.KeyTemplates[i]
			if tmpl.SelectorKey != selectorKey {
				continue
			}
			if tmpl.FixedValue != "" && tmpl.FixedValue != selectorValue {
				continue
			}
			match = def.Key + "/" + tmpl.PathSegment + "/" + url.PathEscape(selectorValue)
			count++
		}
	}
	if count != 1 {
		return "", false
	}
	return match, true
}

// parseSingleSelectorTarget splits "<resource_type>?<key>=<value>" into its
// decoded parts. It requires the '?' delimiter and EXACTLY ONE selector pair —
// anything else (no query, multiple selectors, a repeated key, or an
// unparseable query) is reported ok=false, since ResolveEventKey only ever
// emits a single-selector target_resource.
func parseSingleSelectorTarget(target string) (resourceType, selectorKey, selectorValue string, ok bool) {
	i := strings.IndexByte(target, '?')
	if i < 0 {
		return "", "", "", false
	}
	resourceType = target[:i]
	values, err := url.ParseQuery(target[i+1:])
	if err != nil || len(values) != 1 {
		return "", "", "", false
	}
	for k, vs := range values {
		if len(vs) != 1 {
			return "", "", "", false
		}
		selectorKey = k
		selectorValue = vs[0]
	}
	return resourceType, selectorKey, selectorValue, true
}
