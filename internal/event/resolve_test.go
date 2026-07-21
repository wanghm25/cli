// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"errors"
	"strings"
	"testing"

	"github.com/larksuite/cli/errs"
)

// registerResolveFixtures registers, for the duration of t, a legacy key
// (im.message.receive_v1) and the im.message.created_v1 refined-subscription
// base key with its two design-spec §2.2 templates (chat-id/{chat_id} and the
// fixed-value owner/me). internal/event cannot import events/refined (that
// package imports internal/event, not the reverse), so this mirrors
// events/refined/refined_keys_mock.json inline rather than sharing it.
func registerResolveFixtures(t *testing.T) {
	t.Helper()

	RegisterKey(KeyDefinition{
		Key:       "im.message.receive_v1",
		EventType: "im.message.receive_v1",
		Schema:    nativeSchema(),
	})
	t.Cleanup(func() { UnregisterKeyForTest("im.message.receive_v1") })

	RegisterKey(KeyDefinition{
		Key:                 "im.message.created_v1",
		EventType:           "im.message.created_v1",
		Schema:              nativeSchema(),
		RefinedSubscription: true,
		ResourceType:        "im.message",
		AuthTypes:           []string{"user", "bot"},
		KeyTemplates: []KeyTemplate{
			{
				Template:    "im.message.created_v1/chat-id/{chat_id}",
				Example:     "im.message.created_v1/chat-id/oc_9f3b1c2d8a",
				Description: "listen to new messages in a specific chat",
				SelectorKey: "chat_id",
				PathSegment: "chat-id",
				AuthTypes:   []string{"user", "bot"},
			},
			{
				Template:    "im.message.created_v1/owner/me",
				Example:     "im.message.created_v1/owner/me",
				Description: "listen to messages visible to the current user; me resolves from subscription identity",
				SelectorKey: "owner",
				PathSegment: "owner",
				FixedValue:  "me",
				AuthTypes:   []string{"user"},
			},
		},
	})
	t.Cleanup(func() { UnregisterKeyForTest("im.message.created_v1") })
}

// assertInvalidArgument asserts err is a typed *errs.ValidationError with
// Subtype invalid_argument and Param "event_key" (errs/ERROR_CONTRACT.md §
// "Validation parameters": positional arguments use the canonical name
// without dashes). When hintSubstr is non-empty it must appear in Hint.
func assertInvalidArgument(t *testing.T, err error, hintSubstr string) *errs.ValidationError {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeInvalidArgument {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeInvalidArgument)
	}
	if ve.Param != "event_key" {
		t.Errorf("Param = %q, want %q", ve.Param, "event_key")
	}
	if hintSubstr != "" && !strings.Contains(ve.Hint, hintSubstr) {
		t.Errorf("Hint = %q, want substring %q", ve.Hint, hintSubstr)
	}
	return ve
}

// TestResolveEventKey is the design spec §2.3 task-brief case list, verbatim.
func TestResolveEventKey(t *testing.T) {
	registerResolveFixtures(t)

	// legacy exact
	r, err := ResolveEventKey("im.message.receive_v1")
	if err != nil || r.IsRefined {
		t.Fatalf("legacy exact: %+v %v", r, err)
	}
	// legacy + suffix rejected (exact match takes priority)
	if _, err := ResolveEventKey("im.message.receive_v1/foo/bar"); err == nil {
		t.Fatal("legacy+suffix must fail")
	}
	// bare refined base key rejected (R1)
	if _, err := ResolveEventKey("im.message.created_v1"); err == nil {
		t.Fatal("bare refined base must fail (R1)")
	}
	// placeholder template -> target_resource
	r, err = ResolveEventKey("im.message.created_v1/chat-id/oc_9f3b1c2d8a")
	if err != nil || !r.IsRefined || r.TargetResource != "im.message?chat_id=oc_9f3b1c2d8a" {
		t.Fatalf("chat-id: %+v %v", r, err)
	}
	// owner/me fixed value
	r, err = ResolveEventKey("im.message.created_v1/owner/me")
	if err != nil || r.TargetResource != "im.message?owner=me" {
		t.Fatalf("owner/me: %+v %v", r, err)
	}
	// %2F decoded exactly once, not double-encoded; unescaped extra / rejected
	if _, err := ResolveEventKey("im.message.created_v1/chat-id/a/b"); err == nil {
		t.Fatal("extra unescaped / must fail")
	}
	r, _ = ResolveEventKey("im.message.created_v1/chat-id/oc_%2F")
	if want := "im.message?chat_id=oc_%2F"; r.TargetResource != want {
		t.Fatalf("encode: got %q want %q", r.TargetResource, want)
	}
	// illegal % escape rejected
	if _, err := ResolveEventKey("im.message.created_v1/chat-id/oc_%zz"); err == nil {
		t.Fatal("bad %% escape must fail")
	}
}

func TestResolveEventKey_LegacyExactReturnsSameDefinitionNoRefinedFields(t *testing.T) {
	registerResolveFixtures(t)

	r, err := ResolveEventKey("im.message.receive_v1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Definition == nil || r.Definition.Key != "im.message.receive_v1" {
		t.Fatalf("Definition = %+v", r.Definition)
	}
	if r.MaterializedKey != "im.message.receive_v1" {
		t.Errorf("MaterializedKey = %q, want %q", r.MaterializedKey, "im.message.receive_v1")
	}
	if r.Template != nil || r.SelectorKey != "" || r.SelectorValue != "" || r.TargetResource != "" {
		t.Errorf("legacy result must not carry refined fields: %+v", r)
	}
}

func TestResolveEventKey_ChatIDFieldsPopulated(t *testing.T) {
	registerResolveFixtures(t)

	r, err := ResolveEventKey("im.message.created_v1/chat-id/oc_9f3b1c2d8a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !r.IsRefined {
		t.Error("IsRefined = false, want true")
	}
	if r.Definition == nil || r.Definition.Key != "im.message.created_v1" {
		t.Errorf("Definition = %+v", r.Definition)
	}
	if r.Template == nil || r.Template.PathSegment != "chat-id" {
		t.Errorf("Template = %+v", r.Template)
	}
	if r.SelectorKey != "chat_id" {
		t.Errorf("SelectorKey = %q, want %q", r.SelectorKey, "chat_id")
	}
	if r.SelectorValue != "oc_9f3b1c2d8a" {
		t.Errorf("SelectorValue = %q, want %q", r.SelectorValue, "oc_9f3b1c2d8a")
	}
	if r.MaterializedKey != "im.message.created_v1/chat-id/oc_9f3b1c2d8a" {
		t.Errorf("MaterializedKey = %q", r.MaterializedKey)
	}
}

func TestResolveEventKey_R1_TypedErrorAndHint(t *testing.T) {
	registerResolveFixtures(t)

	_, err := ResolveEventKey("im.message.created_v1")
	assertInvalidArgument(t, err, "event schema im.message.created_v1 --json")
}

func TestResolveEventKey_LegacySuffix_TypedErrorAndHint(t *testing.T) {
	registerResolveFixtures(t)

	_, err := ResolveEventKey("im.message.receive_v1/foo/bar")
	assertInvalidArgument(t, err, "only accepts an exact match")
}

func TestResolveEventKey_UnknownBase(t *testing.T) {
	registerResolveFixtures(t)

	_, err := ResolveEventKey("does.not.exist/chat-id/oc_xxx")
	assertInvalidArgument(t, err, "event list")
}

func TestResolveEventKey_UnknownPathSegment(t *testing.T) {
	registerResolveFixtures(t)

	_, err := ResolveEventKey("im.message.created_v1/nope/oc_xxx")
	assertInvalidArgument(t, err, "event schema im.message.created_v1 --json")
}

func TestResolveEventKey_FixedValueMismatch(t *testing.T) {
	registerResolveFixtures(t)

	_, err := ResolveEventKey("im.message.created_v1/owner/someone_else")
	assertInvalidArgument(t, err, "owner/me")
}

func TestResolveEventKey_EmptyValueRejected(t *testing.T) {
	registerResolveFixtures(t)

	_, err := ResolveEventKey("im.message.created_v1/chat-id/")
	ve := assertInvalidArgument(t, err, "pass a value")
	if !strings.Contains(ve.Message, "non-empty") {
		t.Errorf("Message = %q, want it to explain the value must be non-empty", ve.Message)
	}
}

// MissingSegmentAndValue: a refined base key with a "/" but no path-segment
// (or with a path-segment but no value) is still "not materialized" and
// falls back to the same R1 guidance as the fully bare base key.
func TestResolveEventKey_MissingSegmentAndValue(t *testing.T) {
	registerResolveFixtures(t)

	_, err := ResolveEventKey("im.message.created_v1/chat-id")
	assertInvalidArgument(t, err, "event schema im.message.created_v1 --json")
}

func TestResolveEventKey_UnicodeValueRoundTrips(t *testing.T) {
	registerResolveFixtures(t)

	r, err := ResolveEventKey("im.message.created_v1/chat-id/你好")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.SelectorValue != "你好" {
		t.Errorf("SelectorValue = %q, want %q", r.SelectorValue, "你好")
	}
	if want := "im.message?chat_id=%E4%BD%A0%E5%A5%BD"; r.TargetResource != want {
		t.Errorf("TargetResource = %q, want %q", r.TargetResource, want)
	}
	if want := "im.message.created_v1/chat-id/%E4%BD%A0%E5%A5%BD"; r.MaterializedKey != want {
		t.Errorf("MaterializedKey = %q, want %q", r.MaterializedKey, want)
	}
}

// SpecialCharsTreatedAsOneOpaqueValue covers spec §2.3's "/ ? & = %" edge
// case together with "duplicate selector": the raw value looks like a
// duplicate-key query string (chat_id appearing twice via "a=1&a=2") plus a
// literal '?' and a validly-escaped '%'. ResolveEventKey must not re-parse
// it — it stays ONE opaque chat_id value, and the '=' / '&' / '?' / '%'
// characters all land percent-escaped in TargetResource so no extra
// "&chat_id=" pair or extra '?' can be smuggled into the query string. This
// also proves single-decode: if PathUnescape ran twice, the second pass
// would choke on the trailing bare '%' left after the first decode and
// ResolveEventKey would return an error instead of succeeding here.
func TestResolveEventKey_SpecialCharsTreatedAsOneOpaqueValue(t *testing.T) {
	registerResolveFixtures(t)

	r, err := ResolveEventKey("im.message.created_v1/chat-id/a=1&a=2?b=3%25")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := "a=1&a=2?b=3%"; r.SelectorValue != want {
		t.Errorf("SelectorValue = %q, want %q", r.SelectorValue, want)
	}
	want := "im.message?chat_id=a%3D1%26a%3D2%3Fb%3D3%25"
	if r.TargetResource != want {
		t.Errorf("TargetResource = %q, want %q", r.TargetResource, want)
	}
	if n := strings.Count(r.TargetResource, "?"); n != 1 {
		t.Errorf("TargetResource has %d '?' delimiters, want exactly 1: %q", n, r.TargetResource)
	}
	if n := strings.Count(r.TargetResource, "chat_id="); n != 1 {
		t.Errorf("TargetResource has %d chat_id= pairs, want exactly 1: %q", n, r.TargetResource)
	}
}

func TestResolveEventKey_OverLongValueNotTruncated(t *testing.T) {
	registerResolveFixtures(t)

	long := strings.Repeat("x", 5000)
	r, err := ResolveEventKey("im.message.created_v1/chat-id/" + long)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.SelectorValue != long {
		t.Errorf("SelectorValue length = %d, want %d (value must not be truncated)", len(r.SelectorValue), len(long))
	}
	if want := "im.message?chat_id=" + long; r.TargetResource != want {
		t.Error("TargetResource must carry the full untruncated value")
	}
}

// DuplicatePathSegmentTemplates_FirstMatchWins: registry-level uniqueness of
// PathSegment across a base key's KeyTemplates is not validated by
// RegisterKey (frozen from an earlier task — see validateRefinedSubscription
// in registry.go), so a buggy catalog could register two templates sharing
// one PathSegment. ResolveEventKey must still behave deterministically
// (first-declared template wins) rather than picking randomly or panicking.
func TestResolveEventKey_DuplicatePathSegmentTemplates_FirstMatchWins(t *testing.T) {
	const key = "test.dup.created_v1"
	RegisterKey(KeyDefinition{
		Key:                 key,
		EventType:           key,
		Schema:              nativeSchema(),
		RefinedSubscription: true,
		ResourceType:        "test.dup",
		AuthTypes:           []string{"user"},
		KeyTemplates: []KeyTemplate{
			{Template: key + "/id/{id}", Example: key + "/id/1", SelectorKey: "id_a", PathSegment: "id", AuthTypes: []string{"user"}},
			{Template: key + "/id/{id}", Example: key + "/id/1", SelectorKey: "id_b", PathSegment: "id", AuthTypes: []string{"user"}},
		},
	})
	t.Cleanup(func() { UnregisterKeyForTest(key) })

	r, err := ResolveEventKey(key + "/id/42")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.SelectorKey != "id_a" {
		t.Errorf("SelectorKey = %q, want %q (first-declared template must win deterministically)", r.SelectorKey, "id_a")
	}
	if want := "test.dup?id_a=42"; r.TargetResource != want {
		t.Errorf("TargetResource = %q, want %q", r.TargetResource, want)
	}
}

func TestResolveEventKey_BadPercentEscapeIsTypedInvalidArgument(t *testing.T) {
	registerResolveFixtures(t)

	_, err := ResolveEventKey("im.message.created_v1/chat-id/oc_%zz")
	ve := assertInvalidArgument(t, err, "")
	// Hint uses a literal '%' via a "%%" Sprintf escape; assert it collapsed
	// correctly (a stray unescaped '%' would either break go vet's printf
	// check or leak a literal "%%" into the rendered text).
	if strings.Contains(ve.Hint, "%%") {
		t.Errorf("Hint leaked an unescaped %%%%: %q", ve.Hint)
	}
	if !strings.Contains(ve.Hint, "%2F") {
		t.Errorf("Hint = %q, want guidance mentioning %%2F", ve.Hint)
	}
}
