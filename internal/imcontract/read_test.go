// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package imcontract

import (
	"encoding/json"
	"testing"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/client"
	"github.com/larksuite/cli/internal/output"
)

func TestReadCompletenessMatrix(t *testing.T) {
	apiErr := errs.NewAPIError(errs.SubtypeServerError, "later page failed")
	networkErr := errs.NewNetworkError(errs.SubtypeNetworkTransport, "request failed").WithRetryable()
	invalidErr := errs.NewInternalError(errs.SubtypeInvalidResponse, "bad pagination")
	tests := []struct {
		name       string
		fullRead   bool
		status     client.PaginationStatus
		wantOK     bool
		wantDone   bool
		wantExit   int
		wantReason client.StopReason
		wantError  bool
		wantHint   string
	}{
		{"single exhausted", false, client.PaginationStatus{PagesFetched: 1, StopReason: client.StopReasonExhausted}, true, true, 0, client.StopReasonExhausted, false, ""},
		{"single has more", false, client.PaginationStatus{PagesFetched: 1, HasMore: true, NextPageToken: "next", StopReason: client.StopReasonSinglePage}, true, false, 0, client.StopReasonSinglePage, false, "Result is incomplete. Re-run with --page-all --page-limit 0 when exhaustive output is required."},
		{"all exhausted", true, client.PaginationStatus{PagesFetched: 2, StopReason: client.StopReasonExhausted}, true, true, 0, client.StopReasonExhausted, false, ""},
		{"page limit", true, client.PaginationStatus{PagesFetched: 2, HasMore: true, NextPageToken: "next", StopReason: client.StopReasonPageLimit}, true, false, 0, client.StopReasonPageLimit, false, "Result is incomplete because --page-limit was reached. Use --page-limit 0 only when exhaustive output is required."},
		{"start token", false, client.PaginationStatus{PagesFetched: 1, StopReason: client.StopReasonStartPageToken}, true, false, 0, client.StopReasonStartPageToken, false, hintStartPage},
		{"api error", true, client.PaginationStatus{PagesFetched: 1, HasMore: true, NextPageToken: "next", StopReason: client.StopReasonAPIError, Cause: apiErr}, false, false, output.ExitAPI, client.StopReasonAPIError, true, "The read is incomplete. Retry the read; do not infer that missing items do not exist."},
		{"transport error", true, client.PaginationStatus{PagesFetched: 1, HasMore: true, NextPageToken: "next", StopReason: client.StopReasonTransportError, Cause: networkErr}, false, false, output.ExitNetwork, client.StopReasonTransportError, true, "The read is incomplete. Retry the read; do not infer that missing items do not exist."},
		{"missing token", true, client.PaginationStatus{PagesFetched: 1, HasMore: true, StopReason: client.StopReasonMissingToken, Cause: invalidErr}, false, false, output.ExitInternal, client.StopReasonMissingToken, true, "The server did not provide a usable next page token. Report the result as incomplete."},
		{"repeated token", true, client.PaginationStatus{PagesFetched: 2, HasMore: true, StopReason: client.StopReasonRepeatedToken, Cause: invalidErr}, false, false, output.ExitInternal, client.StopReasonRepeatedToken, true, "The server did not provide a usable next page token. Report the result as incomplete."},
		{"single truncation", false, client.PaginationStatus{PagesFetched: 1, StopReason: client.StopReasonServerTruncation}, true, false, 0, client.StopReasonServerTruncation, false, "The server truncated the result. Narrow the query range before retrying."},
		{"full truncation", true, client.PaginationStatus{PagesFetched: 1, StopReason: client.StopReasonServerTruncation}, false, false, output.ExitAPI, client.StopReasonServerTruncation, false, "The server truncated the result. Narrow the query range before retrying."},
	}
	contract := mustReadContract(t, "im +chat-list")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			session, err := NewReadSession(contract, ReadOptions{FullRead: tt.fullRead})
			if err != nil {
				t.Fatal(err)
			}
			session.ObservePagination(tt.status)
			got, err := session.Finalize(map[string]any{"items": []any{"a"}})
			if err != nil {
				t.Fatal(err)
			}
			if got.OK != tt.wantOK || got.ExitCode != tt.wantExit {
				t.Fatalf("result OK/exit = %v/%d, want %v/%d", got.OK, got.ExitCode, tt.wantOK, tt.wantExit)
			}
			if got.Meta == nil || got.Meta.Complete == nil || *got.Meta.Complete != tt.wantDone {
				t.Fatalf("complete = %#v, want %v", got.Meta, tt.wantDone)
			}
			if got.Meta.StopReason != string(tt.wantReason) {
				t.Fatalf("stop reason = %q, want %q", got.Meta.StopReason, tt.wantReason)
			}
			if (got.Error != nil) != tt.wantError {
				t.Fatalf("error present = %v, want %v", got.Error != nil, tt.wantError)
			}
			if got.Hint != tt.wantHint {
				t.Fatalf("hint = %q, want %q", got.Hint, tt.wantHint)
			}
		})
	}
}

func TestReadFailureErrorWireShapeDoesNotSerializeCause(t *testing.T) {
	contract := mustReadContract(t, "im +chat-list")
	session, err := NewReadSession(contract, ReadOptions{FullRead: true})
	if err != nil {
		t.Fatal(err)
	}
	secret := "raw-server-cause-must-not-leak"
	cause := errs.NewNetworkError(errs.SubtypeNetworkTransport, "request failed").
		WithRetryable().
		WithCause(assertionError(secret))
	session.ObservePagination(client.PaginationStatus{
		PagesFetched:  1,
		HasMore:       true,
		NextPageToken: "opaque-token",
		StopReason:    client.StopReasonTransportError,
		Cause:         cause,
	})
	result, err := session.Finalize(map[string]any{"items": []any{"kept"}})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(result.Error)
	if err != nil {
		t.Fatal(err)
	}
	if string(wire) == "" || containsAny(string(wire), secret, "opaque-token") {
		t.Fatalf("unsafe error wire: %s", wire)
	}
}

func TestSearchEmptyResultAddsNonExistenceHint(t *testing.T) {
	contract := mustReadContract(t, "im +chat-search")
	session, err := NewReadSession(contract, ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	session.ObservePagination(client.PaginationStatus{PagesFetched: 1, StopReason: client.StopReasonExhausted})
	result, err := session.Finalize(map[string]any{"chats": []any{}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Meta == nil || result.Meta.Complete == nil || !*result.Meta.Complete {
		t.Fatalf("expected exhausted result to be complete: %#v", result.Meta)
	}
	const wantHint = "The search was exhausted, but an empty search result does not prove that the resource does not exist."
	if result.Hint != wantHint {
		t.Fatalf("hint = %q, want %q", result.Hint, wantHint)
	}
}

func TestEntityAndMaterializeDoNotInventPagination(t *testing.T) {
	for _, key := range []ContractKey{"im chat.nickname get", "im +messages-resources-download"} {
		t.Run(string(key), func(t *testing.T) {
			contract := mustReadContract(t, key)
			session, err := NewReadSession(contract, ReadOptions{})
			if err != nil {
				t.Fatal(err)
			}
			result, err := session.Finalize(map[string]any{"nickname": ""})
			if err != nil {
				t.Fatal(err)
			}
			if !result.OK || result.Meta != nil || result.ExitCode != 0 {
				t.Fatalf("unexpected finite result: %#v", result)
			}
		})
	}
}

func TestUnknownReadStrategyFailsClosed(t *testing.T) {
	_, err := NewReadSession(Contract{
		Key:      "im future read",
		Strategy: Strategy{Kind: StrategyKind("future_read")},
	}, ReadOptions{})
	if err == nil || !errs.IsInternal(err) {
		t.Fatalf("expected typed internal error, got %v", err)
	}
}

func mustReadContract(t *testing.T, key ContractKey) Contract {
	t.Helper()
	contract, ok := Lookup(key)
	if !ok {
		t.Fatalf("missing contract %q", key)
	}
	return contract
}

type assertionError string

func (e assertionError) Error() string { return string(e) }

func containsAny(s string, values ...string) bool {
	for _, value := range values {
		if value != "" && stringContains(s, value) {
			return true
		}
	}
	return false
}

func stringContains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
