// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package whiteboard

import (
	"testing"
)

func TestShortcutsIncludesWhiteboardNodeCommands(t *testing.T) {
	t.Parallel()

	got := Shortcuts()
	want := []string{
		"+update",
		"+export",
		"+query",
		"+node-create",
		"+node-update",
		"+node-delete",
	}

	seen := make(map[string]bool, len(got))
	for _, shortcut := range got {
		if seen[shortcut.Command] {
			t.Fatalf("duplicate shortcut command: %s", shortcut.Command)
		}
		seen[shortcut.Command] = true
	}

	for _, command := range want {
		if !seen[command] {
			t.Fatalf("missing shortcut command %q in Shortcuts()", command)
		}
	}
}

func TestParseWhiteboardNodeBatchPayload_MissingNodes(t *testing.T) {
	t.Parallel()

	_, err := parseWhiteboardNodeBatchPayload([]byte(`{}`), false)
	assertValidationParam(t, err, "--source", false)
}

func TestParseWhiteboardNodeBatchPayload_EmptyNodes(t *testing.T) {
	t.Parallel()

	_, err := parseWhiteboardNodeBatchPayload([]byte(`{"nodes":[]}`), false)
	assertValidationParam(t, err, "--source", false)
}

func TestParseWhiteboardNodeBatchPayload_InvalidJSONPreservesCause(t *testing.T) {
	t.Parallel()

	_, err := parseWhiteboardNodeBatchPayload([]byte(`not-json`), false)
	assertValidationParam(t, err, "--source", true)
}

func TestParseWhiteboardNodeBatchPayload_RequireIDMissingID(t *testing.T) {
	t.Parallel()

	_, err := parseWhiteboardNodeBatchPayload([]byte(`{"nodes":[{"text":{"text":"x"}}]}`), true)
	assertValidationParam(t, err, "--source", false)
}

func TestParseWhiteboardNodeBatchPayload_RequireIDBlankID(t *testing.T) {
	t.Parallel()

	_, err := parseWhiteboardNodeBatchPayload([]byte(`{"nodes":[{"id":"   ","text":{"text":"x"}}]}`), true)
	assertValidationParam(t, err, "--source", false)
}

func TestParseWhiteboardNodeBatchPayload_PreservesArbitraryFields(t *testing.T) {
	t.Parallel()

	payload, err := parseWhiteboardNodeBatchPayload([]byte(`{"nodes":[{"id":"node-1","type":"shape","custom":{"x":1},"points":[1,2]}]}`), true)
	if err != nil {
		t.Fatalf("parseWhiteboardNodeBatchPayload() error = %v", err)
	}
	if len(payload.Nodes) != 1 {
		t.Fatalf("len(payload.Nodes) = %d, want 1", len(payload.Nodes))
	}
	node := payload.Nodes[0]
	if got := node["id"]; got != "node-1" {
		t.Errorf("node[id] = %v, want node-1", got)
	}
	if got := node["type"]; got != "shape" {
		t.Errorf("node[type] = %v, want shape", got)
	}
	custom, ok := node["custom"].(map[string]interface{})
	if !ok {
		t.Fatalf("node[custom] = %T, want map[string]interface{}", node["custom"])
	}
	if got := custom["x"]; got != float64(1) {
		t.Errorf("node[custom][x] = %v, want 1", got)
	}
	points, ok := node["points"].([]interface{})
	if !ok {
		t.Fatalf("node[points] = %T, want []interface{}", node["points"])
	}
	if len(points) != 2 || points[0] != float64(1) || points[1] != float64(2) {
		t.Errorf("node[points] = %#v, want [1 2]", points)
	}
}

func TestParseWhiteboardNodeIDs_TrimsItems(t *testing.T) {
	t.Parallel()

	ids, err := parseWhiteboardNodeIDs(" nodeA, nodeB ,nodeC ")
	if err != nil {
		t.Fatalf("parseWhiteboardNodeIDs() error = %v", err)
	}
	want := []string{"nodeA", "nodeB", "nodeC"}
	if len(ids) != len(want) {
		t.Fatalf("len(ids) = %d, want %d", len(ids), len(want))
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Errorf("ids[%d] = %q, want %q", i, ids[i], want[i])
		}
	}
}

func TestParseWhiteboardNodeIDs_RejectsEmptyInput(t *testing.T) {
	t.Parallel()

	_, err := parseWhiteboardNodeIDs("   ")
	assertValidationParam(t, err, "--node-ids", false)
}

func TestParseWhiteboardNodeIDs_RejectsEmptyItems(t *testing.T) {
	t.Parallel()

	_, err := parseWhiteboardNodeIDs("nodeA, ,nodeB")
	assertValidationParam(t, err, "--node-ids", false)
}

func TestParseWhiteboardNodeIDs_RejectsDuplicateIDs(t *testing.T) {
	t.Parallel()

	_, err := parseWhiteboardNodeIDs("nodeA,nodeB,nodeA")
	assertValidationParam(t, err, "--node-ids", false)
}

func TestValidateOptionalWhiteboardNodeIdempotentToken_TooShort(t *testing.T) {
	t.Parallel()

	err := validateOptionalWhiteboardNodeIdempotentToken("short")
	assertValidationParam(t, err, "--idempotent-token", false)
}
