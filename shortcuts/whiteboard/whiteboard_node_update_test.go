// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package whiteboard

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/httpmock"
)

func TestWhiteboardNodeUpdateValidate_SourceMissingIDTypedParam(t *testing.T) {
	t.Parallel()

	rt := newTestRuntime(map[string]string{
		"whiteboard-token": "test-board",
		"source":           `{"nodes":[{"type":"text","text":{"content":"hello"}}]}`,
	}, nil)

	err := wbNodeUpdateValidate(context.Background(), rt)
	assertValidationParam(t, err, "--source", false)
}

func TestWhiteboardNodeUpdateDryRun_RequestShape(t *testing.T) {
	t.Parallel()

	rt := newTestRuntime(map[string]string{
		"whiteboard-token": "test-board",
		"idempotent-token": "update-token-12345",
		"source": `{"nodes":[` +
			`{"id":"nodeA","type":"text","text":{"content":"hello A"}},` +
			`{"id":"nodeB","type":"text","text":{"content":"hello B"}}` +
			`]}`,
	}, nil)

	dryRun := wbNodeUpdateDryRun(context.Background(), rt)
	if dryRun == nil {
		t.Fatal("wbNodeUpdateDryRun() returned nil")
	}

	var got struct {
		API []struct {
			Method string                 `json:"method"`
			URL    string                 `json:"url"`
			Params map[string]interface{} `json:"params"`
			Body   map[string]interface{} `json:"body"`
		} `json:"api"`
	}
	data, err := json.Marshal(dryRun)
	if err != nil {
		t.Fatalf("marshal dry-run: %v", err)
	}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal dry-run: %v\njson=%s", err, string(data))
	}
	if len(got.API) != 2 {
		t.Fatalf("api len = %d, want 2; json=%s", len(got.API), string(data))
	}
	wantURLs := []string{
		"/open-apis/board/v1/whiteboards/test...oard/nodes/nodeA",
		"/open-apis/board/v1/whiteboards/test...oard/nodes/nodeB",
	}
	wantText := []string{"hello A", "hello B"}
	for i := range got.API {
		if got.API[i].Method != "PUT" {
			t.Fatalf("api[%d].method = %q, want PUT", i, got.API[i].Method)
		}
		if got.API[i].URL != wantURLs[i] {
			t.Fatalf("api[%d].url = %q, want %q", i, got.API[i].URL, wantURLs[i])
		}
		if len(got.API[i].Params) != 0 {
			t.Fatalf("api[%d].params = %#v, want empty because single-node update does not accept client_token", i, got.API[i].Params)
		}
		node, ok := got.API[i].Body["node"].(map[string]interface{})
		if !ok {
			t.Fatalf("api[%d].body.node = %T, want map; body=%#v", i, got.API[i].Body["node"], got.API[i].Body)
		}
		if _, exists := node["id"]; exists {
			t.Fatalf("api[%d].body.node.id = %#v, want absent", i, node["id"])
		}
		text, ok := node["text"].(map[string]interface{})
		if !ok || text["content"] != wantText[i] {
			t.Fatalf("api[%d].body.node.text = %#v, want content %q", i, node["text"], wantText[i])
		}
	}
}

func TestWhiteboardNodeUpdateExecute_PatchesNodes(t *testing.T) {
	factory, stdout, reg := newUpdateExecuteFactory(t)

	stubA := &httpmock.Stub{
		Method: "PUT",
		URL:    "/open-apis/board/v1/whiteboards/test-board/nodes/nodeA",
		Body: map[string]interface{}{
			"code": 0,
			"msg":  "success",
			"data": map[string]interface{}{},
		},
	}
	stubB := &httpmock.Stub{
		Method: "PUT",
		URL:    "/open-apis/board/v1/whiteboards/test-board/nodes/nodeB",
		Body: map[string]interface{}{
			"code": 0,
			"msg":  "success",
			"data": map[string]interface{}{},
		},
	}
	reg.Register(stubA)
	reg.Register(stubB)

	source := `{"nodes":[` +
		`{"id":"nodeA","type":"text","text":{"content":"hello A"}},` +
		`{"id":"nodeB","type":"text","text":{"content":"hello B"}}` +
		`]}`
	args := []string{"+node-update", "--whiteboard-token", "test-board", "--source", source}
	if err := runUpdateShortcut(t, WhiteboardNodeUpdate, args, factory, stdout); err != nil {
		t.Fatalf("err=%v", err)
	}

	assertNodeUpdateCapturedBody(t, stubA.CapturedBody, "hello A")
	assertNodeUpdateCapturedBody(t, stubB.CapturedBody, "hello B")
	if !strings.Contains(stdout.String(), `"ids": "nodeA,nodeB"`) {
		t.Fatalf("stdout=%s, want ids nodeA,nodeB", stdout.String())
	}
	if !strings.Contains(stdout.String(), `"count": 2`) {
		t.Fatalf("stdout=%s, want count 2", stdout.String())
	}
}

func TestWhiteboardNodeUpdateExecute_PartialFailureIncludesNodeContext(t *testing.T) {
	factory, stdout, reg := newUpdateExecuteFactory(t)

	reg.Register(&httpmock.Stub{
		Method: "PUT",
		URL:    "/open-apis/board/v1/whiteboards/test-board/nodes/nodeA",
		Body: map[string]interface{}{
			"code": 0,
			"msg":  "success",
			"data": map[string]interface{}{},
		},
	})
	reg.Register(&httpmock.Stub{
		Method: "PUT",
		URL:    "/open-apis/board/v1/whiteboards/test-board/nodes/nodeB",
		Body: map[string]interface{}{
			"code": 1254001,
			"msg":  "node not found",
			"data": map[string]interface{}{},
		},
	})

	source := `{"nodes":[` +
		`{"id":"nodeA","type":"text","text":{"content":"hello A"}},` +
		`{"id":"nodeB","type":"text","text":{"content":"hello B"}}` +
		`]}`
	args := []string{"+node-update", "--whiteboard-token", "test-board", "--source", source}
	err := runUpdateShortcut(t, WhiteboardNodeUpdate, args, factory, stdout)
	if err == nil {
		t.Fatal("expected partial failure error, got nil")
	}
	if !strings.Contains(err.Error(), "nodeB") || !strings.Contains(err.Error(), "nodes[1]") {
		t.Fatalf("err=%v, want nodeB and nodes[1] context", err)
	}
	problem, ok := errs.ProblemOf(err)
	if !ok {
		t.Fatalf("errs.ProblemOf returned false for %T", err)
	}
	if problem.Category != errs.CategoryAPI {
		t.Fatalf("Category = %q, want %q", problem.Category, errs.CategoryAPI)
	}
	if !strings.Contains(problem.Message, "nodeB") || !strings.Contains(problem.Message, "nodes[1]") {
		t.Fatalf("Problem message = %q, want nodeB and nodes[1] context", problem.Message)
	}
	var apiErr *errs.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error type = %T, want *errs.APIError reachable via errors.As", err)
	}
}

func TestWhiteboardNodeUpdateTips_MentionTemporaryNonAtomicBehavior(t *testing.T) {
	t.Parallel()

	tips := strings.Join(WhiteboardNodeUpdate.Tips, "\n")
	for _, want := range []string{"non-atomic", "batch_update", "fans out"} {
		if !strings.Contains(tips, want) {
			t.Fatalf("tips = %q, want substring %q", tips, want)
		}
	}
}

func assertNodeUpdateCapturedBody(t *testing.T, raw []byte, wantContent string) {
	t.Helper()
	var body map[string]interface{}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("unmarshal captured body: %v\nraw=%s", err, string(raw))
	}
	node, ok := body["node"].(map[string]interface{})
	if !ok {
		t.Fatalf("body.node = %T, want map; body=%s", body["node"], string(raw))
	}
	if _, exists := node["id"]; exists {
		t.Fatalf("body.node.id = %#v, want absent; body=%s", node["id"], string(raw))
	}
	text, ok := node["text"].(map[string]interface{})
	if !ok || text["content"] != wantContent {
		t.Fatalf("body.node.text = %#v, want content %q; body=%s", node["text"], wantContent, string(raw))
	}
}
