// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package whiteboard

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
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
	if len(got.API) != 1 {
		t.Fatalf("api len = %d, want 1; json=%s", len(got.API), string(data))
	}
	if got.API[0].Method != "PUT" {
		t.Fatalf("method = %q, want PUT", got.API[0].Method)
	}
	if got.API[0].URL != "/open-apis/board/v1/whiteboards/test...oard/nodes/batch_update" {
		t.Fatalf("url = %q, want masked batch_update URL", got.API[0].URL)
	}
	if got.API[0].Params["client_token"] != "update-token-12345" {
		t.Fatalf("params.client_token = %#v, want update-token-12345", got.API[0].Params["client_token"])
	}
	nodes, ok := got.API[0].Body["nodes"].([]interface{})
	if !ok || len(nodes) != 2 {
		t.Fatalf("body.nodes = %#v, want two nodes", got.API[0].Body["nodes"])
	}
	wantText := []string{"hello A", "hello B"}
	for i := range nodes {
		node, ok := nodes[i].(map[string]interface{})
		if !ok {
			t.Fatalf("body.nodes[%d] = %T, want map; nodes=%#v", i, nodes[i], nodes)
		}
		if node["id"] != []string{"nodeA", "nodeB"}[i] {
			t.Fatalf("body.nodes[%d].id = %#v", i, node["id"])
		}
		text, ok := node["text"].(map[string]interface{})
		if !ok || text["content"] != wantText[i] {
			t.Fatalf("body.nodes[%d].text = %#v, want content %q", i, node["text"], wantText[i])
		}
	}
}

func TestWhiteboardNodeUpdateExecute_BatchUpdatesNodes(t *testing.T) {
	factory, stdout, reg := newUpdateExecuteFactory(t)

	var capturedQuery string
	stub := &httpmock.Stub{
		Method: "PUT",
		URL:    "/open-apis/board/v1/whiteboards/test-board/nodes/batch_update",
		Body: map[string]interface{}{
			"code": 0,
			"msg":  "success",
			"data": map[string]interface{}{
				"ids": []string{"nodeA", "nodeB"},
			},
		},
		OnMatch: func(req *http.Request) {
			capturedQuery = req.URL.RawQuery
		},
	}
	reg.Register(stub)

	source := `{"nodes":[` +
		`{"id":"nodeA","type":"text","text":{"content":"hello A"}},` +
		`{"id":"nodeB","type":"text","text":{"content":"hello B"}}` +
		`]}`
	args := []string{"+node-update", "--whiteboard-token", "test-board", "--source", source, "--idempotent-token", "update-token-12345"}
	if err := runUpdateShortcut(t, WhiteboardNodeUpdate, args, factory, stdout); err != nil {
		t.Fatalf("err=%v", err)
	}

	assertNodeBatchUpdateCapturedBody(t, stub.CapturedBody, []string{"hello A", "hello B"})
	if !strings.Contains(capturedQuery, "client_token=update-token-12345") {
		t.Fatalf("query = %q, want client_token", capturedQuery)
	}
	if !strings.Contains(stdout.String(), `"ids": "nodeA,nodeB"`) {
		t.Fatalf("stdout=%s, want ids nodeA,nodeB", stdout.String())
	}
	if !strings.Contains(stdout.String(), `"count": 2`) {
		t.Fatalf("stdout=%s, want count 2", stdout.String())
	}
}

func TestWhiteboardNodeUpdateExecute_WithoutIdempotentTokenOmitsClientToken(t *testing.T) {
	factory, stdout, reg := newUpdateExecuteFactory(t)

	var capturedQuery string
	stub := &httpmock.Stub{
		Method: "PUT",
		URL:    "/open-apis/board/v1/whiteboards/test-board/nodes/batch_update",
		Body: map[string]interface{}{
			"code": 0,
			"msg":  "success",
			"data": map[string]interface{}{
				"ids": []string{"nodeA"},
			},
		},
		OnMatch: func(req *http.Request) {
			capturedQuery = req.URL.RawQuery
		},
	}
	reg.Register(stub)

	source := `{"nodes":[{"id":"nodeA","type":"text","text":{"content":"hello A"}}]}`
	args := []string{"+node-update", "--whiteboard-token", "test-board", "--source", source}
	if err := runUpdateShortcut(t, WhiteboardNodeUpdate, args, factory, stdout); err != nil {
		t.Fatalf("err=%v", err)
	}
	if capturedQuery != "" {
		t.Fatalf("query = %q, want empty when --idempotent-token is absent", capturedQuery)
	}
}

func TestWhiteboardNodeUpdateExecute_BatchFailureReturnsAPIError(t *testing.T) {
	factory, stdout, reg := newUpdateExecuteFactory(t)

	reg.Register(&httpmock.Stub{
		Method: "PUT",
		URL:    "/open-apis/board/v1/whiteboards/test-board/nodes/batch_update",
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
		t.Fatal("expected batch update failure error, got nil")
	}
	problem, ok := errs.ProblemOf(err)
	if !ok {
		t.Fatalf("errs.ProblemOf returned false for %T", err)
	}
	if problem.Category != errs.CategoryAPI {
		t.Fatalf("Category = %q, want %q", problem.Category, errs.CategoryAPI)
	}
	var apiErr *errs.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error type = %T, want *errs.APIError reachable via errors.As", err)
	}
}

func TestWhiteboardNodeUpdateTips_MentionTemporaryNonAtomicBehavior(t *testing.T) {
	t.Parallel()

	tips := strings.Join(WhiteboardNodeUpdate.Tips, "\n")
	for _, want := range []string{"batch_update", "client_token", "one whiteboard.node batch_update request"} {
		if !strings.Contains(tips, want) {
			t.Fatalf("tips = %q, want substring %q", tips, want)
		}
	}
	for _, banned := range []string{"fans out", "non-atomic", "Temporary behavior"} {
		if strings.Contains(tips, banned) {
			t.Fatalf("tips = %q, should not contain old fan-out wording %q", tips, banned)
		}
	}
}

func assertNodeBatchUpdateCapturedBody(t *testing.T, raw []byte, wantContent []string) {
	t.Helper()
	var body map[string]interface{}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("unmarshal captured body: %v\nraw=%s", err, string(raw))
	}
	nodes, ok := body["nodes"].([]interface{})
	if !ok || len(nodes) != len(wantContent) {
		t.Fatalf("body.nodes = %#v, want %d nodes; body=%s", body["nodes"], len(wantContent), string(raw))
	}
	for i, rawNode := range nodes {
		node, ok := rawNode.(map[string]interface{})
		if !ok {
			t.Fatalf("body.nodes[%d] = %T, want map; body=%s", i, rawNode, string(raw))
		}
		if _, exists := node["id"]; !exists {
			t.Fatalf("body.nodes[%d].id absent; body=%s", i, string(raw))
		}
		text, ok := node["text"].(map[string]interface{})
		if !ok || text["content"] != wantContent[i] {
			t.Fatalf("body.nodes[%d].text = %#v, want content %q; body=%s", i, node["text"], wantContent[i], string(raw))
		}
	}
}
