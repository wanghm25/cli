// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package whiteboard

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/shortcuts/common"
)

var wbNodeUpdateScopes = []string{"board:whiteboard:node:update"}
var wbNodeUpdateAuthTypes = []string{"user", "bot"}
var wbNodeUpdateFlags = []common.Flag{
	{Name: "whiteboard-token", Desc: "whiteboard token of the whiteboard to update nodes in. You need edit permission on the whiteboard.", Required: true},
	{Name: "source", Desc: `JSON payload containing a non-empty "nodes" array. Each node must include "id"; the update body sends every other field.`, Required: true, Input: []string{common.Stdin, common.File}},
	{Name: "idempotent-token", Desc: "idempotent token reserved for future batch update compatibility. Default is empty. Minimum length is 10.", Required: false},
}

func wbNodeUpdateValidate(_ context.Context, runtime *common.RuntimeContext) error {
	if err := common.RejectDangerousCharsTyped("--whiteboard-token", runtime.Str("whiteboard-token")); err != nil {
		return err
	}
	if err := validateOptionalWhiteboardNodeIdempotentToken(runtime.Str("idempotent-token")); err != nil {
		return err
	}
	_, err := parseWhiteboardNodeBatchPayload([]byte(runtime.Str("source")), true)
	return err
}

func wbNodeUpdateDryRun(_ context.Context, runtime *common.RuntimeContext) *common.DryRunAPI {
	payload, err := parseWhiteboardNodeBatchPayload([]byte(runtime.Str("source")), true)
	if err != nil {
		return common.NewDryRunAPI().Desc("parse input failed: " + err.Error())
	}

	dry := common.NewDryRunAPI()
	for _, node := range payload.Nodes {
		nodeID := node["id"].(string)
		dry.PUT(wbNodeUpdateDryRunURL(runtime.Str("whiteboard-token"), nodeID)).
			Body(whiteboardNodeUpdateBody(node)).
			Desc("update a node in the whiteboard.")
	}
	return dry
}

func wbNodeUpdateExecute(ctx context.Context, runtime *common.RuntimeContext) error {
	payload, err := parseWhiteboardNodeBatchPayload([]byte(runtime.Str("source")), true)
	if err != nil {
		return err
	}

	updatedNodeIDs := make([]string, 0, len(payload.Nodes))
	// Temporary compatibility path until whiteboard.node batch_update is available; keep public input contract unchanged.
	for i, node := range payload.Nodes {
		nodeID := node["id"].(string)
		if _, err := callWhiteboardNodeWrite(
			ctx,
			runtime,
			http.MethodPut,
			wbNodeUpdateURL(runtime.Str("whiteboard-token"), nodeID),
			nil,
			whiteboardNodeUpdateBody(node),
		); err != nil {
			return wbNodeUpdateFanoutError(i, nodeID, err)
		}
		updatedNodeIDs = append(updatedNodeIDs, nodeID)
	}

	outData := map[string]interface{}{
		"ids":   strings.Join(updatedNodeIDs, ","),
		"count": len(updatedNodeIDs),
	}
	runtime.OutFormat(outData, nil, func(w io.Writer) {
		fmt.Fprintf(w, "%d nodes updated.\n", len(updatedNodeIDs))
		fmt.Fprintf(w, "Update whiteboard nodes success")
	})
	return nil
}

func wbNodeUpdateURL(token string, nodeID string) string {
	return fmt.Sprintf("/open-apis/board/v1/whiteboards/%s/nodes/%s", url.PathEscape(token), url.PathEscape(nodeID))
}

func wbNodeUpdateDryRunURL(token string, nodeID string) string {
	return fmt.Sprintf("/open-apis/board/v1/whiteboards/%s/nodes/%s", common.MaskToken(url.PathEscape(token)), url.PathEscape(nodeID))
}

func whiteboardNodeUpdateBody(node map[string]interface{}) map[string]interface{} {
	body := make(map[string]interface{}, len(node))
	for key, value := range node {
		if key == "id" {
			continue
		}
		body[key] = value
	}
	return map[string]interface{}{"node": body}
}

func wbNodeUpdateFanoutError(index int, nodeID string, err error) error {
	if p, ok := errs.ProblemOf(err); ok {
		p.Message = fmt.Sprintf("update whiteboard node failed at nodes[%d] id %q: %s", index, nodeID, p.Message)
		return err
	}
	return errs.NewInternalError(
		errs.SubtypeUnknown,
		"update whiteboard node failed at nodes[%d] id %q: %s",
		index,
		nodeID,
		err.Error(),
	).WithCause(err)
}

// WhiteboardNodeUpdate registers the `whiteboard +node-update` shortcut.
var WhiteboardNodeUpdate = common.Shortcut{
	Service:     "whiteboard",
	Command:     "+node-update",
	Description: "Update nodes in an existing whiteboard.",
	Risk:        "write",
	Scopes:      wbNodeUpdateScopes,
	AuthTypes:   wbNodeUpdateAuthTypes,
	Flags:       wbNodeUpdateFlags,
	Tips: []string{
		`Pass --source as JSON with a non-empty "nodes" array; each node must include "id".`,
		`Temporary behavior: +node-update fans out to single-node update calls until whiteboard.node batch_update is available.`,
		`Current +node-update execution is non-atomic; earlier nodes may already be updated if a later node fails.`,
		`The CLI input contract will stay batch-shaped when the internal transport moves to batch_update.`,
	},
	Validate: wbNodeUpdateValidate,
	DryRun:   wbNodeUpdateDryRun,
	Execute:  wbNodeUpdateExecute,
}
