// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package whiteboard

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/client"
	"github.com/larksuite/cli/shortcuts/common"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
)

type whiteboardNodeBatchPayload struct {
	Nodes []map[string]interface{} `json:"nodes"`
}

func parseWhiteboardNodeBatchPayload(raw []byte, requireID bool) (whiteboardNodeBatchPayload, error) {
	var payload whiteboardNodeBatchPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return whiteboardNodeBatchPayload{}, errs.NewValidationError(errs.SubtypeInvalidArgument, "unmarshal input json failed: %v", err).
			WithParam("--source").
			WithCause(err)
	}
	if len(payload.Nodes) == 0 {
		return whiteboardNodeBatchPayload{}, errs.NewValidationError(errs.SubtypeInvalidArgument, `--source must include non-empty "nodes"`).
			WithParam("--source")
	}
	if requireID {
		for i, node := range payload.Nodes {
			id, ok := node["id"].(string)
			if !ok || strings.TrimSpace(id) == "" {
				return whiteboardNodeBatchPayload{}, errs.NewValidationError(errs.SubtypeInvalidArgument, "nodes[%d].id must be a non-empty string", i).
					WithParam("--source")
			}
		}
	}
	return payload, nil
}

func parseWhiteboardNodeIDs(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errs.NewValidationError(errs.SubtypeInvalidArgument, "--node-ids is required").
			WithParam("--node-ids")
	}

	parts := strings.Split(raw, ",")
	ids := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for i, part := range parts {
		id := strings.TrimSpace(part)
		if id == "" {
			return nil, errs.NewValidationError(errs.SubtypeInvalidArgument, "--node-ids item %d must not be empty", i+1).
				WithParam("--node-ids")
		}
		if _, ok := seen[id]; ok {
			return nil, errs.NewValidationError(errs.SubtypeInvalidArgument, "duplicate node id %q", id).
				WithParam("--node-ids")
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	return ids, nil
}

func validateOptionalWhiteboardNodeIdempotentToken(raw string) error {
	if err := common.RejectDangerousCharsTyped("--idempotent-token", raw); err != nil {
		return err
	}
	if raw != "" && len(raw) < 10 {
		return errs.NewValidationError(errs.SubtypeInvalidArgument, "--idempotent-token must be at least 10 characters long.").
			WithParam("--idempotent-token")
	}
	return nil
}

func callWhiteboardNodeWrite(ctx context.Context, runtime *common.RuntimeContext, method, apiPath string, params map[string]interface{}, body interface{}) (map[string]interface{}, error) {
	req := &larkcore.ApiReq{
		HttpMethod:  method,
		ApiPath:     apiPath,
		Body:        body,
		QueryParams: whiteboardNodeQueryParams(params),
	}
	resp, err := runtime.DoAPI(req)
	if err != nil {
		return nil, err
	}
	data, classifyErr := runtime.ClassifyAPIResponse(resp)
	if classifyErr == nil {
		return data, nil
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return data, classifyErr
	}
	if isWhiteboardNodeNonObjectSuccess(classifyErr, resp) {
		return nil, nil
	}
	return data, classifyErr
}

func whiteboardNodeQueryParams(params map[string]interface{}) larkcore.QueryParams {
	query := make(larkcore.QueryParams)
	for key, value := range params {
		switch typed := value.(type) {
		case []string:
			for _, item := range typed {
				query.Add(key, item)
			}
		case []interface{}:
			for _, item := range typed {
				query.Add(key, whiteboardNodeQueryValue(item))
			}
		default:
			query.Set(key, whiteboardNodeQueryValue(value))
		}
	}
	return query
}

func whiteboardNodeQueryValue(value interface{}) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func isWhiteboardNodeNonObjectSuccess(err error, resp *larkcore.ApiResp) bool {
	if resp == nil {
		return false
	}
	if _, ok := errs.ProblemOf(err); !ok {
		return false
	}
	result, parseErr := client.ParseJSONResponse(resp)
	if parseErr != nil {
		return false
	}
	_, isObject := result.(map[string]interface{})
	return !isObject
}
