// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package im

import (
	"context"
	"io"
	"strconv"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/validate"
	"github.com/larksuite/cli/shortcuts/common"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
)

// ImFeedGroupListItem provides the +feed-group-list-item shortcut: it lists the
// feed cards inside one feed group and enriches each item with chat_name resolved
// from its feed_id.
var ImFeedGroupListItem = common.Shortcut{
	Service:     "im",
	Command:     "+feed-group-list-item",
	Description: "List feed cards in a feed group (tag); user-only; enriches each item with chat_name resolved from feed_id; supports --page-all auto-pagination",
	Risk:        "read",
	UserScopes:  []string{feedGroupReadScope, chatReadScope},
	AuthTypes:   []string{"user"},
	HasFormat:   true,
	Flags: append([]common.Flag{
		{Name: "feed-group-id", Desc: "feed group ID (ofg_xxx); path parameter (required)"},
		{Name: "page-size", Type: "int", Default: "50", Desc: "page size (1-50)"},
		{Name: "page-token", Desc: "pagination token for next page"},
		{Name: "start-time", Desc: "update-time window start (Unix milliseconds as a decimal string)"},
		{Name: "end-time", Desc: "update-time window end (Unix milliseconds as a decimal string)"},
	}, imPaginationFlags(imReadDefaultPageLimit)...),
	Tips: []string{
		`Example: lark-cli im +feed-group-list-item --feed-group-id <feed_group_id> --as user`,
	},
	Validate: func(ctx context.Context, runtime *common.RuntimeContext) error {
		return validateFeedGroupListOptions(runtime)
	},
	DryRun: func(ctx context.Context, runtime *common.RuntimeContext) *common.DryRunAPI {
		if err := validateFeedGroupListOptions(runtime); err != nil {
			return common.NewDryRunAPI().Set("error", err.Error())
		}
		return common.NewDryRunAPI().
			GET(feedGroupListItemPath(runtime)).
			Params(feedGroupListDryRunParams(runtime)).
			Desc("will also POST /open-apis/im/v1/chats/batch_query to resolve chat_name from feed_id; requires im:chat:read")
	},
	Execute: func(ctx context.Context, runtime *common.RuntimeContext) error {
		return executeFeedGroupListAllPages(runtime)
	},
}

func validateFeedGroupListOptions(rt *common.RuntimeContext) error {
	if rt.Str("feed-group-id") == "" {
		return errs.NewValidationError(errs.SubtypeInvalidArgument, "--feed-group-id is required").WithParam("--feed-group-id")
	}
	if n := rt.Int("page-size"); n < 1 || n > 50 {
		return errs.NewValidationError(errs.SubtypeInvalidArgument, "--page-size must be an integer between 1 and 50").WithParam("--page-size")
	}
	if n := rt.Int("page-limit"); n > 1000 {
		return errs.NewValidationError(errs.SubtypeInvalidArgument, "--page-limit must be at most 1000 (0 = unlimited)").WithParam("--page-limit")
	}
	if v := rt.Str("start-time"); v != "" {
		if _, err := strconv.ParseInt(v, 10, 64); err != nil {
			return errs.NewValidationError(errs.SubtypeInvalidArgument, "--start-time must be Unix milliseconds (a decimal integer string)").WithParam("--start-time")
		}
	}
	if v := rt.Str("end-time"); v != "" {
		if _, err := strconv.ParseInt(v, 10, 64); err != nil {
			return errs.NewValidationError(errs.SubtypeInvalidArgument, "--end-time must be Unix milliseconds (a decimal integer string)").WithParam("--end-time")
		}
	}
	return validateIMPagination(rt)
}

// feedGroupListItemPath builds the list_item endpoint path with the feed_group_id
// segment safely encoded.
func feedGroupListItemPath(rt *common.RuntimeContext) string {
	return "/open-apis/im/v1/groups/" + validate.EncodePathSegment(rt.Str("feed-group-id")) + "/list_item"
}

// feedGroupListQuery builds the query parameters, sending only non-empty values.
func feedGroupListQuery(rt *common.RuntimeContext) larkcore.QueryParams {
	params := larkcore.QueryParams{
		"page_size": []string{strconv.Itoa(rt.Int("page-size"))},
	}
	if token := rt.Str("page-token"); token != "" {
		params["page_token"] = []string{token}
	}
	if start := rt.Str("start-time"); start != "" {
		params["start_time"] = []string{start}
	}
	if end := rt.Str("end-time"); end != "" {
		params["end_time"] = []string{end}
	}
	return params
}

// feedGroupListDryRunParams mirrors feedGroupListQuery for dry-run display.
func feedGroupListDryRunParams(rt *common.RuntimeContext) map[string]any {
	params := map[string]any{
		"page_size": strconv.Itoa(rt.Int("page-size")),
	}
	if token := rt.Str("page-token"); token != "" {
		params["page_token"] = token
	}
	if start := rt.Str("start-time"); start != "" {
		params["start_time"] = start
	}
	if end := rt.Str("end-time"); end != "" {
		params["end_time"] = end
	}
	return params
}

// executeFeedGroupListAllPages fetches all pages and merges items/deleted_items
// into a single response, then enriches the merged result.
func executeFeedGroupListAllPages(rt *common.RuntimeContext) error {
	pages, status, pageErr := paginateIM(rt, func(pageToken string) (map[string]any, error) {
		params := larkcore.QueryParams{
			"page_size": []string{strconv.Itoa(rt.Int("page-size"))},
		}
		if pageToken != "" {
			params["page_token"] = []string{pageToken}
		}
		if start := rt.Str("start-time"); start != "" {
			params["start_time"] = []string{start}
		}
		if end := rt.Str("end-time"); end != "" {
			params["end_time"] = []string{end}
		}

		return rt.DoAPIJSONTyped("GET", feedGroupListItemPath(rt), params, nil)
	})
	if len(pages) == 0 {
		return pageErr
	}

	rt.RecordPagination(status)
	merged := mergeIMPageArrays(pages, "items", "deleted_items")
	enrichFeedGroupItemsChatName(rt, merged)

	lastHasMore, _ := merged["has_more"].(bool)
	rt.OutFormat(merged, nil, func(w io.Writer) {
		renderFeedGroupItemsTable(w, merged, lastHasMore)
	})
	return nil
}
