// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package im

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/output"
	"github.com/larksuite/cli/shortcuts/common"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
)

const feedGroupListPath = "/open-apis/im/v1/groups"

// ImFeedGroupList provides the +feed-group-list shortcut: it lists the caller's
// feed groups (tags) with auto-pagination that correctly merges BOTH the live
// (groups) and soft-deleted (deleted_groups) lists across pages.
//
// The raw `feed.groups list --page-all` goes through the generic paginator,
// which follows only one array field and silently drops the other list's later
// pages; this shortcut paginates the dual-list response itself.
var ImFeedGroupList = common.Shortcut{
	Service:     "im",
	Command:     "+feed-group-list",
	Description: "List the caller's feed groups (tags); user-only; supports `--page-all` auto-pagination",
	Risk:        "read",
	UserScopes:  []string{feedGroupReadScope},
	AuthTypes:   []string{"user"},
	HasFormat:   true,
	Flags: append([]common.Flag{
		{Name: "page-size", Type: "int", Default: "50", Desc: "page size (1-50)"},
		{Name: "page-token", Desc: "pagination token for next page"},
		{Name: "start-time", Desc: "update-time window start (Unix milliseconds as a decimal string)"},
		{Name: "end-time", Desc: "update-time window end (Unix milliseconds as a decimal string)"},
	}, imPaginationFlags(imReadDefaultPageLimit)...),
	Validate: func(ctx context.Context, runtime *common.RuntimeContext) error {
		return validateFeedGroupListPageOptions(runtime)
	},
	DryRun: func(ctx context.Context, runtime *common.RuntimeContext) *common.DryRunAPI {
		if err := validateFeedGroupListPageOptions(runtime); err != nil {
			return common.NewDryRunAPI().Set("error", err.Error())
		}
		return common.NewDryRunAPI().
			GET(feedGroupListPath).
			Params(feedGroupListGroupsDryRunParams(runtime))
	},
	Execute: func(ctx context.Context, runtime *common.RuntimeContext) error {
		return executeFeedGroupListGroupsAllPages(runtime)
	},
}

func validateFeedGroupListPageOptions(rt *common.RuntimeContext) error {
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

// feedGroupListGroupsQuery builds the query parameters. page_token is always
// sent (empty string = first page) because the groups endpoint rejects requests
// that omit it (HTTP 400 "Missing required parameter: page_token").
func feedGroupListGroupsQuery(rt *common.RuntimeContext) larkcore.QueryParams {
	params := larkcore.QueryParams{
		"page_size":  []string{strconv.Itoa(rt.Int("page-size"))},
		"page_token": []string{rt.Str("page-token")},
	}
	if start := rt.Str("start-time"); start != "" {
		params["start_time"] = []string{start}
	}
	if end := rt.Str("end-time"); end != "" {
		params["end_time"] = []string{end}
	}
	return params
}

// feedGroupListGroupsDryRunParams mirrors feedGroupListGroupsQuery for dry-run display.
func feedGroupListGroupsDryRunParams(rt *common.RuntimeContext) map[string]any {
	params := map[string]any{
		"page_size":  strconv.Itoa(rt.Int("page-size")),
		"page_token": rt.Str("page-token"),
	}
	if start := rt.Str("start-time"); start != "" {
		params["start_time"] = start
	}
	if end := rt.Str("end-time"); end != "" {
		params["end_time"] = end
	}
	return params
}

// executeFeedGroupListGroupsAllPages fetches all pages and merges both the live
// (groups) and soft-deleted (deleted_groups) lists into a single response. It
// merges each array independently so neither list loses its later pages.
func executeFeedGroupListGroupsAllPages(rt *common.RuntimeContext) error {
	pages, status, pageErr := paginateIM(rt, func(pageToken string) (map[string]any, error) {
		params := larkcore.QueryParams{
			"page_size":  []string{strconv.Itoa(rt.Int("page-size"))},
			"page_token": []string{pageToken},
		}
		if start := rt.Str("start-time"); start != "" {
			params["start_time"] = []string{start}
		}
		if end := rt.Str("end-time"); end != "" {
			params["end_time"] = []string{end}
		}

		return rt.DoAPIJSONTyped("GET", feedGroupListPath, params, nil)
	})
	if len(pages) == 0 {
		return pageErr
	}

	rt.RecordPagination(status)
	merged := mergeIMPageArrays(pages, "groups", "deleted_groups")
	lastHasMore, _ := merged["has_more"].(bool)
	rt.OutFormat(merged, nil, func(w io.Writer) {
		renderFeedGroupsTable(w, merged, lastHasMore)
	})
	return nil
}

// renderFeedGroupsTable prints the active groups[] as a table (group_id / name /
// type), followed by a summary line. When hasMore is true a pagination hint is
// appended; when there are deleted groups their count is noted.
func renderFeedGroupsTable(w io.Writer, data map[string]any, hasMore bool) {
	groups, _ := data["groups"].([]any)
	rows := make([]map[string]interface{}, 0, len(groups))
	for _, g := range groups {
		m, _ := g.(map[string]any)
		if m == nil {
			continue
		}
		id, _ := m["group_id"].(string)
		name, _ := m["name"].(string)
		typ, _ := m["type"].(string)
		rows = append(rows, map[string]interface{}{
			"group_id": id,
			"name":     name,
			"type":     typ,
		})
	}
	output.PrintTable(w, rows)

	moreHint := ""
	if hasMore {
		moreHint = " (more available, use --page-token to fetch next page)"
	}
	fmt.Fprintf(w, "\n%d group(s)%s\n", len(groups), moreHint)

	if deleted, _ := data["deleted_groups"].([]any); len(deleted) > 0 {
		fmt.Fprintf(w, "(%d deleted)\n", len(deleted))
	}
}
