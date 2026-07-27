// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package im

import (
	"strconv"
	"time"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/client"
	"github.com/larksuite/cli/shortcuts/common"
)

const imReadDefaultPageLimit = 20

type imPageFetcher func(pageToken string) (map[string]any, error)

func imPaginationFlags(defaultLimit int) []common.Flag {
	if defaultLimit < 0 {
		defaultLimit = imReadDefaultPageLimit
	}
	return []common.Flag{
		{Name: "page-all", Type: "bool", Desc: "automatically paginate through all pages"},
		{Name: "page-limit", Type: "int", Default: strconv.Itoa(defaultLimit), Desc: "maximum pages fetched with --page-all (0 = unlimited)"},
	}
}

func validateIMPagination(runtime *common.RuntimeContext) error {
	if runtime.Int("page-limit") < 0 {
		return errs.NewValidationError(
			errs.SubtypeInvalidArgument,
			"--page-limit must be a non-negative integer",
		).WithParam("--page-limit")
	}
	if runtime.Cmd.Flags().Lookup("page-delay") != nil && runtime.Int("page-delay") < 0 {
		return errs.NewValidationError(
			errs.SubtypeInvalidArgument,
			"--page-delay must be a non-negative integer",
		).WithParam("--page-delay")
	}
	return nil
}

// paginateIM walks an IM shortcut's pages without interpreting whether the
// result is complete. It returns every successful page plus neutral pagination
// facts; the IM contract session owns output, hint, and exit-code semantics.
func paginateIM(runtime *common.RuntimeContext, fetch imPageFetcher) ([]map[string]any, client.PaginationStatus, error) {
	return paginateIMWithMode(runtime, runtime.Bool("page-all"), fetch)
}

func paginateIMWithMode(runtime *common.RuntimeContext, autoPaginate bool, fetch imPageFetcher) ([]map[string]any, client.PaginationStatus, error) {
	startToken := runtime.Str("page-token")
	pageAll := autoPaginate && startToken == ""
	pageLimit := runtime.Int("page-limit")
	pageDelay := 0
	if runtime.Cmd.Flags().Lookup("page-delay") != nil {
		pageDelay = runtime.Int("page-delay")
	}

	pages := make([]map[string]any, 0, 1)
	status := client.PaginationStatus{}
	requestToken := startToken
	seenTokens := make(map[string]struct{})
	if startToken != "" {
		seenTokens[startToken] = struct{}{}
	}

	for {
		page, err := fetch(requestToken)
		if err != nil {
			status.Cause = err
			status.StopReason = paginationErrorStopReason(err)
			return pages, status, err
		}
		pages = append(pages, page)
		status.PagesFetched = len(pages)
		status.HasMore, status.NextPageToken = common.PaginationMeta(page)

		if explicitlyTruncated(page) {
			status.StopReason = client.StopReasonServerTruncation
			return pages, status, nil
		}
		if !status.HasMore {
			if startToken != "" {
				status.StopReason = client.StopReasonStartPageToken
				return pages, status, nil
			}
			status.StopReason = client.StopReasonExhausted
			return pages, status, nil
		}
		if status.NextPageToken == "" {
			err := errs.NewInternalError(
				errs.SubtypeInvalidResponse,
				"paginated response has_more=true but next page token is missing",
			)
			status.Cause = err
			status.StopReason = client.StopReasonMissingToken
			return pages, status, err
		}
		if _, repeated := seenTokens[status.NextPageToken]; repeated {
			err := errs.NewInternalError(
				errs.SubtypeInvalidResponse,
				"paginated response repeated the same next page token",
			)
			status.Cause = err
			status.StopReason = client.StopReasonRepeatedToken
			return pages, status, err
		}
		if startToken != "" {
			status.StopReason = client.StopReasonStartPageToken
			return pages, status, nil
		}
		if !pageAll {
			status.StopReason = client.StopReasonSinglePage
			return pages, status, nil
		}
		if pageLimit > 0 && status.PagesFetched >= pageLimit {
			status.StopReason = client.StopReasonPageLimit
			return pages, status, nil
		}

		requestToken = status.NextPageToken
		seenTokens[requestToken] = struct{}{}
		if pageDelay > 0 {
			time.Sleep(time.Duration(pageDelay) * time.Millisecond)
		}
	}
}

func paginationErrorStopReason(err error) client.StopReason {
	problem, ok := errs.ProblemOf(err)
	if ok && problem.Category == errs.CategoryNetwork {
		return client.StopReasonTransportError
	}
	return client.StopReasonAPIError
}

func explicitlyTruncated(page map[string]any) bool {
	if truncated, _ := page["truncated"].(bool); truncated {
		return true
	}
	switch truncations := page["truncations"].(type) {
	case []any:
		return len(truncations) > 0
	case []map[string]any:
		return len(truncations) > 0
	default:
		return false
	}
}

// mergeIMPageArrays preserves the first page's non-pagination metadata, merges
// every named array bucket, and carries the final page's cursor state.
func mergeIMPageArrays(pages []map[string]any, arrayFields ...string) map[string]any {
	merged := make(map[string]any)
	if len(pages) == 0 {
		for _, field := range arrayFields {
			merged[field] = []any{}
		}
		return merged
	}
	for key, value := range pages[0] {
		merged[key] = value
	}
	for _, field := range []string{"has_more", "page_token", "next_page_token"} {
		delete(merged, field)
	}
	for _, field := range arrayFields {
		items := make([]any, 0)
		for _, page := range pages {
			if pageItems, ok := page[field].([]any); ok {
				items = append(items, pageItems...)
			}
		}
		merged[field] = items
	}
	last := pages[len(pages)-1]
	if value, ok := last["has_more"]; ok {
		merged["has_more"] = value
	}
	if value, ok := last["page_token"]; ok {
		merged["page_token"] = value
	}
	if value, ok := last["next_page_token"]; ok {
		merged["next_page_token"] = value
	}
	return merged
}

func cloneQueryParams(source map[string][]string) map[string][]string {
	cloned := make(map[string][]string, len(source))
	for key, values := range source {
		cloned[key] = append([]string(nil), values...)
	}
	return cloned
}
