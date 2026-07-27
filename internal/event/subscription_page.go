// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"context"
	"fmt"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"
)

// MaxSubscriptionListPages bounds how many List pages WalkSubscriptionPages
// reads before giving up. Every caller filters server-side to one CLI-
// relevant scope (the subscription Observer's event_type+target_resource, or one
// app's own Subscription set for status's unfiltered supplement read), so a
// page count in the low tens already covers realistic fan-out; this cap
// exists purely to bound worst-case latency/cost against a pathological
// account, never as a normal stopping point.
const MaxSubscriptionListPages = 10

// SubscriptionLister is the narrow read-only seam WalkSubscriptionPages depends
// on: a single List call. It is enough to scan existing remote Subscription
// state without ever writing, so it is safe to call from a --dry-run / plan-only
// preflight. The platform/lark gateway's identity-bound SDK client satisfies it
// structurally — no explicit "implements" declaration needed, Go interfaces are
// structural.
type SubscriptionLister interface {
	List(ctx context.Context, req *larkeventv1.ListSubscriptionReq) (*larkeventv1.ListSubscriptionResp, error)
}

// SubscriptionPageRequestFunc builds the ListSubscriptionReq for one page:
// pageToken is "" for the first page and, for every later page, whatever the
// previous page's response returned as its own page_token. Implementations
// close over their own filters (event_type/target_resource/state, or none at
// all for an unfiltered List) and add PageToken(pageToken) only when it is
// non-empty.
type SubscriptionPageRequestFunc func(pageToken string) *larkeventv1.ListSubscriptionReq

// WalkSubscriptionPages calls visit once for every non-nil SubscriptionDetail
// svc.List returns, paging via has_more/page_token, until one of:
//   - visit returns false (the caller found what it needed);
//   - a page reports has_more=false, or has_more=true with no page_token to
//     continue (exhausted -- capped stays false either way);
//   - MaxSubscriptionListPages pages have been read with has_more still true
//     and visit never returning false (capped=true: the caller MUST treat
//     this as "no definitive answer within the pages read", never as
//     "confirmed absent", and should log it rather than silently truncating);
//     or
//   - ctx is done BETWEEN pages (err is ctx.Err(), no further List call is
//     made).
//
// The FIRST page's List call always goes out regardless of ctx state --
// cancellation is only ever checked before requesting a SECOND or later page,
// i.e. genuinely mid-pagination. This deliberately leaves a single-page scan
// byte-for-byte as sensitive to ctx as a plain, unpaginated svc.List call
// always was: that one request's own success/failure (typed by svc.List's
// own implementation, e.g. SubscriptionClient's error classification) is
// what callers see, never a raw, unwrapped ctx.Err() substituted ahead of it.
//
// This is the one paginated-List seam every caller that may need more than a
// single page shares: the subscription Observer (find the first authority match)
// and status's remote supplement (collect every wanted remote_subscription_id)
// both walk through it instead of each open-coding their own page loop.
func WalkSubscriptionPages(ctx context.Context, svc SubscriptionLister, buildReq SubscriptionPageRequestFunc, visit func(*larkeventv1.SubscriptionDetail) bool) (capped bool, err error) {
	pageToken := ""
	for page := 0; page < MaxSubscriptionListPages; page++ {
		resp, err := svc.List(ctx, buildReq(pageToken))
		if err != nil {
			return false, err
		}
		if resp == nil || resp.Data == nil {
			return false, nil
		}
		for _, item := range resp.Data.Items {
			if item == nil {
				continue
			}
			if !visit(item) {
				return false, nil
			}
		}
		if !boolVal(resp.Data.HasMore) {
			return false, nil
		}
		next := strVal(resp.Data.PageToken)
		if next == "" {
			// has_more=true but the server gave us nothing to continue
			// with -- treat as exhausted rather than re-requesting the same
			// (now token-less) page forever.
			return false, nil
		}
		// About to fetch another page -- honor cancellation now rather than
		// firing off a doomed request for it.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, ctxErr
		}
		pageToken = next
	}
	return true, nil
}

// PaginationCappedWarning is the fixed advisory a caller should log when a
// WalkSubscriptionPages-backed scan (the subscription Observer's Indeterminate
// completeness, or status's remote-supplement capped result) hit MaxSubscriptionListPages
// before finding what it was looking for or exhausting every page. This is
// NOT confirmed absence -- callers must never read a capped scan as proof
// that nothing exists, only that nothing was found within the pages actually
// read.
func PaginationCappedWarning(eventType, targetResource string) string {
	return fmt.Sprintf("[event] warning: subscription list scan for event_type=%s target_resource=%s stopped at the %d-page cap before finding a match or exhausting all results -- this is NOT confirmed absence, only \"no match found within the pages read\"",
		eventType, targetResource, MaxSubscriptionListPages)
}

func strVal(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func boolVal(b *bool) bool {
	return b != nil && *b
}
