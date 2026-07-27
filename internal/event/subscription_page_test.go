// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"context"
	"errors"
	"fmt"
	"testing"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"
)

// These tests exercise WalkSubscriptionPages directly (the bounded, ctx-aware
// paginated List seam every remote-Subscription scan shares). The pagination /
// early-stop / cap / ctx-cancel behaviors were previously covered only through
// the retired ReconcileExisting; they live on here against their actual owner.

func strPtr(s string) *string { return &s }
func boolPtr(b bool) *bool    { return &b }

// walkDetail is a minimal SubscriptionDetail carrying just an id.
func walkDetail(id string) *larkeventv1.SubscriptionDetail {
	return &larkeventv1.SubscriptionDetail{SubscriptionId: strPtr(id)}
}

// pageResp builds one page with explicit has_more/page_token.
func pageResp(items []*larkeventv1.SubscriptionDetail, hasMore bool, nextToken string) *larkeventv1.ListSubscriptionResp {
	d := &larkeventv1.ListSubscriptionRespData{Items: items, HasMore: boolPtr(hasMore)}
	if nextToken != "" {
		d.PageToken = strPtr(nextToken)
	}
	return &larkeventv1.ListSubscriptionResp{Data: d}
}

// pagedLister serves a response per call (0-indexed by call order) via pageAt,
// so a test can drive WalkSubscriptionPages across multiple pages — including an
// unbounded has_more sequence for the page-cap test. cancel/cancelAfterCall
// optionally cancel a context.CancelFunc right after serving a given call, to
// test ctx cancellation mid-pagination.
type pagedLister struct {
	pageAt          func(call int) (*larkeventv1.ListSubscriptionResp, error)
	calls           int
	cancel          context.CancelFunc
	cancelAfterCall int
}

func (p *pagedLister) List(_ context.Context, _ *larkeventv1.ListSubscriptionReq) (*larkeventv1.ListSubscriptionResp, error) {
	call := p.calls
	p.calls++
	if p.cancel != nil && call == p.cancelAfterCall {
		p.cancel()
	}
	return p.pageAt(call)
}

// buildReq is a trivial SubscriptionPageRequestFunc for these tests.
func buildReq(pageToken string) *larkeventv1.ListSubscriptionReq {
	b := larkeventv1.NewListSubscriptionReqBuilder()
	if pageToken != "" {
		b = b.PageToken(pageToken)
	}
	return b.Build()
}

func TestWalkSubscriptionPages_VisitsAllItemsAcrossPages_NotCapped(t *testing.T) {
	page1 := pageResp([]*larkeventv1.SubscriptionDetail{walkDetail("a")}, true, "tok-2")
	page2 := pageResp([]*larkeventv1.SubscriptionDetail{walkDetail("b")}, false, "")
	fake := &pagedLister{pageAt: func(call int) (*larkeventv1.ListSubscriptionResp, error) {
		if call == 0 {
			return page1, nil
		}
		return page2, nil
	}}

	var seen []string
	capped, err := WalkSubscriptionPages(context.Background(), fake, buildReq, func(d *larkeventv1.SubscriptionDetail) bool {
		seen = append(seen, strVal(d.SubscriptionId))
		return true
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capped {
		t.Error("capped = true, want false (has_more=false exhausted every page)")
	}
	if len(seen) != 2 || seen[0] != "a" || seen[1] != "b" {
		t.Errorf("visited %v, want [a b] across both pages", seen)
	}
	if fake.calls != 2 {
		t.Errorf("List called %d times, want 2", fake.calls)
	}
}

func TestWalkSubscriptionPages_StopsWhenVisitReturnsFalse_NotCapped(t *testing.T) {
	page1 := pageResp([]*larkeventv1.SubscriptionDetail{walkDetail("a"), walkDetail("stop"), walkDetail("never")}, true, "tok-2")
	fake := &pagedLister{pageAt: func(int) (*larkeventv1.ListSubscriptionResp, error) { return page1, nil }}

	var seen []string
	capped, err := WalkSubscriptionPages(context.Background(), fake, buildReq, func(d *larkeventv1.SubscriptionDetail) bool {
		id := strVal(d.SubscriptionId)
		seen = append(seen, id)
		return id != "stop"
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capped {
		t.Error("capped = true, want false (visit stopped the scan before the cap)")
	}
	if len(seen) != 2 || seen[1] != "stop" {
		t.Errorf("visited %v, want to stop at [a stop] without visiting 'never'", seen)
	}
	if fake.calls != 1 {
		t.Errorf("List called %d times, want 1 (stopped on page 1)", fake.calls)
	}
}

func TestWalkSubscriptionPages_HitsPageCap_ReturnsCapped(t *testing.T) {
	fake := &pagedLister{pageAt: func(call int) (*larkeventv1.ListSubscriptionResp, error) {
		return pageResp([]*larkeventv1.SubscriptionDetail{walkDetail(fmt.Sprintf("s%d", call))}, true, fmt.Sprintf("tok-%d", call+1)), nil
	}}

	capped, err := WalkSubscriptionPages(context.Background(), fake, buildReq, func(*larkeventv1.SubscriptionDetail) bool { return true })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !capped {
		t.Error("capped = false, want true (an unbounded has_more sequence must terminate at the cap)")
	}
	if fake.calls != MaxSubscriptionListPages {
		t.Errorf("List called %d times, want exactly %d (bounded by the page cap)", fake.calls, MaxSubscriptionListPages)
	}
}

func TestWalkSubscriptionPages_HasMoreTrueButEmptyToken_Exhausted_NotCapped(t *testing.T) {
	// has_more=true but no page_token to continue: treated as exhausted, not
	// re-requested forever, and not capped.
	fake := &pagedLister{pageAt: func(int) (*larkeventv1.ListSubscriptionResp, error) {
		return pageResp([]*larkeventv1.SubscriptionDetail{walkDetail("a")}, true, ""), nil
	}}

	capped, err := WalkSubscriptionPages(context.Background(), fake, buildReq, func(*larkeventv1.SubscriptionDetail) bool { return true })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capped {
		t.Error("capped = true, want false (an empty continuation token exhausts the scan)")
	}
	if fake.calls != 1 {
		t.Errorf("List called %d times, want 1", fake.calls)
	}
}

func TestWalkSubscriptionPages_CtxCancelledMidPagination_ReturnsErr_NoFurtherCall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fake := &pagedLister{
		pageAt: func(call int) (*larkeventv1.ListSubscriptionResp, error) {
			return pageResp(nil, true, fmt.Sprintf("tok-%d", call+1)), nil
		},
		cancel:          cancel,
		cancelAfterCall: 0, // cancel right after the first page is served
	}

	capped, err := WalkSubscriptionPages(ctx, fake, buildReq, func(*larkeventv1.SubscriptionDetail) bool { return true })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if capped {
		t.Error("capped = true, want false on a cancellation error")
	}
	if fake.calls != 1 {
		t.Errorf("List called %d times, want exactly 1 (cancellation caught before a second call)", fake.calls)
	}
}

func TestWalkSubscriptionPages_TransportError_Propagates(t *testing.T) {
	sentinel := errors.New("boom: connection reset")
	fake := &pagedLister{pageAt: func(int) (*larkeventv1.ListSubscriptionResp, error) { return nil, sentinel }}

	_, err := WalkSubscriptionPages(context.Background(), fake, buildReq, func(*larkeventv1.SubscriptionDetail) bool { return true })
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want it passed through unchanged (%v)", err, sentinel)
	}
}

func TestWalkSubscriptionPages_NilData_ReturnsNotCapped(t *testing.T) {
	fake := &pagedLister{pageAt: func(int) (*larkeventv1.ListSubscriptionResp, error) {
		return &larkeventv1.ListSubscriptionResp{}, nil // nil Data
	}}

	capped, err := WalkSubscriptionPages(context.Background(), fake, buildReq, func(*larkeventv1.SubscriptionDetail) bool {
		t.Fatal("visit must not be called for a nil-Data response")
		return true
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capped {
		t.Error("capped = true, want false for a nil-Data response")
	}
}
