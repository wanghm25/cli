// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"context"
	"errors"
	"testing"

	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/event/model"
)

// fakeWalker emulates the gateway's bounded, early-stopping List scan: it visits
// items in order, stops as soon as visit returns false (so a found match is never
// "capped"), and otherwise reports cappedIfExhausted once every item is visited.
type fakeWalker struct {
	items           []model.RemoteSubscription
	cappedIfExhaust bool
	err             error
	lastParams      ListParams
}

func (f *fakeWalker) WalkSubscriptions(_ context.Context, params ListParams, visit func(model.RemoteSubscription) bool) (bool, error) {
	f.lastParams = params
	if f.err != nil {
		return false, f.err
	}
	for _, it := range f.items {
		if !visit(it) {
			return false, nil // stopped early -> not capped
		}
	}
	return f.cappedIfExhaust, nil
}

func TestObserve_AuthorityMatch_Found_Complete(t *testing.T) {
	w := &fakeWalker{items: []model.RemoteSubscription{
		activeSub("sub_other", false, "app"), // wrong authority for AsUser
		activeSub("sub_1", false, "user"),
	}}
	obs, err := NewObserver(w).Observe(context.Background(), Request{EventType: "im.message.created_v1", TargetResource: "im.message?chat_id=oc_aaa", Identity: core.AsUser})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if obs.Completeness != Complete {
		t.Errorf("Completeness = %v, want Complete", obs.Completeness)
	}
	m := obs.authorityMatch()
	if m == nil || m.ID.String() != "sub_1" {
		t.Errorf("match = %+v, want sub_1", m)
	}
	if w.lastParams.EventType != "im.message.created_v1" || w.lastParams.TargetResource != "im.message?chat_id=oc_aaa" {
		t.Errorf("List params = %+v, want event_type+target_resource forwarded", w.lastParams)
	}
}

func TestObserve_AuthorityMismatch_NoMatch_Complete(t *testing.T) {
	w := &fakeWalker{items: []model.RemoteSubscription{activeSub("sub_app", false, "app")}}
	obs, err := NewObserver(w).Observe(context.Background(), Request{Identity: core.AsUser})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if obs.Completeness != Complete || obs.authorityMatch() != nil {
		t.Errorf("obs = %+v, want Complete with no match (an app authority is not the user's)", obs)
	}
}

func TestObserve_BotMatchesAppAuthority(t *testing.T) {
	w := &fakeWalker{items: []model.RemoteSubscription{activeSub("sub_app", false, "app")}}
	obs, err := NewObserver(w).Observe(context.Background(), Request{Identity: core.AsBot})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m := obs.authorityMatch(); m == nil || m.ID.String() != "sub_app" {
		t.Errorf("match = %+v, want sub_app for a bot identity", m)
	}
}

func TestObserve_NoMatch_Capped_Indeterminate(t *testing.T) {
	w := &fakeWalker{items: []model.RemoteSubscription{activeSub("sub_app", false, "app")}, cappedIfExhaust: true}
	obs, err := NewObserver(w).Observe(context.Background(), Request{Identity: core.AsUser})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if obs.Completeness != Indeterminate {
		t.Errorf("Completeness = %v, want Indeterminate (capped scan, no match)", obs.Completeness)
	}
	if obs.authorityMatch() != nil {
		t.Errorf("match = %+v, want nil on a capped no-match", obs.authorityMatch())
	}
}

func TestObserve_NoMatch_Exhausted_Complete(t *testing.T) {
	w := &fakeWalker{items: []model.RemoteSubscription{activeSub("sub_app", false, "app")}, cappedIfExhaust: false}
	obs, err := NewObserver(w).Observe(context.Background(), Request{Identity: core.AsUser})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if obs.Completeness != Complete {
		t.Errorf("Completeness = %v, want Complete (every page genuinely read, no match)", obs.Completeness)
	}
}

func TestObserve_TransportError_Propagates(t *testing.T) {
	sentinel := errors.New("boom: connection reset")
	_, err := NewObserver(&fakeWalker{err: sentinel}).Observe(context.Background(), Request{Identity: core.AsUser})
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want it passed through unchanged (%v)", err, sentinel)
	}
}

func TestAuthorityMatchesIdentity(t *testing.T) {
	cases := []struct {
		typ      string
		identity core.Identity
		want     bool
	}{
		{"user", core.AsUser, true},
		{"app", core.AsBot, true},
		{"app", core.AsUser, false},
		{"user", core.AsBot, false},
		{"", core.AsUser, false},
	}
	for _, c := range cases {
		if got := authorityMatchesIdentity(model.RemoteAuthority{Type: c.typ}, c.identity); got != c.want {
			t.Errorf("authorityMatchesIdentity(%q, %v) = %v, want %v", c.typ, c.identity, got, c.want)
		}
	}
}
