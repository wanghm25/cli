// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package app

import (
	"reflect"
	"testing"
)

// fakePlane records the order StatusUseCase.Collect drives the steps and
// returns a canned capped value from Supplement.
type fakePlane struct {
	order  []string
	capped bool
}

func (p *fakePlane) Derive()                  { p.order = append(p.order, "derive") }
func (p *fakePlane) AnnotateCurrentIdentity() { p.order = append(p.order, "annotate") }
func (p *fakePlane) Supplement() bool {
	p.order = append(p.order, "supplement")
	return p.capped
}

// TestStatusUseCase_Collect_OrdersStepsAndBubblesCapped locks the read-only
// orchestration order (derive -> annotate -> supplement) and that Supplement's
// capped advisory is returned to the caller.
func TestStatusUseCase_Collect_OrdersStepsAndBubblesCapped(t *testing.T) {
	p := &fakePlane{capped: true}
	capped := NewStatusUseCase().Collect(p)

	if want := []string{"derive", "annotate", "supplement"}; !reflect.DeepEqual(p.order, want) {
		t.Errorf("step order = %v, want %v", p.order, want)
	}
	if !capped {
		t.Error("Collect must return the plane's capped advisory (true), got false")
	}
}
