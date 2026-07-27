// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package app

// StatusPlane is the set of read-only orchestration steps StatusUseCase.Collect
// sequences for `event status`. The command implements it over its bus process
// scanner, status querier, current-identity load, and weak remote supplement,
// so this use case owns only the ORDER of the read-only status flow — without
// depending on the command's concrete status model or the Factory.
//
// This is deliberately a thin relocation of the orchestration sequence out of
// the command; the heavier status→pure-projection rework (returning a rendered
// projection rather than mutating a shared status set in place) is a later PR.
type StatusPlane interface {
	// Derive classifies each seeded/scanned app as running/orphan/not_running
	// from the bus socket + process scan. First step; it establishes the plane's
	// status set that the next two steps annotate in place.
	Derive()
	// AnnotateCurrentIdentity stamps the freshly-resolved current identity onto
	// the one status row that is the current app, for later owner-vs-current
	// matching. Local, no scope, no network.
	AnnotateCurrentIdentity()
	// Supplement applies the weak, optional, current-app-only remote read that
	// fills refined consumers' remote_state/expire_time/etc. capped reports
	// whether the bounded List scan hit its page cap before every wanted id was
	// found — an advisory the caller surfaces, never a hard failure.
	Supplement() (capped bool)
}

// StatusUseCase owns the read-only `event status` orchestration: it sequences a
// StatusPlane's Derive → AnnotateCurrentIdentity → Supplement steps. It performs
// no I/O, holds no Factory, and renders nothing — the command builds the plane
// over its bus/remote ports and renders the result.
type StatusUseCase struct{}

// NewStatusUseCase builds a StatusUseCase.
func NewStatusUseCase() StatusUseCase { return StatusUseCase{} }

// Collect runs the status orchestration in order and returns whether the weak
// remote supplement's List scan was capped (an advisory for the caller). The
// derived/annotated/supplemented status set lives on the plane the command
// supplied.
func (StatusUseCase) Collect(plane StatusPlane) (capped bool) {
	plane.Derive()
	plane.AnnotateCurrentIdentity()
	return plane.Supplement()
}
