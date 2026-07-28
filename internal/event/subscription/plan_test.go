// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/model"
	lark "github.com/larksuite/cli/internal/event/platform/lark"
)

// ---- fixtures ----

func activeSub(id string, includeResourceData bool, authorityType string) lark.RemoteSubscription {
	ird := includeResourceData
	return lark.RemoteSubscription{
		ID:                    model.RemoteSubscriptionID(id),
		EventType:             "im.message.created_v1",
		TargetResource:        "im.message?chat_id=oc_aaa",
		Authority:             model.RemoteAuthority{Type: authorityType, OpenID: "ou_aaa"},
		State:                 "active",
		PayloadOptionsPresent: true,
		IncludeResourceData:   &ird,
		Filter:                &event.Filter{},
	}
}

func suspendedSub(id, reason string) lark.RemoteSubscription {
	return lark.RemoteSubscription{
		ID:               model.RemoteSubscriptionID(id),
		EventType:        "im.message.created_v1",
		TargetResource:   "im.message?chat_id=oc_aaa",
		Authority:        model.RemoteAuthority{Type: "user", OpenID: "ou_aaa"},
		State:            "suspended",
		SuspensionReason: reason,
		Filter:           &event.Filter{},
	}
}

func stateSub(id, state string) lark.RemoteSubscription {
	return lark.RemoteSubscription{
		ID:             model.RemoteSubscriptionID(id),
		EventType:      "im.message.created_v1",
		TargetResource: "im.message?chat_id=oc_aaa",
		Authority:      model.RemoteAuthority{Type: "user", OpenID: "ou_aaa"},
		State:          state,
		Filter:         &event.Filter{},
	}
}

// sampleFilter / filterAlt are two distinct, non-empty filters (Equal is false
// between them) built via the exported FilterNode fields.
func sampleFilter() *event.Filter {
	return &event.Filter{Root: &event.FilterNode{
		LogicOp:  "and",
		Children: []*event.FilterNode{{Condition: &event.FilterCond{Operand: "message_type", Op: "eq", Value: "image"}}},
	}}
}

func filterAlt() *event.Filter {
	return &event.Filter{Root: &event.FilterNode{
		LogicOp:  "and",
		Children: []*event.FilterNode{{Condition: &event.FilterCond{Operand: "message_type", Op: "eq", Value: "text"}}},
	}}
}

func completeObs(subs ...lark.RemoteSubscription) Observation {
	return Observation{Completeness: Complete, Matches: subs}
}

func req(includeResourceData bool, filter *event.Filter) Request {
	return Request{
		EventType:           "im.message.created_v1",
		TargetResource:      "im.message?chat_id=oc_aaa",
		IncludeResourceData: includeResourceData,
		Filter:              filter,
	}
}

// ---- fakeProber: GetEncryptKey seam for the encrypted-active probe ----

type fakeProber struct {
	key   string
	err   error
	calls int
}

func (f *fakeProber) GetEncryptKey(_ context.Context, _ string) (string, error) {
	f.calls++
	return f.key, f.err
}

// noProber constructs a Planner whose Policy never probes.
func noProber() Planner { return NewPlanner(nil) }

// ---- Planner: state table ----

func TestPlan_NotFound_ReturnsCreate(t *testing.T) {
	plan, err := noProber().Plan(context.Background(), completeObs(), ManagementCreate, req(false, nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != ActionCreate {
		t.Errorf("Action = %q, want %q", plan.Action, ActionCreate)
	}
	if plan.Before != nil {
		t.Errorf("Before = %+v, want nil", plan.Before)
	}
}

func TestPlan_ActiveCompatible_ReturnsReuse(t *testing.T) {
	plan, err := noProber().Plan(context.Background(), completeObs(activeSub("sub_1", false, "user")), ManagementCreate, req(false, nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != ActionReuse {
		t.Errorf("Action = %q, want %q", plan.Action, ActionReuse)
	}
	if plan.Before == nil || plan.Before.ID.String() != "sub_1" {
		t.Errorf("Before = %+v, want sub_1", plan.Before)
	}
}

func TestPlan_ActiveIncludeResourceDataMismatch_ReturnsBlockWithField(t *testing.T) {
	// existing include_resource_data=true, request false.
	plan, err := noProber().Plan(context.Background(), completeObs(activeSub("sub_1", true, "user")), ManagementCreate, req(false, nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != ActionBlock {
		t.Fatalf("Action = %q, want %q", plan.Action, ActionBlock)
	}
	if len(plan.ConflictFields) != 1 || plan.ConflictFields[0].Name != "include_resource_data" {
		t.Errorf("ConflictFields = %+v, want one entry naming include_resource_data", plan.ConflictFields)
	}
	if plan.ConflictFields[0].Reason == "" {
		t.Error("ConflictFields[0].Reason must not be empty")
	}
}

func TestPlan_SuspendedManagementCreate_ReturnsBlockNoConflictFields(t *testing.T) {
	plan, err := noProber().Plan(context.Background(), completeObs(suspendedSub("sub_1", "authority_revoked")), ManagementCreate, req(false, nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != ActionBlock {
		t.Errorf("Action = %q, want %q (management create never overwrites a suspended match)", plan.Action, ActionBlock)
	}
	if len(plan.ConflictFields) != 0 {
		t.Errorf("ConflictFields = %+v, want none (a suspended block is not a config conflict)", plan.ConflictFields)
	}
	if plan.Before == nil || plan.Before.State != "suspended" {
		t.Errorf("Before = %+v, want the suspended match", plan.Before)
	}
}

func TestPlan_SuspendedConsumeBootstrap_ReturnsReactivate(t *testing.T) {
	plan, err := noProber().Plan(context.Background(), completeObs(suspendedSub("sub_1", "authority_revoked")), ConsumeBootstrap, req(false, nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != ActionReactivate {
		t.Errorf("Action = %q, want %q (consume bootstrap auto-resumes a compatible suspended match)", plan.Action, ActionReactivate)
	}
	if plan.Before == nil || plan.Before.ID.String() != "sub_1" {
		t.Errorf("Before = %+v, want sub_1", plan.Before)
	}
}

// TestPlan_ExpiredDeleted_TreatedAsCreate: a terminal remote state is inert --
// the subscription can be neither reused nor reactivated, so it is treated like
// not-found and a fresh one is created.
func TestPlan_ExpiredDeleted_TreatedAsCreate(t *testing.T) {
	for _, state := range []string{"expired", "deleted"} {
		t.Run(state, func(t *testing.T) {
			plan, err := noProber().Plan(context.Background(), completeObs(stateSub("sub_old", state)), ManagementCreate, req(false, nil))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if plan.Action != ActionCreate {
				t.Errorf("Action = %q, want %q (state %q is terminal/inert)", plan.Action, ActionCreate, state)
			}
		})
	}
}

// TestPlan_UnrecognizedState_ReturnsBlock_NeverCreate (fail-fast #5a): a matched
// subscription in a state this CLI does not recognize (the empty string or any
// unknown token) cannot be safely classified. It must fail closed -- Block,
// never Create (which could duplicate a still-live subscription) and never
// silently reuse/reactivate -- carrying the "state" conflict dimension and the
// matched subscription as Before, under every policy.
func TestPlan_UnrecognizedState_ReturnsBlock_NeverCreate(t *testing.T) {
	for _, state := range []string{"", "some_future_state", "pending", "paused"} {
		for _, policy := range []Policy{ManagementCreate, ConsumeBootstrap} {
			t.Run("state="+state+"/"+policy.Name(), func(t *testing.T) {
				plan, err := noProber().Plan(context.Background(), completeObs(stateSub("sub_weird", state)), policy, req(false, nil))
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if plan.Action != ActionBlock {
					t.Fatalf("Action = %q, want %q (an unrecognized state must never Create)", plan.Action, ActionBlock)
				}
				if !ConflictOnState(plan.ConflictFields) {
					t.Errorf("ConflictFields = %+v, want the state dimension", plan.ConflictFields)
				}
				if len(plan.ConflictFields) != 1 || plan.ConflictFields[0].Reason == "" {
					t.Errorf("ConflictFields = %+v, want one entry with a non-empty reason", plan.ConflictFields)
				}
				if plan.Before == nil || plan.Before.ID.String() != "sub_weird" {
					t.Errorf("Before = %+v, want the matched sub_weird", plan.Before)
				}
			})
		}
	}
}

// ---- Planner: Completeness=Indeterminate => never Create (must-fix) ----

func TestPlan_Indeterminate_ReturnsIndeterminate_NeverCreate(t *testing.T) {
	obs := Observation{Completeness: Indeterminate}
	for _, policy := range []Policy{ManagementCreate, ConsumeBootstrap} {
		t.Run(policy.Name(), func(t *testing.T) {
			plan, err := noProber().Plan(context.Background(), obs, policy, req(false, nil))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if plan.Action != ActionIndeterminate {
				t.Errorf("Action = %q, want %q (an inconclusive scan must never Create)", plan.Action, ActionIndeterminate)
			}
		})
	}
}

// ---- Planner: encryption conflict matrix (request includeResourceData=true) ----

func TestPlan_EncryptedProbe_UsableKey_ReturnsReuse(t *testing.T) {
	prober := &fakeProber{key: "usable-key-value"}
	plan, err := NewPlanner(prober).Plan(context.Background(), completeObs(activeSub("sub_enc", true, "user")), ManagementCreate, req(true, nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != ActionReuse {
		t.Errorf("Action = %q, want %q", plan.Action, ActionReuse)
	}
	if prober.calls != 1 {
		t.Errorf("prober.calls = %d, want 1", prober.calls)
	}
}

func TestPlan_EncryptedProbe_EmptyKeyOrError_ReturnsBlock(t *testing.T) {
	// The gateway maps an empty key to InvalidResponse, and any business/transport
	// failure surfaces as an error; both classify as a conflict here.
	cases := map[string]*fakeProber{
		"invalid_response_empty_key": {key: "", err: errors.New("subscription get_encrypt_key reported success but returned no encrypt_key")},
		"transport_error":            {key: "", err: errors.New("boom: permission denied")},
	}
	for name, prober := range cases {
		t.Run(name, func(t *testing.T) {
			plan, err := NewPlanner(prober).Plan(context.Background(), completeObs(activeSub("sub_enc", true, "user")), ManagementCreate, req(true, nil))
			if err != nil {
				t.Fatalf("unexpected error (a probe failure must classify as Block, not propagate): %v", err)
			}
			if plan.Action != ActionBlock {
				t.Errorf("Action = %q, want %q", plan.Action, ActionBlock)
			}
			if len(plan.ConflictFields) != 1 || plan.ConflictFields[0].Name != "include_resource_data" {
				t.Errorf("ConflictFields = %+v, want one entry naming include_resource_data", plan.ConflictFields)
			}
			if prober.calls != 1 {
				t.Errorf("prober.calls = %d, want 1", prober.calls)
			}
		})
	}
}

func TestPlan_EncryptedRemoteFalse_ReturnsBlock_NeverProbes(t *testing.T) {
	prober := &fakeProber{key: "usable-key-value"}
	plan, err := NewPlanner(prober).Plan(context.Background(), completeObs(activeSub("sub_plain", false, "user")), ManagementCreate, req(true, nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != ActionBlock {
		t.Errorf("Action = %q, want %q", plan.Action, ActionBlock)
	}
	if prober.calls != 0 {
		t.Errorf("prober.calls = %d, want 0 (the state mismatch is decisive)", prober.calls)
	}
}

func TestPlan_EncryptedProbingPolicy_NoProber_FailsClosed(t *testing.T) {
	plan, err := noProber().Plan(context.Background(), completeObs(activeSub("sub_enc", true, "user")), ManagementCreate, req(true, nil))
	if err == nil {
		t.Fatalf("expected a fail-closed internal error, got plan: %+v", plan)
	}
	if plan.Action == ActionReuse {
		t.Errorf("must never silently reuse without a prober, got %+v", plan)
	}
}

func TestPlan_EncryptedDeferred_ReusesWithoutProbing(t *testing.T) {
	// ConsumeBootstrap defers to the bus; no prober is consulted at all.
	prober := &fakeProber{key: "unused"}
	plan, err := NewPlanner(prober).Plan(context.Background(), completeObs(activeSub("sub_enc", true, "user")), ConsumeBootstrap, req(true, nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != ActionReuse {
		t.Errorf("Action = %q, want %q (deferred confirmation reuses without probing)", plan.Action, ActionReuse)
	}
	if prober.calls != 0 {
		t.Errorf("prober.calls = %d, want 0 (deferred path never probes)", prober.calls)
	}
}

// ---- Planner: filter reuse dimension (active) ----

func filteredActive(id, authorityType string, f *event.Filter) lark.RemoteSubscription {
	sub := activeSub(id, false, authorityType)
	sub.Filter = f
	return sub
}

func TestPlan_FilterMismatch_ReturnsBlockWithFilterField_NeverLeaks(t *testing.T) {
	plan, err := noProber().Plan(context.Background(), completeObs(filteredActive("sub_1", "user", sampleFilter())), ManagementCreate, req(false, filterAlt()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != ActionBlock {
		t.Fatalf("Action = %q, want %q", plan.Action, ActionBlock)
	}
	if len(plan.ConflictFields) != 1 || plan.ConflictFields[0].Name != "filter" {
		t.Fatalf("ConflictFields = %+v, want one entry naming filter", plan.ConflictFields)
	}
	for _, leak := range []string{"message_type", "image", "text"} {
		if strings.Contains(plan.ConflictFields[0].Reason, leak) {
			t.Errorf("Reason must not leak filter contents (%q): %q", leak, plan.ConflictFields[0].Reason)
		}
	}
}

func TestPlan_FilterEqual_ReturnsReuse(t *testing.T) {
	plan, err := noProber().Plan(context.Background(), completeObs(filteredActive("sub_1", "user", sampleFilter())), ManagementCreate, req(false, sampleFilter()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != ActionReuse {
		t.Errorf("Action = %q, want %q (identical filter is compatible)", plan.Action, ActionReuse)
	}
}

func TestPlan_EmptyRequestedVsFilteredRemote_ReturnsBlock(t *testing.T) {
	plan, err := noProber().Plan(context.Background(), completeObs(filteredActive("sub_1", "user", sampleFilter())), ManagementCreate, req(false, nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != ActionBlock || !ConflictOnFilter(plan.ConflictFields) {
		t.Errorf("Action = %q fields=%+v, want Block on filter", plan.Action, plan.ConflictFields)
	}
}

func TestPlan_FilteredRequestedVsEmptyRemote_ReturnsBlock(t *testing.T) {
	plan, err := noProber().Plan(context.Background(), completeObs(activeSub("sub_1", false, "user")), ManagementCreate, req(false, sampleFilter()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != ActionBlock || !ConflictOnFilter(plan.ConflictFields) {
		t.Errorf("Action = %q fields=%+v, want Block on filter", plan.Action, plan.ConflictFields)
	}
}

func TestPlan_BothEmptyFilter_ReturnsReuse(t *testing.T) {
	plan, err := noProber().Plan(context.Background(), completeObs(activeSub("sub_1", false, "user")), ManagementCreate, req(false, &event.Filter{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != ActionReuse {
		t.Errorf("Action = %q, want %q (both empty is compatible)", plan.Action, ActionReuse)
	}
}

func TestPlan_EncryptedFilterMismatch_ReturnsBlock_NeverProbes(t *testing.T) {
	prober := &fakeProber{key: "usable-key-value"}
	match := activeSub("sub_enc", true, "user")
	match.Filter = sampleFilter()
	plan, err := NewPlanner(prober).Plan(context.Background(), completeObs(match), ManagementCreate, req(true, filterAlt()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != ActionBlock || !ConflictOnFilter(plan.ConflictFields) {
		t.Fatalf("Action = %q fields=%+v, want Block on filter", plan.Action, plan.ConflictFields)
	}
	if prober.calls != 0 {
		t.Errorf("prober.calls = %d, want 0 (a filter mismatch is decisive before the probe)", prober.calls)
	}
}

func TestPlan_EncryptedDeferred_FilterMismatch_ReturnsBlock(t *testing.T) {
	match := activeSub("sub_enc", true, "user")
	match.Filter = sampleFilter()
	plan, err := noProber().Plan(context.Background(), completeObs(match), ConsumeBootstrap, req(true, filterAlt()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != ActionBlock || !ConflictOnFilter(plan.ConflictFields) {
		t.Fatalf("Action = %q fields=%+v, want Block on filter", plan.Action, plan.ConflictFields)
	}
}

// ---- Planner: suspended-path reuse dimensions ----

func TestPlan_Suspended_FilterMismatch_ReturnsBlock_NeverLeaks(t *testing.T) {
	match := suspendedSub("sub_1", "authority_revoked")
	match.Filter = sampleFilter()
	plan, err := noProber().Plan(context.Background(), completeObs(match), ConsumeBootstrap, req(false, filterAlt()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != ActionBlock {
		t.Fatalf("Action = %q, want %q (an incompatible suspended sub must not be silently reactivated)", plan.Action, ActionBlock)
	}
	if len(plan.ConflictFields) != 1 || plan.ConflictFields[0].Name != "filter" {
		t.Fatalf("ConflictFields = %+v, want one entry naming filter", plan.ConflictFields)
	}
	for _, leak := range []string{"message_type", "image", "text"} {
		if strings.Contains(plan.ConflictFields[0].Reason, leak) {
			t.Errorf("Reason must not leak filter contents (%q): %q", leak, plan.ConflictFields[0].Reason)
		}
	}
}

func TestPlan_Suspended_FilterEqual_ConsumeReactivates(t *testing.T) {
	match := suspendedSub("sub_1", "authority_revoked")
	match.Filter = sampleFilter()
	plan, err := noProber().Plan(context.Background(), completeObs(match), ConsumeBootstrap, req(false, sampleFilter()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != ActionReactivate {
		t.Errorf("Action = %q, want %q (an equal filter keeps the reactivate-reuse eligible)", plan.Action, ActionReactivate)
	}
}

func TestPlan_Suspended_IncludeResourceDataMismatch_ReturnsBlock(t *testing.T) {
	match := suspendedSub("sub_1", "authority_revoked")
	present := true
	match.PayloadOptionsPresent = true
	match.IncludeResourceData = &present // existing true, request false
	plan, err := noProber().Plan(context.Background(), completeObs(match), ConsumeBootstrap, req(false, nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Action != ActionBlock {
		t.Fatalf("Action = %q, want %q", plan.Action, ActionBlock)
	}
	if len(plan.ConflictFields) != 1 || plan.ConflictFields[0].Name != "include_resource_data" {
		t.Fatalf("ConflictFields = %+v, want one entry naming include_resource_data", plan.ConflictFields)
	}
}
