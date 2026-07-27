// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/credential"
	eventlib "github.com/larksuite/cli/internal/event"
	larkgw "github.com/larksuite/cli/internal/event/platform/lark"
)

// rowFromDetail maps an SDK SubscriptionDetail through the same
// gateway-projection + row-mapper the migrated commands use, so tests build a
// *subscriptionRow fixture identically to how production code obtains "before"
// rather than hand-constructing one that could drift from the real mapping.
// delete_test.go uses it; this file is its one definition site.
func rowFromDetail(t *testing.T, d *larkeventv1.SubscriptionDetail) *subscriptionRow {
	t.Helper()
	row := mapRemoteSubscription(larkgw.ProjectSubscription(d))
	return &row
}

// ---- fixtures ----

// sampleUpdateFilterJSON is a wire-valid filter for im.message.created_v1 (the
// event_type activeDetail reports): a single message_type=text condition.
const sampleUpdateFilterJSON = `{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"message_type","op":"eq","value":"text"}}]}}`

// mustParseCreatedFilter parses raw against im.message.created_v1's filter
// capability, failing the test on any error — for fixtures that need a valid
// *eventlib.Filter to seed a remote subscription or compare against.
func mustParseCreatedFilter(t *testing.T, raw string) *eventlib.Filter {
	t.Helper()
	f, err := eventlib.ParseAndValidateFilter(raw, eventlib.FilterMetaFor("im.message.created_v1"))
	if err != nil {
		t.Fatalf("mustParseCreatedFilter: unexpected error: %v", err)
	}
	return f
}

// activeSubWithFilter is activeSub carrying an existing server-side filter
// (projected exactly as a gateway read would surface it), for the no-op /
// clear-a-present-filter cases.
func activeSubWithFilter(id string, f *eventlib.Filter) larkgw.RemoteSubscription {
	d := activeDetail(id, false, "user")
	d.Filter = eventlib.FilterToSDK(f)
	return larkgw.ProjectSubscription(d)
}

// ---- fake updateSubscriptionAPI ----

// fakeUpdateAPI is a network-free stand-in for the platform/lark gateway's
// Get+Patch — the updateSubscriptionAPI test seam. Get hands back a domain
// RemoteSubscription; Patch captures the PatchSpec and returns one.
type fakeUpdateAPI struct {
	getSub *larkgw.RemoteSubscription
	getErr error

	patchFunc  func(larkgw.PatchSpec) (*larkgw.RemoteSubscription, error)
	patchCalls int
	patchSpec  larkgw.PatchSpec
}

func (f *fakeUpdateAPI) Get(_ context.Context, _ string) (*larkgw.RemoteSubscription, error) {
	return f.getSub, f.getErr
}

func (f *fakeUpdateAPI) Patch(_ context.Context, _ string, spec larkgw.PatchSpec) (*larkgw.RemoteSubscription, error) {
	f.patchCalls++
	f.patchSpec = spec
	if f.patchFunc == nil {
		return subPtr(activeSub("sub_1", false, "user")), nil
	}
	return f.patchFunc(spec)
}

// The Patch request body's filter projection (set filter / {"filter":{}} clear
// form) now lives at the gateway — see platform/lark's
// TestBuildPatchBody_SetFilter/ClearFilter; the gateway also validates a Patch
// response's required fields (TestGateway_Patch_NilData_ReturnsInvalidResponse).

// ---- doUpdateSubscription ----

func TestDoUpdateSubscription_CallsPatchAndReturnsSubscription(t *testing.T) {
	fake := &fakeUpdateAPI{patchFunc: func(larkgw.PatchSpec) (*larkgw.RemoteSubscription, error) {
		return subPtr(activeSub("sub_1", false, "user")), nil
	}}

	sub, err := doUpdateSubscription(context.Background(), fake, "sub_1", &eventlib.Filter{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.patchCalls != 1 {
		t.Errorf("patchCalls = %d, want 1", fake.patchCalls)
	}
	if sub.ID.String() != "sub_1" {
		t.Errorf("sub.ID = %q, want sub_1", sub.ID)
	}
}

func TestDoUpdateSubscription_TransportError_PropagatesUnchanged(t *testing.T) {
	sentinel := errors.New("boom: connection reset")
	fake := &fakeUpdateAPI{patchFunc: func(larkgw.PatchSpec) (*larkgw.RemoteSubscription, error) { return nil, sentinel }}

	_, err := doUpdateSubscription(context.Background(), fake, "sub_1", &eventlib.Filter{})
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want it passed through unchanged (%v)", err, sentinel)
	}
}

// ---- applyUpdate (read + decide + write core, against the fake) ----

func TestApplyUpdate_SetFilter_PatchesWhenChanged(t *testing.T) {
	fake := &fakeUpdateAPI{getSub: subPtr(activeSub("sub_1", false, "user"))}

	err := applyUpdate(context.Background(), fake, io.Discard, "sub_1", core.AsUser,
		updateOpts{filter: sampleUpdateFilterJSON})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.patchCalls != 1 {
		t.Errorf("patchCalls = %d, want 1 (a real filter change must patch)", fake.patchCalls)
	}
}

func TestApplyUpdate_MalformedFilterJSON_RejectedNoPatch(t *testing.T) {
	fake := &fakeUpdateAPI{getSub: subPtr(activeSub("sub_1", false, "user"))}

	err := applyUpdate(context.Background(), fake, io.Discard, "sub_1", core.AsUser,
		updateOpts{filter: `{"composite_condition":`})
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeInvalidArgument {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeInvalidArgument)
	}
	if ve.Param != "--filter" {
		t.Errorf("Param = %q, want --filter", ve.Param)
	}
	if fake.patchCalls != 0 {
		t.Errorf("patchCalls = %d, want 0 (an invalid filter must never patch)", fake.patchCalls)
	}
}

// TestApplyUpdate_InvalidFilterRule_RejectedNoPatch_NeverLeaksValue locks two
// things at once: a rule violation is a typed invalid_argument on --filter
// with no Patch, and the rejected filter's own value never leaks into the
// error text (the never-leak guarantee for filter contents).
func TestApplyUpdate_InvalidFilterRule_RejectedNoPatch_NeverLeaksValue(t *testing.T) {
	const secret = "sekret-not-an-openid"
	// sender requires an open_id value; this one is not, so it is rejected by
	// rule (not by JSON parsing).
	raw := `{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"sender","op":"eq","value":"` + secret + `"}}]}}`
	fake := &fakeUpdateAPI{getSub: subPtr(activeSub("sub_1", false, "user"))}

	err := applyUpdate(context.Background(), fake, io.Discard, "sub_1", core.AsUser,
		updateOpts{filter: raw})
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Param != "--filter" {
		t.Errorf("Param = %q, want --filter", ve.Param)
	}
	if fake.patchCalls != 0 {
		t.Errorf("patchCalls = %d, want 0", fake.patchCalls)
	}
	if strings.Contains(ve.Error(), secret) || strings.Contains(ve.Hint, secret) {
		t.Errorf("error text leaked the filter value %q: %v (hint=%q)", secret, ve.Error(), ve.Hint)
	}
}

func TestApplyUpdate_ClearFilter_PatchesWhenFilterPresent(t *testing.T) {
	present := mustParseCreatedFilter(t, sampleUpdateFilterJSON)
	fake := &fakeUpdateAPI{getSub: subPtr(activeSubWithFilter("sub_1", present))}

	err := applyUpdate(context.Background(), fake, io.Discard, "sub_1", core.AsUser,
		updateOpts{clearFilter: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.patchCalls != 1 {
		t.Errorf("patchCalls = %d, want 1 (clearing a present filter must patch)", fake.patchCalls)
	}
}

// TestApplyUpdate_ClearFilter_NoOpWhenAlreadyEmpty locks that clearing an
// already-unfiltered subscription is a no-op (no Patch) and that its message
// says the subscription already has no filter — not the --filter no-op's
// "already has the requested filter" wording — mirroring the same
// --clear-filter/--filter distinction updateDryRunNextAction already makes
// for the --dry-run no-op case.
func TestApplyUpdate_ClearFilter_NoOpWhenAlreadyEmpty(t *testing.T) {
	fake := &fakeUpdateAPI{getSub: subPtr(activeSub("sub_1", false, "user"))} // no filter
	var buf bytes.Buffer

	err := applyUpdate(context.Background(), fake, &buf, "sub_1", core.AsUser,
		updateOpts{clearFilter: true, asJSON: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.patchCalls != 0 {
		t.Errorf("patchCalls = %d, want 0 (clearing an already-unfiltered subscription is a no-op)", fake.patchCalls)
	}

	var generic map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &generic); err != nil {
		t.Fatalf("json.Unmarshal: %v; raw: %s", err, buf.Bytes())
	}
	nextAction, _ := generic["next_action"].(string)
	if !strings.Contains(nextAction, "already has no filter") {
		t.Errorf("next_action = %q, want it to say the subscription already has no filter", nextAction)
	}
	if strings.Contains(nextAction, "requested filter") {
		t.Errorf(`next_action = %q, must not use the --filter no-op's "requested filter" wording for a --clear-filter no-op`, nextAction)
	}
}

func TestApplyUpdate_SetFilter_NoOpWhenEqualsCurrent(t *testing.T) {
	current := mustParseCreatedFilter(t, sampleUpdateFilterJSON)
	fake := &fakeUpdateAPI{getSub: subPtr(activeSubWithFilter("sub_1", current))}

	err := applyUpdate(context.Background(), fake, io.Discard, "sub_1", core.AsUser,
		updateOpts{filter: sampleUpdateFilterJSON})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.patchCalls != 0 {
		t.Errorf("patchCalls = %d, want 0 (desired filter equals current — no-op)", fake.patchCalls)
	}
}

func TestApplyUpdate_DryRun_ReadsButDoesNotPatch(t *testing.T) {
	fake := &fakeUpdateAPI{getSub: subPtr(activeSub("sub_1", false, "user"))}
	var buf bytes.Buffer

	err := applyUpdate(context.Background(), fake, &buf, "sub_1", core.AsUser,
		updateOpts{filter: sampleUpdateFilterJSON, dryRun: true, asJSON: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.patchCalls != 0 {
		t.Errorf("patchCalls = %d, want 0: --dry-run must never issue a write", fake.patchCalls)
	}

	var generic map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &generic); err != nil {
		t.Fatalf("json.Unmarshal dry-run output: %v; raw: %s", err, buf.Bytes())
	}
	for _, field := range []string{"operation", "dry_run", "remote_subscription_id", "required_scopes", "preflight", "remote_before", "planned_change", "local_impact", "next_action"} {
		if _, ok := generic[field]; !ok {
			t.Errorf("dry-run JSON missing field %q; got: %s", field, buf.Bytes())
		}
	}
	if generic["operation"] != "update" {
		t.Errorf(`"operation" = %v, want "update"`, generic["operation"])
	}
	if generic["dry_run"] != true {
		t.Errorf(`"dry_run" = %v, want true`, generic["dry_run"])
	}
	pc, _ := generic["planned_change"].(map[string]interface{})
	if pc["action"] != "update" {
		t.Errorf(`planned_change.action = %v, want "update"`, pc["action"])
	}
	// A real filter change affects any running local consumer's stream, so the
	// dry-run must disclose that rather than assert none.
	li, _ := generic["local_impact"].(map[string]interface{})
	if li["local_consumer_affected"] != true {
		t.Errorf("local_impact.local_consumer_affected = %v, want true for a real filter change", li["local_consumer_affected"])
	}
}

// TestApplyUpdate_DryRun_NoOp_ReportsNoopAction locks that a --dry-run whose
// requested filter already matches previews a "noop" plan (and still never
// patches).
func TestApplyUpdate_DryRun_NoOp_ReportsNoopAction(t *testing.T) {
	current := mustParseCreatedFilter(t, sampleUpdateFilterJSON)
	fake := &fakeUpdateAPI{getSub: subPtr(activeSubWithFilter("sub_1", current))}
	var buf bytes.Buffer

	err := applyUpdate(context.Background(), fake, &buf, "sub_1", core.AsUser,
		updateOpts{filter: sampleUpdateFilterJSON, dryRun: true, asJSON: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.patchCalls != 0 {
		t.Errorf("patchCalls = %d, want 0", fake.patchCalls)
	}
	var generic map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &generic); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	pc, _ := generic["planned_change"].(map[string]interface{})
	if pc["action"] != "noop" {
		t.Errorf(`planned_change.action = %v, want "noop" for an already-matching filter`, pc["action"])
	}
	// A no-op changes nothing, so it must not claim local-consumer impact.
	li, _ := generic["local_impact"].(map[string]interface{})
	if li["local_consumer_affected"] != false {
		t.Errorf("local_impact.local_consumer_affected = %v, want false for a no-op", li["local_consumer_affected"])
	}
}

// TestApplyUpdate_EventTypeComesFromGet proves --filter is validated against
// the FETCHED subscription's event_type (update carries no EventKey): the same
// filter that is valid for im.message.created_v1 is rejected when the fetched
// detail reports an event_type with no filter capability — and nothing is
// patched.
func TestApplyUpdate_EventTypeComesFromGet(t *testing.T) {
	sub := activeSub("sub_1", false, "user")
	sub.EventType = "im.message.receive_v1" // no filter capability
	fake := &fakeUpdateAPI{getSub: &sub}

	err := applyUpdate(context.Background(), fake, io.Discard, "sub_1", core.AsUser,
		updateOpts{filter: sampleUpdateFilterJSON})
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Param != "--filter" {
		t.Errorf("Param = %q, want --filter", ve.Param)
	}
	if !strings.Contains(ve.Error(), "does not support") {
		t.Errorf("Error() = %q, want it to say the event type does not support --filter", ve.Error())
	}
	if fake.patchCalls != 0 {
		t.Errorf("patchCalls = %d, want 0", fake.patchCalls)
	}
}

func TestApplyUpdate_MissingEventType_TypedError(t *testing.T) {
	sub := activeSub("sub_1", false, "user")
	sub.EventType = ""
	fake := &fakeUpdateAPI{getSub: &sub}

	err := applyUpdate(context.Background(), fake, io.Discard, "sub_1", core.AsUser,
		updateOpts{clearFilter: true})
	if _, ok := errs.ProblemOf(err); !ok {
		t.Fatalf("expected a typed errs.* error, got %T: %v", err, err)
	}
	if fake.patchCalls != 0 {
		t.Errorf("patchCalls = %d, want 0", fake.patchCalls)
	}
}

func TestApplyUpdate_GetError_Propagates(t *testing.T) {
	sentinel := errors.New("boom: subscription not found")
	fake := &fakeUpdateAPI{getErr: sentinel}

	err := applyUpdate(context.Background(), fake, io.Discard, "sub_1", core.AsUser,
		updateOpts{clearFilter: true})
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want the Get error passed through unchanged (%v)", err, sentinel)
	}
	if fake.patchCalls != 0 {
		t.Errorf("patchCalls = %d, want 0", fake.patchCalls)
	}
}

// ---- runUpdate wiring (cobra-level) ----
//
// Every rejection below must short-circuit before any identity/scope/network
// work — each uses a zero-value *cmdutil.Factory (no config, credential, or
// client wiring), so any attempt to resolve an identity or build a
// network-capable client would panic or surface an unrelated error instead of
// the typed error asserted here.

func TestRunUpdate_EmptyID_RejectedBeforeNetwork(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdUpdate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"", "--clear-filter"})

	err := cmd.Execute()
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeInvalidArgument {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeInvalidArgument)
	}
	if ve.Param != "remote_subscription_id" {
		t.Errorf("Param = %q, want remote_subscription_id", ve.Param)
	}
}

// TestRunUpdate_IncludeResourceData_RejectedBeforeNetwork locks that trying to
// change include_resource_data via update is refused, in either direction,
// with a typed failed_precondition — purely locally, before any
// identity/scope/network step (it short-circuits ahead of even the filter-flag
// checks).
func TestRunUpdate_IncludeResourceData_RejectedBeforeNetwork(t *testing.T) {
	for _, value := range []string{"true", "false"} {
		t.Run(value, func(t *testing.T) {
			f := &cmdutil.Factory{}
			cmd := NewCmdUpdate(f)
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetArgs([]string{"sub_1", "--include-resource-data=" + value})

			err := cmd.Execute()
			var ve *errs.ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
			}
			if ve.Subtype != errs.SubtypeFailedPrecondition {
				t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
			}
			if ve.Param != "--include-resource-data" {
				t.Errorf("Param = %q, want --include-resource-data", ve.Param)
			}
			if !strings.Contains(ve.Error(), "include_resource_data") {
				t.Errorf("Error() = %q, want it to mention include_resource_data", ve.Error())
			}
			for _, want := range []string{"delete", "create", "confirm", "sub_1", "--include-resource-data=" + value} {
				if !strings.Contains(ve.Hint, want) {
					t.Errorf("Hint = %q, want it to mention %q", ve.Hint, want)
				}
			}
		})
	}
}

func TestRunUpdate_BothFilterFlags_RejectedBeforeNetwork(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdUpdate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"sub_1", "--filter", sampleUpdateFilterJSON, "--clear-filter"})

	err := cmd.Execute()
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeInvalidArgument {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeInvalidArgument)
	}
	if ve.Param != "--filter" {
		t.Errorf("Param = %q, want --filter", ve.Param)
	}
}

func TestRunUpdate_NoFilterFlag_RejectedBeforeNetwork(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdUpdate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"sub_1"}) // neither --filter nor --clear-filter

	err := cmd.Execute()
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeInvalidArgument {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeInvalidArgument)
	}
	if ve.Param != "--filter" {
		t.Errorf("Param = %q, want --filter", ve.Param)
	}
}

// TestRunUpdate_BlankFilterValue_RejectedBeforeNetwork locks that --filter
// given an explicit but blank value (e.g. --filter "$UNSET_VAR" expanding to
// "" in a script) is rejected as a caller mistake, purely locally, before any
// identity/scope/network step — like its sibling mutual-exclusion checks
// above, it reaches the scope preflight (and Get/Patch) only past this check,
// so a zero-value *cmdutil.Factory proves no network call was attempted.
// Left unrejected, this would fall through to ParseAndValidateFilter, which
// accepts "" as a valid empty filter with no error — silently clearing a
// currently-filtered subscription instead of surfacing the caller's mistake.
func TestRunUpdate_BlankFilterValue_RejectedBeforeNetwork(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdUpdate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"sub_1", "--filter", ""})

	err := cmd.Execute()
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeInvalidArgument {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeInvalidArgument)
	}
	if ve.Param != "--filter" {
		t.Errorf("Param = %q, want --filter", ve.Param)
	}
}

// TestRunUpdate_MissingWriteScope_ReturnsPermissionError locks that update —
// like renew/reactivate/delete — hard-requires event:subscription:write on top
// of read (it reads the current filter before patching). It reaches the scope
// preflight only past the local flag checks, so it passes --clear-filter.
func TestRunUpdate_MissingWriteScope_ReturnsPermissionError(t *testing.T) {
	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{AppID: "cli_x"})
	f.Credential = credential.NewCredentialProvider(nil, nil, &fakeTokenResolver{
		result: &credential.TokenResult{Token: "u-tok", Scopes: "event:subscription:read"},
	}, nil)

	cmd := NewCmdUpdate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"sub_1", "--clear-filter", "--as", "user"})

	err := cmd.Execute()
	var permErr *errs.PermissionError
	if !errors.As(err, &permErr) {
		t.Fatalf("expected *errs.PermissionError, got %T: %v", err, err)
	}
	if len(permErr.MissingScopes) != 1 || permErr.MissingScopes[0] != "event:subscription:write" {
		t.Errorf("MissingScopes = %v, want [event:subscription:write]", permErr.MissingScopes)
	}
}

// TestNewCmdUpdate_HasExpectedFlags locks update's flag surface: the two
// filter flags, the kept-for-a-clear-rejection --include-resource-data,
// --dry-run/--json/--as, no --yes (update is not confirmation-gated), and
// risk=write.
func TestNewCmdUpdate_HasExpectedFlags(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdUpdate(f)
	for _, name := range []string{"filter", "clear-filter", "include-resource-data", "dry-run", "json", "as"} {
		if cmd.Flags().Lookup(name) == nil {
			t.Errorf("NewCmdUpdate missing --%s flag", name)
		}
	}
	if cmd.Flags().Lookup("yes") != nil {
		t.Error("NewCmdUpdate must not expose --yes (a filter change is reversible, not confirmation-gated)")
	}
	if level, ok := cmdutil.GetRisk(cmd); !ok || level != cmdutil.RiskWrite {
		t.Errorf("risk = (%q, %v), want (%q, true)", level, ok, cmdutil.RiskWrite)
	}
}

// TestNewCmdSubscription_RegistersUpdateAsWrite mirrors create_test.go's
// TestNewCmdSubscription_RegistersCreateAsWrite: it locks that update is
// registered in the subscription group (without disturbing list/get/create)
// and carries risk=write.
func TestNewCmdSubscription_RegistersUpdateAsWrite(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdSubscription(f)

	var update *cobra.Command
	for _, c := range cmd.Commands() {
		if c.Name() == "update" {
			update = c
		}
	}
	if update == nil {
		t.Fatal(`subscription command group missing "update" subcommand`)
	}
	level, ok := cmdutil.GetRisk(update)
	if !ok || level != cmdutil.RiskWrite {
		t.Errorf(`"update" risk = (%q, %v), want (%q, true)`, level, ok, cmdutil.RiskWrite)
	}
}
