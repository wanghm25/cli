// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
)

// rowFromDetail maps d through the exact same getSubscription seam
// runDelete uses, so tests build a *subscriptionRow fixture identically to
// how production code obtains "before" rather than hand-constructing one
// that could drift from the real mapping. update itself no longer needs
// this (its rejection is pure local, with no remote read), but
// delete_test.go still does, and this file was its one definition site —
// kept here for that.
func rowFromDetail(t *testing.T, d *larkeventv1.SubscriptionDetail) *subscriptionRow {
	t.Helper()
	fake := &fakeGetAPI{resp: okGetResp(d)}
	row, err := getSubscription(context.Background(), fake, strVal(d.SubscriptionId))
	if err != nil {
		t.Fatalf("rowFromDetail: unexpected error: %v", err)
	}
	return row
}

// ---- runUpdate wiring (cobra-level) ----
//
// update has no successful path: the platform's update API is filter-only
// now, and this CLI does not support filter updates yet. Every case below
// must be rejected before any identity/scope/network work — each uses a
// zero-value *cmdutil.Factory (no config, credential, or client wiring), so
// any attempt to resolve an identity or build a network-capable client
// would panic or surface an unrelated error instead of the typed
// errs.ValidationError asserted below.

func TestRunUpdate_EmptyID_RejectedBeforeNetwork(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdUpdate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"", "--include-resource-data=false"})

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

func TestRunUpdate_MissingIncludeResourceDataFlag_ReturnsInvalidArgument(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdUpdate(f)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"sub_1"}) // --include-resource-data never passed

	err := cmd.Execute()
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeInvalidArgument {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeInvalidArgument)
	}
	if ve.Param != "--include-resource-data" {
		t.Errorf("Param = %q, want --include-resource-data", ve.Param)
	}
}

// TestRunUpdate_IncludeResourceData_AlwaysRejectedWithNoNetworkCall locks the
// filter-only SDK's consequence: include_resource_data can never be changed
// via update in either direction anymore, so both explicit values return the
// same typed not-updatable failed_precondition, purely locally — no remote
// Get, no confirmation gate, no client ever built.
func TestRunUpdate_IncludeResourceData_AlwaysRejectedWithNoNetworkCall(t *testing.T) {
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
			// Guides to delete + recreate, after a human confirms, naming
			// this remote_subscription_id and echoing back the caller's own
			// requested value.
			for _, want := range []string{"delete", "create", "confirm", "sub_1", "--include-resource-data=" + value} {
				if !strings.Contains(ve.Hint, want) {
					t.Errorf("Hint = %q, want it to mention %q", ve.Hint, want)
				}
			}
		})
	}
}

func TestNewCmdUpdate_HasExpectedFlags(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdUpdate(f)
	if cmd.Flags().Lookup("include-resource-data") == nil {
		t.Error("NewCmdUpdate missing --include-resource-data flag")
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
