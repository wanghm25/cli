// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package lark

import (
	"bytes"
	"testing"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/internal/event"
)

func strPtr(s string) *string { return &s }
func intPtr(i int) *int       { return &i }
func boolPtr(b bool) *bool    { return &b }

// TestProjectSubscription_FullDetail_MapsEveryField locks the SDK ->
// RemoteSubscription projection (the mapping that moved here from the command
// layer's mapSubscriptionDetail).
func TestProjectSubscription_FullDetail_MapsEveryField(t *testing.T) {
	d := &larkeventv1.SubscriptionDetail{
		SubscriptionId: strPtr("sub_abc"),
		Authority:      &larkeventv1.Authority{Type: strPtr("user"), OpenId: strPtr("ou_xxx")},
		TargetResource: strPtr("im.message?chat_id=oc_xxx"),
		EventType:      strPtr("im.message.created_v1"),
		PayloadOptions: &larkeventv1.PayloadOptions{IncludeResourceData: boolPtr(true)},
		State:          strPtr("suspended"),
		Suspension:     &larkeventv1.Suspension{Code: strPtr("authority_revoked")},
		ExpireTime:     intPtr(1732000000),
		CreateTime:     intPtr(1730000000),
		UpdateTime:     intPtr(1731000000),
	}

	sub := ProjectSubscription(d)

	if sub.ID.String() != "sub_abc" {
		t.Errorf("ID = %q, want sub_abc", sub.ID)
	}
	if sub.EventType != "im.message.created_v1" {
		t.Errorf("EventType = %q, want im.message.created_v1", sub.EventType)
	}
	if sub.TargetResource != "im.message?chat_id=oc_xxx" {
		t.Errorf("TargetResource = %q, want im.message?chat_id=oc_xxx", sub.TargetResource)
	}
	if sub.Authority.String() != "user:ou_xxx" {
		t.Errorf("Authority.String() = %q, want user:ou_xxx", sub.Authority.String())
	}
	if !sub.PayloadOptionsPresent || sub.IncludeResourceData == nil || !*sub.IncludeResourceData {
		t.Errorf("payload options = (present=%v, %v), want (true, true)", sub.PayloadOptionsPresent, sub.IncludeResourceData)
	}
	if sub.State != "suspended" {
		t.Errorf("State = %q, want suspended", sub.State)
	}
	if sub.SuspensionReason != "authority_revoked" {
		t.Errorf("SuspensionReason = %q, want authority_revoked", sub.SuspensionReason)
	}
	if sub.ExpireTime == nil || *sub.ExpireTime != 1732000000 {
		t.Errorf("ExpireTime = %v, want 1732000000", sub.ExpireTime)
	}
	if sub.CreateTime == nil || *sub.CreateTime != 1730000000 {
		t.Errorf("CreateTime = %v, want 1730000000", sub.CreateTime)
	}
	if sub.UpdateTime == nil || *sub.UpdateTime != 1731000000 {
		t.Errorf("UpdateTime = %v, want 1731000000", sub.UpdateTime)
	}
}

// TestProjectSubscription_MinimalDetail_LeavesOptionalsAbsent locks that an
// omitted payload_options / suspension / authority projects to the absent form
// (never a fabricated present-but-empty value).
func TestProjectSubscription_MinimalDetail_LeavesOptionalsAbsent(t *testing.T) {
	sub := ProjectSubscription(&larkeventv1.SubscriptionDetail{
		SubscriptionId: strPtr("sub_min"),
		EventType:      strPtr("im.message.receive_v1"),
	})
	if sub.PayloadOptionsPresent {
		t.Error("PayloadOptionsPresent = true, want false when the SDK omitted payload_options")
	}
	if sub.IncludeResourceData != nil {
		t.Errorf("IncludeResourceData = %v, want nil", sub.IncludeResourceData)
	}
	if sub.SuspensionReason != "" {
		t.Errorf("SuspensionReason = %q, want empty", sub.SuspensionReason)
	}
	if !sub.Authority.IsZero() {
		t.Errorf("Authority = %+v, want zero", sub.Authority)
	}
	if !sub.Filter.IsEmpty() {
		t.Error("Filter should be empty when the SDK omitted it")
	}
}

func TestProjectSubscription_Nil_ReturnsZeroValue(t *testing.T) {
	if got := ProjectSubscription(nil); got.ID.String() != "" || got.EventType != "" || !got.Filter.IsEmpty() {
		t.Errorf("ProjectSubscription(nil) = %+v, want zero value", got)
	}
}

// TestProjectSubscription_WithFilter_ProjectsCLIModel proves the remote filter
// projects to the CLI Filter model (whose canonical form matches the wire form).
func TestProjectSubscription_WithFilter_ProjectsCLIModel(t *testing.T) {
	f, err := event.ParseAndValidateFilter(
		`{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"message_type","op":"in","list_value":["text"]}}]}}`,
		event.FilterMetaFor("im.message.created_v1"))
	if err != nil {
		t.Fatalf("build filter: %v", err)
	}
	sub := ProjectSubscription(&larkeventv1.SubscriptionDetail{
		SubscriptionId: strPtr("sub_f"),
		Filter:         FilterToSDK(f),
	})
	if sub.Filter.IsEmpty() {
		t.Fatal("sub.Filter is empty, want the projected remote filter")
	}
	got, err := sub.Filter.Canonicalize()
	if err != nil {
		t.Fatalf("canonicalize projected: %v", err)
	}
	want, err := f.Canonicalize()
	if err != nil {
		t.Fatalf("canonicalize want: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("projected filter = %s, want %s", got, want)
	}
}

func init() {
	// Seed im.message.created_v1's filter capability so the filter-projection
	// test above can parse a valid filter, mirroring the command package's own
	// minimal fixture.
	event.RegisterFilterMeta("im.message.created_v1", event.FilterMeta{
		Supported:     true,
		LogicOps:      []string{"and", "or"},
		Operators:     []string{"eq", "in", "contains"},
		MaxDepth:      2,
		MaxConditions: 10,
		MaxBytes:      1024,
		Operands: []event.FilterOperandMeta{
			{Key: "message_type", Operators: []string{"eq", "in"}, ListValueMax: 10},
		},
	})
}
