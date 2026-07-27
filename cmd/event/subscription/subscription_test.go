// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/credential"
	eventlib "github.com/larksuite/cli/internal/event"
)

// fakeTokenResolver is a network-free stand-in for credential.DefaultTokenResolver,
// mirroring shortcuts/common/runner_scope_test.go's scopeCheckTokenResolver so
// resolveUATAndCheckScopes is exercised without a real credential chain.
type fakeTokenResolver struct {
	result *credential.TokenResult
	err    error
}

func (r *fakeTokenResolver) ResolveToken(_ context.Context, _ credential.TokenSpec) (*credential.TokenResult, error) {
	return r.result, r.err
}

func factoryWithToken(result *credential.TokenResult, err error) *cmdutil.Factory {
	return &cmdutil.Factory{
		Credential: credential.NewCredentialProvider(nil, nil, &fakeTokenResolver{result: result, err: err}, nil),
	}
}

func strPtr(s string) *string { return &s }
func intPtr(i int) *int       { return &i }
func boolPtr(b bool) *bool    { return &b }

// ---- resolveUATAndCheckScopes ----

func TestResolveUATAndCheckScopes_UserAllScopesGranted_ReturnsToken(t *testing.T) {
	f := factoryWithToken(&credential.TokenResult{Token: "u-tok", Scopes: "event:subscription:read event:subscription:write"}, nil)

	uat, err := resolveUATAndCheckScopes(context.Background(), f, "cli_x", core.AsUser, subscriptionReadScopes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if uat != "u-tok" {
		t.Errorf("uat = %q, want %q", uat, "u-tok")
	}
}

// TestResolveUATAndCheckScopes_MissingReadScope_ReturnsTypedPermissionError
// locks that list/get require event:subscription:read; when the
// resolved identity's stored scopes are known and lack it, the command must
// fail closed with a typed *errs.PermissionError carrying MissingScopes —
// not a bare error, not a silent pass-through.
func TestResolveUATAndCheckScopes_MissingReadScope_ReturnsTypedPermissionError(t *testing.T) {
	f := factoryWithToken(&credential.TokenResult{Token: "u-tok", Scopes: "im:message:send"}, nil)

	_, err := resolveUATAndCheckScopes(context.Background(), f, "cli_x", core.AsUser, subscriptionReadScopes)
	if err == nil {
		t.Fatal("expected an error when the required scope is missing, got nil")
	}
	var permErr *errs.PermissionError
	if !errors.As(err, &permErr) {
		t.Fatalf("expected *errs.PermissionError, got %T: %v", err, err)
	}
	if permErr.Category != errs.CategoryAuthorization {
		t.Errorf("Category = %q, want %q", permErr.Category, errs.CategoryAuthorization)
	}
	if permErr.Subtype != errs.SubtypeMissingScope {
		t.Errorf("Subtype = %q, want %q", permErr.Subtype, errs.SubtypeMissingScope)
	}
	if permErr.Identity != string(core.AsUser) {
		t.Errorf("Identity = %q, want %q", permErr.Identity, string(core.AsUser))
	}
	if len(permErr.MissingScopes) != 1 || permErr.MissingScopes[0] != "event:subscription:read" {
		t.Errorf("MissingScopes = %v, want [event:subscription:read]", permErr.MissingScopes)
	}
	if permErr.Hint == "" {
		t.Error("expected a non-empty Hint with a recovery action")
	}
	if !strings.Contains(permErr.Hint, "auth login") {
		t.Errorf("user hint should point at `auth login`, got: %s", permErr.Hint)
	}
}

// TestResolveUATAndCheckScopes_BotMissingScope_HintPointsAtConsole mirrors
// the user case but for bot identity: the remediation hint must not tell a
// bot identity to `auth login` (that only re-authorizes a user token).
func TestResolveUATAndCheckScopes_BotMissingScope_HintPointsAtConsole(t *testing.T) {
	f := factoryWithToken(&credential.TokenResult{Token: "t-tok", Scopes: "im:message"}, nil)

	_, err := resolveUATAndCheckScopes(context.Background(), f, "cli_x", core.AsBot, subscriptionReadScopes)
	if err == nil {
		t.Fatal("expected an error when the required scope is missing, got nil")
	}
	var permErr *errs.PermissionError
	if !errors.As(err, &permErr) {
		t.Fatalf("expected *errs.PermissionError, got %T: %v", err, err)
	}
	if permErr.Identity != string(core.AsBot) {
		t.Errorf("Identity = %q, want %q", permErr.Identity, string(core.AsBot))
	}
	if strings.Contains(permErr.Hint, "auth login") {
		t.Errorf("bot hint must not say `auth login`, got: %s", permErr.Hint)
	}
	if !strings.Contains(permErr.Hint, "event:subscription:read") {
		t.Errorf("hint should name the missing scope, got: %s", permErr.Hint)
	}
}

// TestResolveUATAndCheckScopes_ScopesUnknown_SkipsCheck locks the
// best-effort idiom shared with shortcuts/common/runner.go's
// checkScopePrereqs and cmd/event/consume.go's preflightScopes: this CLI's
// default credential provider never populates a bot/tenant token's Scopes
// (internal/credential/default_provider.go doResolveTAT), so an empty
// Scopes string must never be treated as "definitely missing" — it must
// skip the local check and let the real API call be the authority.
func TestResolveUATAndCheckScopes_ScopesUnknown_SkipsCheck(t *testing.T) {
	f := factoryWithToken(&credential.TokenResult{Token: "t-tok", Scopes: ""}, nil)

	uat, err := resolveUATAndCheckScopes(context.Background(), f, "cli_x", core.AsBot, subscriptionReadScopes)
	if err != nil {
		t.Fatalf("unknown scopes must skip the local check, got error: %v", err)
	}
	if uat != "t-tok" {
		t.Errorf("uat = %q, want %q", uat, "t-tok")
	}
}

// TestResolveUATAndCheckScopes_TokenResolutionFails_BestEffortSkips: a
// non-context token-resolution failure (e.g. transient cache miss) must not
// itself become the reported error — eventlib.NewSubscriptionClient already
// fails closed with a typed AuthenticationError when uat is required and
// empty, and for bot identity the real API call surfaces any genuine auth
// problem.
func TestResolveUATAndCheckScopes_TokenResolutionFails_BestEffortSkips(t *testing.T) {
	f := factoryWithToken(nil, errors.New("token cache unavailable"))

	uat, err := resolveUATAndCheckScopes(context.Background(), f, "cli_x", core.AsUser, subscriptionReadScopes)
	if err != nil {
		t.Fatalf("expected best-effort skip (nil error), got: %v", err)
	}
	if uat != "" {
		t.Errorf("uat = %q, want empty on resolution failure", uat)
	}
}

func TestResolveUATAndCheckScopes_ContextCanceled_Propagates(t *testing.T) {
	f := factoryWithToken(nil, context.Canceled)

	_, err := resolveUATAndCheckScopes(context.Background(), f, "cli_x", core.AsUser, subscriptionReadScopes)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled to propagate", err)
	}
}

// ---- resolveEffectiveIdentity ----

func newCmdWithAsFlag(t *testing.T, asValue string) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{Use: "x"}
	addAsFlag(cmd)
	if err := cmd.Flags().Set("as", asValue); err != nil {
		t.Fatalf("Flags().Set(as, %q): %v", asValue, err)
	}
	cmd.SetContext(context.Background())
	return cmd
}

// TestResolveEffectiveIdentity_RejectsBogusAsValue locks that subscription
// commands validate --as against {user, bot} even though they have no
// EventKey/KeyTemplate AuthTypes to check against (they
// resolve --as to a single identity with no
// per-template whitelist) — an invalid literal must still fail closed
// rather than be passed through to the SDK client.
func TestResolveEffectiveIdentity_RejectsBogusAsValue(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := newCmdWithAsFlag(t, "bogus")

	_, err := resolveEffectiveIdentity(cmd, f)
	if err == nil {
		t.Fatal("expected an error for --as bogus, got nil")
	}
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeInvalidArgument {
		t.Errorf("Subtype = %s, want %s", ve.Subtype, errs.SubtypeInvalidArgument)
	}
	if ve.Param != "--as" {
		t.Errorf("Param = %q, want %q", ve.Param, "--as")
	}
}

func TestResolveEffectiveIdentity_AcceptsUserAndBot(t *testing.T) {
	f := &cmdutil.Factory{}
	for _, v := range []string{"user", "bot"} {
		cmd := newCmdWithAsFlag(t, v)
		as, err := resolveEffectiveIdentity(cmd, f)
		if err != nil {
			t.Errorf("--as %s: unexpected error: %v", v, err)
		}
		if string(as) != v {
			t.Errorf("--as %s: resolved identity = %q, want %q", v, as, v)
		}
	}
}

// TestResolveEffectiveIdentity_StrictModeRejectsCrossIdentity locks that
// resolveEffectiveIdentity must enforce the
// administrator's configured strict-mode identity policy (f.CheckStrictMode)
// right after f.ResolveAs — exactly like cmd/api/api.go's apiRun,
// cmd/service/service.go's serviceMethodRun, and cmd/whoami/whoami.go's
// whoamiRun (see TestWhoami_StrictModeRejectsCrossIdentity for the mirrored
// pattern this locks for subscription list/get). Without that call, an
// explicit --as of the disallowed identity would sail through
// resolveEffectiveIdentity's {"user","bot"} CheckIdentity list whenever a
// credential of that type was still resolvable (e.g. a cached user token
// under a bot-only strict-mode account), bypassing the policy.
func TestResolveEffectiveIdentity_StrictModeRejectsCrossIdentity(t *testing.T) {
	// Bot-only account -> strict mode bot (SupportedIdentities bit 2). A real
	// API call under this account would reject an explicit --as user via
	// f.CheckStrictMode; resolveEffectiveIdentity must reject it identically.
	f, _, _, _ := cmdutil.TestFactory(t, &core.CliConfig{
		ProfileName: "p", AppID: "test-app", AppSecret: "test-secret", Brand: core.BrandFeishu,
		SupportedIdentities: 2, // bot only
	})
	cmd := newCmdWithAsFlag(t, "user")

	_, err := resolveEffectiveIdentity(cmd, f)
	if err == nil {
		t.Fatal("expected an error for --as user under strict mode bot, got nil")
	}
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
}

// ---- mapSubscriptionDetail / formatAuthority ----

func TestMapSubscriptionDetail_NilDetail_ReturnsZeroValue(t *testing.T) {
	row := mapSubscriptionDetail(nil)
	if !reflect.DeepEqual(row, subscriptionRow{}) {
		t.Errorf("mapSubscriptionDetail(nil) = %+v, want zero value", row)
	}
}

func TestMapSubscriptionDetail_FullDetail_MapsEveryRemoteField(t *testing.T) {
	d := &larkeventv1.SubscriptionDetail{
		SubscriptionId: strPtr("sub_abc"),
		Authority: &larkeventv1.Authority{
			Type:   strPtr("user"),
			OpenId: strPtr("ou_xxx"),
		},
		TargetResource: strPtr("im.message?chat_id=oc_xxx"),
		EventType:      strPtr("im.message.created_v1"),
		PayloadOptions: &larkeventv1.PayloadOptions{IncludeResourceData: boolPtr(true)},
		State:          strPtr("suspended"),
		Suspension:     &larkeventv1.Suspension{Code: strPtr("authority_revoked")},
		ExpireTime:     intPtr(1732000000),
		CreateTime:     intPtr(1730000000),
		UpdateTime:     intPtr(1731000000),
	}

	row := mapSubscriptionDetail(d)

	if row.RemoteSubscriptionID != "sub_abc" {
		t.Errorf("RemoteSubscriptionID = %q, want sub_abc", row.RemoteSubscriptionID)
	}
	if row.EventType != "im.message.created_v1" {
		t.Errorf("EventType = %q, want im.message.created_v1", row.EventType)
	}
	if row.EventKey != row.EventType {
		t.Errorf("EventKey = %q, want it to equal EventType (%q) per the documented best-effort mapping", row.EventKey, row.EventType)
	}
	if row.TargetResource != "im.message?chat_id=oc_xxx" {
		t.Errorf("TargetResource = %q, want im.message?chat_id=oc_xxx", row.TargetResource)
	}
	if row.Identity != "user:ou_xxx" {
		t.Errorf("Identity = %q, want user:ou_xxx", row.Identity)
	}
	if row.PayloadOptions == nil || !row.PayloadOptions.IncludeResourceData {
		t.Errorf("PayloadOptions = %+v, want IncludeResourceData=true", row.PayloadOptions)
	}
	if row.Remote.State != "suspended" {
		t.Errorf("Remote.State = %q, want suspended", row.Remote.State)
	}
	if row.Remote.SuspensionReason != "authority_revoked" {
		t.Errorf("Remote.SuspensionReason = %q, want authority_revoked", row.Remote.SuspensionReason)
	}
	if row.Remote.ExpireTime == nil || *row.Remote.ExpireTime != 1732000000 {
		t.Errorf("Remote.ExpireTime = %v, want 1732000000", row.Remote.ExpireTime)
	}
	if row.Remote.CreateTime == nil || *row.Remote.CreateTime != 1730000000 {
		t.Errorf("Remote.CreateTime = %v, want 1730000000", row.Remote.CreateTime)
	}
	if row.Remote.UpdateTime == nil || *row.Remote.UpdateTime != 1731000000 {
		t.Errorf("Remote.UpdateTime = %v, want 1731000000", row.Remote.UpdateTime)
	}
	if row.Local != nil {
		t.Errorf("Local = %v, want nil/omitted — local-consumer association is a future concern", row.Local)
	}
}

func TestMapSubscriptionDetail_MinimalDetail_OmitsOptionalFields(t *testing.T) {
	d := &larkeventv1.SubscriptionDetail{
		SubscriptionId: strPtr("sub_min"),
		EventType:      strPtr("im.message.receive_v1"),
	}
	row := mapSubscriptionDetail(d)
	if row.PayloadOptions != nil {
		t.Errorf("PayloadOptions = %+v, want nil when the SDK omitted it", row.PayloadOptions)
	}
	if row.Remote.SuspensionReason != "" {
		t.Errorf("SuspensionReason = %q, want empty when the SDK omitted Suspension", row.Remote.SuspensionReason)
	}
	if row.Identity != "" {
		t.Errorf("Identity = %q, want empty when the SDK omitted Authority", row.Identity)
	}
	if row.Filter != nil {
		t.Errorf("Filter = %s, want nil/omitted when the SDK omitted it", row.Filter)
	}
}

// TestMapSubscriptionDetail_WithRemoteFilter_SurfacesCanonicalJSON proves a
// remote filter is surfaced as canonical JSON (the exact wire form), shared by
// list and get.
func TestMapSubscriptionDetail_WithRemoteFilter_SurfacesCanonicalJSON(t *testing.T) {
	f, err := eventlib.ParseAndValidateFilter(
		`{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"message_type","op":"in","list_value":["text"]}}]}}`,
		eventlib.FilterMetaFor("im.message.created_v1"))
	if err != nil {
		t.Fatalf("build filter: %v", err)
	}
	d := &larkeventv1.SubscriptionDetail{
		SubscriptionId: strPtr("sub_f"),
		EventType:      strPtr("im.message.created_v1"),
		Filter:         eventlib.FilterToSDK(f),
	}
	row := mapSubscriptionDetail(d)
	if len(row.Filter) == 0 {
		t.Fatal("row.Filter is empty, want the remote filter as canonical JSON")
	}
	want, err := f.Canonicalize()
	if err != nil {
		t.Fatalf("canonicalize: %v", err)
	}
	if !bytes.Equal(row.Filter, want) {
		t.Errorf("row.Filter = %s, want %s", row.Filter, want)
	}
}

func TestFormatAuthority(t *testing.T) {
	tests := []struct {
		name string
		a    *larkeventv1.Authority
		want string
	}{
		{"nil", nil, ""},
		{"nil type", &larkeventv1.Authority{}, ""},
		{"user with open_id", &larkeventv1.Authority{Type: strPtr("user"), OpenId: strPtr("ou_xxx")}, "user:ou_xxx"},
		{"user without open_id", &larkeventv1.Authority{Type: strPtr("user")}, "user"},
		{"app", &larkeventv1.Authority{Type: strPtr("app"), AppId: strPtr("cli_xxx")}, "app"},
		{"unrecognized type passes through", &larkeventv1.Authority{Type: strPtr("service")}, "service"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatAuthority(tc.a); got != tc.want {
				t.Errorf("formatAuthority(%+v) = %q, want %q", tc.a, got, tc.want)
			}
		})
	}
}

// ---- NewCmdSubscription wiring ----

func TestNewCmdSubscription_RegistersListAndGetAsRead(t *testing.T) {
	f := &cmdutil.Factory{}
	cmd := NewCmdSubscription(f)

	found := map[string]*cobra.Command{}
	for _, c := range cmd.Commands() {
		found[c.Name()] = c
	}
	for _, name := range []string{"list", "get"} {
		sub, ok := found[name]
		if !ok {
			t.Fatalf("subscription command group missing %q subcommand", name)
			continue
		}
		level, ok := cmdutil.GetRisk(sub)
		if !ok || level != cmdutil.RiskRead {
			t.Errorf("%q risk = (%q, %v), want (%q, true)", name, level, ok, cmdutil.RiskRead)
		}
	}
}
