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
	"github.com/larksuite/cli/internal/event/model"
	larkgw "github.com/larksuite/cli/internal/event/platform/lark"
)

// activeSub / suspendedSub are the domain (gateway-projected) counterparts of
// activeDetail / suspendedDetail (create_test.go), for the migrated
// read/simple-write command fakes that now speak model.RemoteSubscription
// instead of the SDK type. Projecting the SDK fixture keeps the domain fixture
// byte-identical to what the gateway would hand a command in production.
func activeSub(id string, includeResourceData bool, authorityType string) model.RemoteSubscription {
	return larkgw.ProjectSubscription(activeDetail(id, includeResourceData, authorityType))
}

func suspendedSub(id, reason string) model.RemoteSubscription {
	return larkgw.ProjectSubscription(suspendedDetail(id, reason))
}

// subPtr returns a pointer to s, for building the *model.RemoteSubscription
// values the domain command fakes hand back from Get/Renew/Reactivate/Patch.
func subPtr(s model.RemoteSubscription) *model.RemoteSubscription { return &s }

// The subscription tests build minimal catalog fixtures instead of importing the
// full events catalog, so they seed im.message.created_v1's filter capability
// directly through the registry API — the same capability the business layer
// registers in a real run. Only this event_type is seeded, so keys without a
// filter capability (e.g. im.message.receive_v1) stay fail-closed.
func init() {
	eventlib.RegisterFilterMeta("im.message.created_v1", eventlib.FilterMeta{
		Supported:     true,
		LogicOps:      []string{"and", "or"},
		Operators:     []string{"eq", "in", "contains"},
		MaxDepth:      2,
		MaxConditions: 10,
		MaxBytes:      1024,
		Operands: []eventlib.FilterOperandMeta{
			{Key: "sender", Operators: []string{"eq"}, InputValueType: "open_id"},
			{Key: "message_type", Operators: []string{"eq", "in"}, ListValueMax: 10},
		},
	})
}

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

	uat, verified, err := resolveUATAndCheckScopes(context.Background(), f, "cli_x", core.AsUser, subscriptionReadScopes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if uat != "u-tok" {
		t.Errorf("uat = %q, want %q", uat, "u-tok")
	}
	// Scopes were present AND satisfied -> a genuine verification.
	if !verified {
		t.Error("scopesVerified = false, want true when the granted scopes were checked")
	}
}

// TestResolveUATAndCheckScopes_MissingReadScope_ReturnsTypedPermissionError
// locks that list/get require event:subscription:read; when the
// resolved identity's stored scopes are known and lack it, the command must
// fail closed with a typed *errs.PermissionError carrying MissingScopes —
// not a bare error, not a silent pass-through.
func TestResolveUATAndCheckScopes_MissingReadScope_ReturnsTypedPermissionError(t *testing.T) {
	f := factoryWithToken(&credential.TokenResult{Token: "u-tok", Scopes: "im:message:send"}, nil)

	_, _, err := resolveUATAndCheckScopes(context.Background(), f, "cli_x", core.AsUser, subscriptionReadScopes)
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

	_, _, err := resolveUATAndCheckScopes(context.Background(), f, "cli_x", core.AsBot, subscriptionReadScopes)
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

	uat, verified, err := resolveUATAndCheckScopes(context.Background(), f, "cli_x", core.AsBot, subscriptionReadScopes)
	if err != nil {
		t.Fatalf("unknown scopes must skip the local check, got error: %v", err)
	}
	if uat != "t-tok" {
		t.Errorf("uat = %q, want %q", uat, "t-tok")
	}
	// Scope data was unavailable -> satisfaction was NOT verified.
	if verified {
		t.Error("scopesVerified = true, want false when scope data was unavailable")
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

	uat, verified, err := resolveUATAndCheckScopes(context.Background(), f, "cli_x", core.AsUser, subscriptionReadScopes)
	if err != nil {
		t.Fatalf("expected best-effort skip (nil error), got: %v", err)
	}
	if uat != "" {
		t.Errorf("uat = %q, want empty on resolution failure", uat)
	}
	// Token resolution failed -> nothing was verified.
	if verified {
		t.Error("scopesVerified = true, want false when token resolution failed")
	}
}

func TestResolveUATAndCheckScopes_ContextCanceled_Propagates(t *testing.T) {
	f := factoryWithToken(nil, context.Canceled)

	_, _, err := resolveUATAndCheckScopes(context.Background(), f, "cli_x", core.AsUser, subscriptionReadScopes)
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

// ---- mapRemoteSubscription (domain -> row) ----
//
// The SDK -> RemoteSubscription projection is covered by the gateway's
// TestProjectSubscription_* and the authority vocabulary by model's
// TestRemoteAuthority_String; these route an SDK fixture through
// ProjectSubscription + mapRemoteSubscription to lock the end-to-end row shape
// the migrated commands emit.

func TestMapRemoteSubscription_ZeroValue_ReturnsZeroRow(t *testing.T) {
	row := mapRemoteSubscription(larkgw.ProjectSubscription(nil))
	if !reflect.DeepEqual(row, subscriptionRow{}) {
		t.Errorf("mapRemoteSubscription(zero) = %+v, want zero value", row)
	}
}

func TestMapRemoteSubscription_FullDetail_MapsEveryRemoteField(t *testing.T) {
	registerCreateFixtures(t) // so ReverseResolve can reconstruct the executable event_key

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

	row := mapRemoteSubscription(larkgw.ProjectSubscription(d))

	if row.RemoteSubscriptionID != "sub_abc" {
		t.Errorf("RemoteSubscriptionID = %q, want sub_abc", row.RemoteSubscriptionID)
	}
	if row.EventType != "im.message.created_v1" {
		t.Errorf("EventType = %q, want im.message.created_v1", row.EventType)
	}
	// event_key is the reversed, EXECUTABLE materialized key — not the raw
	// event_type — so an AI can run it directly against consume/schema.
	if row.EventKey != "im.message.created_v1/chat-id/oc_xxx" {
		t.Errorf("EventKey = %q, want the reversed materialized key im.message.created_v1/chat-id/oc_xxx", row.EventKey)
	}
	if row.TargetResource != "im.message?chat_id=oc_xxx" {
		t.Errorf("TargetResource = %q, want im.message?chat_id=oc_xxx", row.TargetResource)
	}
	// identity is the `--as`-composable token; the open_id is preserved separately.
	if row.Identity != "user" {
		t.Errorf("Identity = %q, want user", row.Identity)
	}
	if row.UserOpenID != "ou_xxx" {
		t.Errorf("UserOpenID = %q, want ou_xxx", row.UserOpenID)
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

// TestMapRemoteSubscription_AppAuthority_IdentityIsBotNoOpenID locks the other
// authority tier: a remote "app" authority surfaces as the `--as bot` token with
// no user_open_id, so the row's identity round-trips through `--as` for a bot too.
func TestMapRemoteSubscription_AppAuthority_IdentityIsBotNoOpenID(t *testing.T) {
	registerCreateFixtures(t)
	row := mapRemoteSubscription(larkgw.ProjectSubscription(&larkeventv1.SubscriptionDetail{
		SubscriptionId: strPtr("sub_bot"),
		Authority:      &larkeventv1.Authority{Type: strPtr("app"), AppId: strPtr("cli_xxx")},
		TargetResource: strPtr("im.message?chat_id=oc_xxx"),
		EventType:      strPtr("im.message.created_v1"),
		State:          strPtr("active"),
	}))
	if row.Identity != "bot" {
		t.Errorf("Identity = %q, want bot", row.Identity)
	}
	if row.UserOpenID != "" {
		t.Errorf("UserOpenID = %q, want empty for an app authority", row.UserOpenID)
	}
}

// TestMapRemoteSubscription_LegacyEventKey_ReversesToPlainKey locks that a
// legacy (empty target_resource) subscription reverses to its plain, executable
// EventKey rather than being reported unavailable.
func TestMapRemoteSubscription_LegacyEventKey_ReversesToPlainKey(t *testing.T) {
	registerCreateFixtures(t)
	row := mapRemoteSubscription(larkgw.ProjectSubscription(&larkeventv1.SubscriptionDetail{
		SubscriptionId: strPtr("sub_legacy"),
		EventType:      strPtr("im.message.receive_v1"),
	}))
	if row.EventKey != "im.message.receive_v1" {
		t.Errorf("EventKey = %q, want the reversed legacy key im.message.receive_v1", row.EventKey)
	}
}

// TestMapRemoteSubscription_Unreversible_EmitsUnavailableMarker locks the
// explicit-unavailable contract: when event_type + target_resource cannot be
// reversed to a registered EventKey, event_key is the EventKeyUnavailable marker
// (never the raw event_type), while event_type itself stays available.
func TestMapRemoteSubscription_Unreversible_EmitsUnavailableMarker(t *testing.T) {
	registerCreateFixtures(t)
	row := mapRemoteSubscription(larkgw.ProjectSubscription(&larkeventv1.SubscriptionDetail{
		SubscriptionId: strPtr("sub_unknown"),
		EventType:      strPtr("does.not.exist_v1"),
		TargetResource: strPtr("im.message?chat_id=oc_zzz"),
	}))
	if row.EventKey != EventKeyUnavailable {
		t.Errorf("EventKey = %q, want the explicit %q marker for an unreversible pair", row.EventKey, EventKeyUnavailable)
	}
	if row.EventKey == row.EventType {
		t.Errorf("event_key must never be the raw event_type (%q) when unreversible", row.EventType)
	}
	if row.EventType != "does.not.exist_v1" {
		t.Errorf("EventType = %q, want it preserved verbatim alongside the unavailable event_key", row.EventType)
	}
}

func TestMapRemoteSubscription_MinimalDetail_OmitsOptionalFields(t *testing.T) {
	d := &larkeventv1.SubscriptionDetail{
		SubscriptionId: strPtr("sub_min"),
		EventType:      strPtr("im.message.receive_v1"),
	}
	row := mapRemoteSubscription(larkgw.ProjectSubscription(d))
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

// TestMapRemoteSubscription_WithRemoteFilter_SurfacesCanonicalJSON proves a
// remote filter is surfaced as canonical JSON (the exact wire form), shared by
// list and get.
func TestMapRemoteSubscription_WithRemoteFilter_SurfacesCanonicalJSON(t *testing.T) {
	f, err := eventlib.ParseAndValidateFilter(
		`{"composite_condition":{"logic_op":"and","composite_conditions":[{"condition":{"operand":"message_type","op":"in","list_value":["text"]}}]}}`,
		eventlib.FilterMetaFor("im.message.created_v1"))
	if err != nil {
		t.Fatalf("build filter: %v", err)
	}
	d := &larkeventv1.SubscriptionDetail{
		SubscriptionId: strPtr("sub_f"),
		EventType:      strPtr("im.message.created_v1"),
		Filter:         larkgw.FilterToSDK(f),
	}
	row := mapRemoteSubscription(larkgw.ProjectSubscription(d))
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

// ---- #22.3 dry-run scopes_ok bool + additive scope_status ----

// TestBuildMutationDryRunResult_ScopesOK locks that the mutation dry-run
// preflight reports scope satisfaction honestly AND backward-compatibly:
// scopes_ok stays a bool (true ONLY when the pre-check actually confirmed the
// scopes, false when it could not — never a green light we never checked), and
// the additive scope_status carries the "verified" vs "unknown" nuance.
func TestBuildMutationDryRunResult_ScopesOK(t *testing.T) {
	for _, tc := range []struct {
		verified   bool
		wantStatus string
	}{
		{true, "verified"},
		{false, "unknown"},
	} {
		result := buildMutationDryRunResult("renew", "sub_1", core.AsUser, tc.verified, nil,
			"renew", false, "", "next")
		if result.Preflight.ScopesOK != tc.verified {
			t.Errorf("scopesVerified=%v -> ScopesOK=%v, want %v", tc.verified, result.Preflight.ScopesOK, tc.verified)
		}
		if result.Preflight.ScopeStatus != tc.wantStatus {
			t.Errorf("scopesVerified=%v -> ScopeStatus=%q, want %q", tc.verified, result.Preflight.ScopeStatus, tc.wantStatus)
		}
	}
}

// ---- #22.4 renew/reactivate dry-run plans from real remote state ----

// TestMutationDryRunPlan_StateAppropriate locks that the renew/reactivate
// dry-run plan is derived from the OBSERVED remote state, not a static
// assumption: an already-active reactivate is a no-op, a suspended one
// reactivates, and a renew is valid ONLY on an active subscription — while any
// state OUTSIDE each operation's valid set (reactivate: not suspended; renew:
// not active) is "blocked" (the real run fails closed), so the preview and the
// real run agree.
func TestMutationDryRunPlan_StateAppropriate(t *testing.T) {
	cases := []struct {
		name         string
		operation    string
		state        string
		wantAction   string
		nextContains string
	}{
		{"reactivate active -> noop", "reactivate", "active", "noop", "already active"},
		{"reactivate suspended -> reactivate", "reactivate", "suspended", "reactivate", "run without --dry-run to reactivate"},
		{"reactivate expired -> blocked", "reactivate", "expired", "blocked", "only a suspended subscription can be reactivated"},
		{"reactivate empty -> blocked", "reactivate", "", "blocked", "cannot reactivate"},
		{"renew active -> renew", "renew", "active", "renew", "run without --dry-run to renew"},
		{"renew suspended -> blocked", "renew", "suspended", "blocked", "only an active subscription can be renewed"},
		{"renew expired -> blocked", "renew", "expired", "blocked", "only an active subscription can be renewed"},
		{"renew empty -> blocked", "renew", "", "blocked", "cannot renew"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			action, next := mutationDryRunPlan(c.operation, "sub_1", c.state)
			if action != c.wantAction {
				t.Errorf("plannedAction = %q, want %q", action, c.wantAction)
			}
			if !strings.Contains(next, c.nextContains) {
				t.Errorf("nextAction = %q, want it to contain %q", next, c.nextContains)
			}
		})
	}
}
