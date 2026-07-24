// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package consume

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/protocol"
	"github.com/larksuite/cli/internal/event/testutil"
)

// ---- fixtures ----

// refinedFixture builds a self-contained event.ResolvedEventKey (as
// cmd/event/consume.go's fork seam would hand RunRefined after
// eventlib.ResolveEventKey succeeds) without touching the global KeyDefinition
// registry — every test in this file constructs its own Definition, so there
// is no RegisterKey/UnregisterKeyForTest cleanup dance to get wrong.
func refinedFixture() event.ResolvedEventKey {
	def := &event.KeyDefinition{
		Key:                 "im.message.created_v1",
		EventType:           "im.message.created_v1",
		RefinedSubscription: true,
		ResourceType:        "im.message",
		Process: func(_ context.Context, _ event.APIClient, raw *event.RawEvent, _ map[string]string) (json.RawMessage, error) {
			return raw.Payload, nil
		},
	}
	return event.ResolvedEventKey{
		Definition:      def,
		IsRefined:       true,
		MaterializedKey: "im.message.created_v1/chat-id/oc_aaa",
		SelectorKey:     "chat_id",
		SelectorValue:   "oc_aaa",
		TargetResource:  "im.message?chat_id=oc_aaa",
	}
}

func strPtr(s string) *string { return &s }
func boolPtr(b bool) *bool     { return &b }

// activeEncryptedListResp is a List response with a single active,
// include_resource_data=true match under the given authority type — the remote
// shape a consume reconcile classifies as an encrypted reuse.
func activeEncryptedListResp(id, authorityType string) *larkeventv1.ListSubscriptionResp {
	return &larkeventv1.ListSubscriptionResp{
		Data: &larkeventv1.ListSubscriptionRespData{
			Items: []*larkeventv1.SubscriptionDetail{{
				SubscriptionId: strPtr(id),
				EventType:      strPtr("im.message.created_v1"),
				TargetResource: strPtr("im.message?chat_id=oc_aaa"),
				Authority:      &larkeventv1.Authority{Type: strPtr(authorityType), OpenId: strPtr("ou_aaa")},
				State:          strPtr("active"),
				PayloadOptions: &larkeventv1.PayloadOptions{IncludeResourceData: boolPtr(true)},
			}},
		},
	}
}

// ---- fakeStatusBusTransport: a minimal local "bus" that only answers status_query ----

// fakeStatusBusTransport plays a local bus that answers exactly one
// status_query with a canned StatusResponse, over a net.Pipe (no real
// socket/listener needed) — enough for busctl.QueryStatus's single
// dial-encode-read-decode round trip.
type fakeStatusBusTransport struct {
	resp *protocol.StatusResponse
}

func (f fakeStatusBusTransport) Listen(string) (net.Listener, error) {
	return nil, errors.New("fakeStatusBusTransport: Listen not supported")
}

func (f fakeStatusBusTransport) Dial(string) (net.Conn, error) {
	client, server := net.Pipe()
	go func() {
		defer server.Close()
		br := bufio.NewReader(server)
		line, err := protocol.ReadFrame(br)
		if err != nil {
			return
		}
		if _, err := protocol.Decode(bytes.TrimRight(line, "\n")); err != nil {
			return
		}
		_ = protocol.Encode(server, f.resp)
	}()
	return client, nil
}

func (f fakeStatusBusTransport) Address(string) string { return "fake-status-bus-addr" }
func (f fakeStatusBusTransport) Cleanup(string)        {}

// ---- ProbeBusEligibility: stage (a) remote-connection guard ----

func TestProbeBusEligibility_RemoteConnectionBusy_FailedPrecondition_NoWrite(t *testing.T) {
	apiClient := &testutil.StubAPIClient{Body: `{"code":0,"msg":"ok","data":{"online_instance_cnt":2}}`}

	err := ProbeBusEligibility(context.Background(), failDialTransport{}, "cli_probe_test", apiClient, io.Discard)
	if err == nil {
		t.Fatal("expected failed_precondition when a remote connection is already online, got nil")
	}
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	if !strings.Contains(ve.Hint, "probe_bus_eligibility") {
		t.Errorf("Hint should name the probe stage, got: %q", ve.Hint)
	}
	if !strings.Contains(ve.Hint, "online_instance_cnt=2") {
		t.Errorf("Hint should carry the observed count, got: %q", ve.Hint)
	}
	// Probe is read-only: exactly the connection-check GET, nothing else.
	if apiClient.Calls != 1 {
		t.Errorf("expected exactly 1 API call (read-only connection check), got %d", apiClient.Calls)
	}
	if apiClient.GotMethod != "GET" || apiClient.GotPath != "/open-apis/event/v1/connection" {
		t.Errorf("unexpected call: %s %s (probe must never write)", apiClient.GotMethod, apiClient.GotPath)
	}
}

func TestProbeBusEligibility_RemoteConnectionCheckErrors_FailsOpenMirroringEnsureBus(t *testing.T) {
	// EnsureBus's own existing CheckRemoteConnections handling treats a
	// transport/decode failure as inconclusive (log + proceed), not fatal —
	// Probe reuses that exact call, so it must mirror that fail-open
	// behavior for THIS specific failure mode (as opposed to a confirmed
	// online_instance_cnt>0, which fails closed above).
	apiClient := &testutil.StubAPIClient{Body: `not json at all`}
	err := ProbeBusEligibility(context.Background(), failDialTransport{}, "cli_probe_test", apiClient, io.Discard)
	if err != nil {
		t.Errorf("expected the probe to fail OPEN on an inconclusive remote check, got error: %v", err)
	}
}

// ---- ProbeBusEligibility: stage (b) local-bus capability guard ----

func TestProbeBusEligibility_NoLocalBus_NoError(t *testing.T) {
	// No bus reachable at all -- fine, a fresh one gets forked later with
	// full v2 capabilities (StartOrConnectBus, a later stage).
	if err := ProbeBusEligibility(context.Background(), failDialTransport{}, "cli_probe_test", nil, io.Discard); err != nil {
		t.Errorf("expected no error when no local bus is running yet, got: %v", err)
	}
}

func TestProbeBusEligibility_OldLocalBus_MissingCapabilities_FailedPrecondition_PromptsEventStop(t *testing.T) {
	// An old (pre-refined) bus never sets ProtocolVersion/Capabilities at
	// all -- their ABSENCE is the incompatibility signal (see
	// protocol.StatusResponse's own doc comment), not a version mismatch.
	oldBusResp := protocol.NewStatusResponse(4242, 10, 1, nil)
	tr := fakeStatusBusTransport{resp: oldBusResp}

	err := ProbeBusEligibility(context.Background(), tr, "cli_probe_test", nil, io.Discard)
	if err == nil {
		t.Fatal("expected failed_precondition for an old (pre-v2) local bus, got nil")
	}
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	if !strings.Contains(ve.Hint, "event stop") {
		t.Errorf("Hint should fail closed and prompt `event stop`, got: %q", ve.Hint)
	}
}

func TestProbeBusEligibility_HealthyLocalBus_NoError(t *testing.T) {
	healthyResp := protocol.NewStatusResponse(4242, 10, 1, nil)
	healthyResp.ProtocolVersion = protocol.ProtocolVersionV2
	healthyResp.Capabilities = []string{protocol.CapabilityRefinedRouting, protocol.CapabilityHelloV2}
	tr := fakeStatusBusTransport{resp: healthyResp}

	if err := ProbeBusEligibility(context.Background(), tr, "cli_probe_test", nil, io.Discard); err != nil {
		t.Errorf("expected no error for a bus advertising full v2 capabilities, got: %v", err)
	}
}

func TestProbeBusEligibility_HealthyLocalBusAlreadyOnline_RemoteCheckNotAppliedToItsOwnConnection(t *testing.T) {
	// The realistic steady state once ANY bus for this app is already up:
	// that bus's own WebSocket connection makes the remote API's
	// online_instance_cnt >= 1. A SECOND (or third...) refined consumer
	// attaching to that SAME already-healthy local bus must not be
	// rejected just because the remote API reports the bus's own
	// connection back to it. EnsureBus's own existing logic (startup.go)
	// already establishes this exact condition — it calls
	// CheckRemoteConnections ONLY inside the "local bus not found" branch
	// (see its "local bus not found; checking remote connections..." log
	// line), never when probeAndDialBus already succeeded — so Probe must
	// mirror that same condition, not just the same underlying call.
	healthyResp := protocol.NewStatusResponse(4242, 10, 1, nil)
	healthyResp.ProtocolVersion = protocol.ProtocolVersionV2
	healthyResp.Capabilities = []string{protocol.CapabilityRefinedRouting, protocol.CapabilityHelloV2}
	tr := fakeStatusBusTransport{resp: healthyResp}
	apiClient := &testutil.StubAPIClient{Body: `{"code":0,"data":{"online_instance_cnt":1}}`}

	err := ProbeBusEligibility(context.Background(), tr, "cli_probe_test", apiClient, io.Discard)
	if err != nil {
		t.Errorf("a healthy already-running local bus must not be rejected merely because the remote API reports ITS OWN connection back; got: %v", err)
	}
	// The remote check is inapplicable once a local bus already answered —
	// asserting zero calls proves Probe did not even attempt it (not just
	// that it tolerated the result), consistent with EnsureBus's own
	// "checking remote connections" log firing only in the no-local-bus branch.
	if apiClient.Calls != 0 {
		t.Errorf("expected the remote connection check to be skipped once a local bus already answered, got %d call(s)", apiClient.Calls)
	}
}

func TestProbeBusEligibility_LocalBusMissingOneCapabilityMarker_FailedPrecondition(t *testing.T) {
	// ProtocolVersion present but the capability list is incomplete (e.g. a
	// mid-rollout partial build) must still fail closed -- absence of a
	// value, not just of the whole field, is what disqualifies it.
	partialResp := protocol.NewStatusResponse(4242, 10, 1, nil)
	partialResp.ProtocolVersion = protocol.ProtocolVersionV2
	partialResp.Capabilities = []string{protocol.CapabilityRefinedRouting} // missing hello_v2
	tr := fakeStatusBusTransport{resp: partialResp}

	err := ProbeBusEligibility(context.Background(), tr, "cli_probe_test", nil, io.Discard)
	if err == nil {
		t.Fatal("expected failed_precondition when a required capability marker is missing")
	}
}

// ---- ProbeBusEligibility: Dial-succeeds-but-status-fails is NOT "no local bus" ----

// fakeDialOKStatusFailsTransport plays a local bus that accepts the Dial and
// reads the status_query, then closes without ever answering it -- Dial
// itself succeeds (a local bus IS listening) but the status exchange fails
// afterward (busctl.ErrStatusUnverified), unlike failDialTransport (used
// elsewhere in this file) where Dial itself is refused.
type fakeDialOKStatusFailsTransport struct{}

func (fakeDialOKStatusFailsTransport) Listen(string) (net.Listener, error) {
	return nil, errors.New("fakeDialOKStatusFailsTransport: Listen not supported")
}

func (fakeDialOKStatusFailsTransport) Dial(string) (net.Conn, error) {
	client, server := net.Pipe()
	go func() {
		defer server.Close()
		br := bufio.NewReader(server)
		_, _ = protocol.ReadFrame(br) // read (and discard) the status_query
		// Deliberately send nothing back, then close: the client's own
		// ReadFrame sees EOF -- Dial succeeded, the status exchange did not.
	}()
	return client, nil
}

func (fakeDialOKStatusFailsTransport) Address(string) string { return "fake-dial-ok-status-fail-addr" }
func (fakeDialOKStatusFailsTransport) Cleanup(string)        {}

func TestProbeBusEligibility_LocalBusDialOKStatusUnverified_FailsClosed_NeverChecksRemote(t *testing.T) {
	// Before the fix, EVERY busctl.QueryStatus error (Dial failing outright,
	// or Dial succeeding but the subsequent status exchange erroring) was
	// treated identically as "no local bus" -- so this exact scenario would
	// have fallen through to the remote-connection check below and, seeing
	// online_instance_cnt=0, returned nil (Probe passing, Plan/Apply free to
	// proceed) even though a local bus IS actually running and its
	// v2-capability could never be confirmed. This locks the fix: it must
	// fail closed instead, and must never even attempt the remote check.
	apiClient := &testutil.StubAPIClient{Body: `{"code":0,"data":{"online_instance_cnt":0}}`}
	tr := fakeDialOKStatusFailsTransport{}

	err := ProbeBusEligibility(context.Background(), tr, "cli_probe_test", apiClient, io.Discard)
	if err == nil {
		t.Fatal("expected failed_precondition when a local bus is dialable but its status could not be verified, got nil")
	}
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	if !strings.Contains(ve.Hint, "event stop") {
		t.Errorf("Hint should fail closed and prompt `event stop`, got: %q", ve.Hint)
	}
	if apiClient.Calls != 0 {
		t.Errorf("expected the remote connection check to be skipped entirely (a local bus IS reachable), got %d call(s)", apiClient.Calls)
	}
}

// ---- ProbeBusEligibility: remote-connection check is bounded regardless of the caller's own ctx ----

// probeDeadlineCapturingAPIClient records whether the ctx it receives for
// CallAPI carries a deadline, and how far out -- used to prove
// ProbeBusEligibility bounds its own CheckRemoteConnections call
// independently of the caller's ctx, without this test actually having to
// wait out the real bound (which would make it slow).
type probeDeadlineCapturingAPIClient struct {
	hadDeadline bool
	remaining   time.Duration
}

func (c *probeDeadlineCapturingAPIClient) CallAPI(ctx context.Context, _, _ string, _ interface{}) (json.RawMessage, error) {
	if dl, ok := ctx.Deadline(); ok {
		c.hadDeadline = true
		c.remaining = time.Until(dl)
	}
	return json.RawMessage(`{"code":0,"data":{"online_instance_cnt":0}}`), nil
}

func TestProbeBusEligibility_RemoteConnectionCheck_BoundedRegardlessOfCallerCtx(t *testing.T) {
	// context.Background() carries no deadline at all -- exactly the shape
	// of `event consume`'s own ctx when --timeout is left at its 0
	// (unbounded) default; before the fix, that meant a slow/unreachable
	// remote API could hang this read-only probe indefinitely.
	apiClient := &probeDeadlineCapturingAPIClient{}
	err := ProbeBusEligibility(context.Background(), failDialTransport{}, "cli_probe_test", apiClient, io.Discard)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !apiClient.hadDeadline {
		t.Fatal("expected the remote-connection-check ctx to carry a deadline even though the caller's own ctx had none")
	}
	if apiClient.remaining <= 0 || apiClient.remaining > remoteConnectionCheckTimeout {
		t.Errorf("expected a bound of at most %s, got %s remaining", remoteConnectionCheckTimeout, apiClient.remaining)
	}
}

// ---- computeConsumerScopeID / buildHelloV2 (pure, no network) ----

func TestComputeConsumerScopeID_DeterministicAndSensitiveToEachInput(t *testing.T) {
	base := computeConsumerScopeID("im.message.created_v1", "im.message.created_v1/chat-id/oc_aaa", "user", "cli_app", "ou_123")
	same := computeConsumerScopeID("im.message.created_v1", "im.message.created_v1/chat-id/oc_aaa", "user", "cli_app", "ou_123")
	if base != same {
		t.Error("computeConsumerScopeID must be deterministic for identical inputs")
	}
	if base == "" {
		t.Error("computeConsumerScopeID must not be empty")
	}

	variants := map[string]string{
		"materialized_key": computeConsumerScopeID("im.message.created_v1", "im.message.created_v1/chat-id/oc_BBB", "user", "cli_app", "ou_123"),
		"authority_type":   computeConsumerScopeID("im.message.created_v1", "im.message.created_v1/chat-id/oc_aaa", "app", "cli_app", "ou_123"),
		"app_id":           computeConsumerScopeID("im.message.created_v1", "im.message.created_v1/chat-id/oc_aaa", "user", "cli_other", "ou_123"),
		"user_open_id":     computeConsumerScopeID("im.message.created_v1", "im.message.created_v1/chat-id/oc_aaa", "user", "cli_app", "ou_999"),
	}
	for field, v := range variants {
		if v == base {
			t.Errorf("changing %s produced the same scope id as base; inputs must be distinguishable", field)
		}
	}
}

func TestAuthorityTypeFor_MapsIdentityToRemoteAuthorityVocabulary(t *testing.T) {
	if got := authorityTypeFor(core.AsBot); got != "app" {
		t.Errorf("authorityTypeFor(bot) = %q, want %q", got, "app")
	}
	if got := authorityTypeFor(core.AsUser); got != "user" {
		t.Errorf("authorityTypeFor(user) = %q, want %q", got, "user")
	}
}

func TestBuildHelloV2_PopulatesV2Fields(t *testing.T) {
	resolved := refinedFixture()
	h := buildHelloV2(resolved, core.AsUser, "local-sub-id", "my-profile", "ou_user_1", "sub_remote_1", "scope-hash-1", resolved.TargetResource)

	if h.Type != protocol.MsgTypeHello {
		t.Errorf("Type = %q, want %q", h.Type, protocol.MsgTypeHello)
	}
	if h.Version != "v1" {
		t.Errorf(`Version = %q, want frozen "v1" (v2-ness is signaled via Capabilities, never a Version bump)`, h.Version)
	}
	if h.EventKey != resolved.MaterializedKey {
		t.Errorf("EventKey = %q, want the filled template instance %q", h.EventKey, resolved.MaterializedKey)
	}
	if len(h.EventTypes) != 1 || h.EventTypes[0] != resolved.Definition.EventType {
		t.Errorf("EventTypes = %v, want [%q]", h.EventTypes, resolved.Definition.EventType)
	}
	if h.SubscriptionID != "local-sub-id" {
		t.Errorf("SubscriptionID = %q, want the LOCAL fingerprint (frozen meaning, never repurposed)", h.SubscriptionID)
	}
	if h.Identity != "user" {
		t.Errorf("Identity = %q, want %q", h.Identity, "user")
	}
	if h.Profile != "my-profile" {
		t.Errorf("Profile = %q, want %q", h.Profile, "my-profile")
	}
	if h.UserOpenID != "ou_user_1" {
		t.Errorf("UserOpenID = %q, want %q", h.UserOpenID, "ou_user_1")
	}
	if h.RemoteSubscriptionID != "sub_remote_1" {
		t.Errorf("RemoteSubscriptionID = %q, want %q (distinct id space from SubscriptionID)", h.RemoteSubscriptionID, "sub_remote_1")
	}
	if h.ConsumerScopeID != "scope-hash-1" {
		t.Errorf("ConsumerScopeID = %q, want %q", h.ConsumerScopeID, "scope-hash-1")
	}
	if h.TargetResource != resolved.TargetResource {
		t.Errorf("TargetResource = %q, want %q (issue #7: the bus stores this as the consumer's own listening intent)", h.TargetResource, resolved.TargetResource)
	}
	var foundHelloV2 bool
	for _, c := range h.Capabilities {
		if c == protocol.CapabilityHelloV2 {
			foundHelloV2 = true
		}
	}
	if !foundHelloV2 {
		t.Errorf("Capabilities = %v, want it to include %q", h.Capabilities, protocol.CapabilityHelloV2)
	}
}

func TestBuildHelloV2_BotIdentity_NeverCarriesUserOpenID(t *testing.T) {
	resolved := refinedFixture()
	h := buildHelloV2(resolved, core.AsBot, "local-sub-id", "my-profile", "ou_should_be_dropped", "sub_remote_1", "scope-hash-1", resolved.TargetResource)
	if h.Identity != "bot" {
		t.Errorf("Identity = %q, want %q", h.Identity, "bot")
	}
	if h.UserOpenID != "" {
		t.Errorf("UserOpenID = %q, want empty for a bot identity", h.UserOpenID)
	}
}

// ---- doHelloV2: v2 Hello survives the actual wire round trip ----

func TestDoHelloV2_SendsPopulatedHelloAndReadsAck(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	recvCh := make(chan *protocol.Hello, 1)
	go func() {
		br := bufio.NewReader(server)
		line, err := protocol.ReadFrame(br)
		if err != nil {
			return
		}
		msg, err := protocol.Decode(bytes.TrimRight(line, "\n"))
		if err != nil {
			return
		}
		if h, ok := msg.(*protocol.Hello); ok {
			recvCh <- h
		}
		_ = protocol.Encode(server, protocol.NewHelloAck("test-bus-v", true))
	}()

	resolved := refinedFixture()
	hello := buildHelloV2(resolved, core.AsUser, "local-sub", "profile-x", "ou_1", "sub_remote", "scope-1", resolved.TargetResource)

	ack, _, err := doHelloV2(client, hello)
	if err != nil {
		t.Fatalf("doHelloV2: unexpected error: %v", err)
	}
	if ack == nil || !ack.FirstForKey {
		t.Fatalf("expected a FirstForKey ack, got %+v", ack)
	}

	select {
	case got := <-recvCh:
		if got.RemoteSubscriptionID != "sub_remote" || got.ConsumerScopeID != "scope-1" || got.Identity != "user" {
			t.Errorf("bus received a Hello without its v2 fields intact: %+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("bus side never received the Hello frame")
	}
}

// ---- applyRemoteSubscriptionPlan: the ONLY remote write ----

type fakeApplyAPI struct {
	listResp *larkeventv1.ListSubscriptionResp
	listErr  error

	// listPageAt, when non-nil, overrides listResp/listErr and returns the
	// page for the given 0-indexed call number — used to test the plan
	// stage's pagination (a fixed listResp can only ever serve one page).
	listPageAt func(call int) *larkeventv1.ListSubscriptionResp

	createResp *larkeventv1.CreateSubscriptionResp
	createErr  error

	reactivateResp *larkeventv1.ReactivateSubscriptionResp
	reactivateErr  error

	getEncryptKeyResp *larkeventv1.GetEncryptKeySubscriptionResp
	getEncryptKeyErr  error

	listCalls          int
	createCalls        int
	reactivateCalls    int
	getEncryptKeyCalls int
}

func (f *fakeApplyAPI) List(context.Context, *larkeventv1.ListSubscriptionReq) (*larkeventv1.ListSubscriptionResp, error) {
	call := f.listCalls
	f.listCalls++
	if f.listPageAt != nil {
		return f.listPageAt(call), f.listErr
	}
	return f.listResp, f.listErr
}

func (f *fakeApplyAPI) Create(context.Context, *larkeventv1.CreateSubscriptionReq) (*larkeventv1.CreateSubscriptionResp, error) {
	f.createCalls++
	return f.createResp, f.createErr
}

func (f *fakeApplyAPI) Reactivate(context.Context, *larkeventv1.ReactivateSubscriptionReq) (*larkeventv1.ReactivateSubscriptionResp, error) {
	f.reactivateCalls++
	return f.reactivateResp, f.reactivateErr
}

func (f *fakeApplyAPI) GetEncryptKey(_ context.Context, _ *larkeventv1.GetEncryptKeySubscriptionReq) (*larkeventv1.GetEncryptKeySubscriptionResp, error) {
	f.getEncryptKeyCalls++
	return f.getEncryptKeyResp, f.getEncryptKeyErr
}

func TestApplyRemoteSubscriptionPlan_Create_CallsCreateExactlyOnce(t *testing.T) {
	fake := &fakeApplyAPI{createResp: &larkeventv1.CreateSubscriptionResp{
		Data: &larkeventv1.CreateSubscriptionRespData{Subscription: &larkeventv1.SubscriptionDetail{SubscriptionId: strPtr("sub_new")}},
	}}
	id, created, err := applyRemoteSubscriptionPlan(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", event.ReconcilePlan{Action: event.PlanActionCreate}, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != "sub_new" || !created {
		t.Errorf("got id=%q created=%v, want sub_new/true", id, created)
	}
	if fake.createCalls != 1 || fake.reactivateCalls != 0 {
		t.Errorf("createCalls=%d reactivateCalls=%d, want 1/0", fake.createCalls, fake.reactivateCalls)
	}
}

func TestApplyRemoteSubscriptionPlan_Reuse_NeverWrites(t *testing.T) {
	fake := &fakeApplyAPI{}
	existing := &larkeventv1.SubscriptionDetail{SubscriptionId: strPtr("sub_existing")}
	id, created, err := applyRemoteSubscriptionPlan(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", event.ReconcilePlan{Action: event.PlanActionReuse, Existing: existing}, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != "sub_existing" || created {
		t.Errorf("got id=%q created=%v, want sub_existing/false", id, created)
	}
	if fake.createCalls != 0 || fake.reactivateCalls != 0 {
		t.Errorf("reuse must never call Create/Reactivate; got create=%d reactivate=%d", fake.createCalls, fake.reactivateCalls)
	}
}

func TestApplyRemoteSubscriptionPlan_Suspended_CallsReactivateNotCreate(t *testing.T) {
	fake := &fakeApplyAPI{reactivateResp: &larkeventv1.ReactivateSubscriptionResp{
		Data: &larkeventv1.ReactivateSubscriptionRespData{Subscription: &larkeventv1.SubscriptionDetail{SubscriptionId: strPtr("sub_susp")}},
	}}
	existing := &larkeventv1.SubscriptionDetail{SubscriptionId: strPtr("sub_susp")}
	id, created, err := applyRemoteSubscriptionPlan(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", event.ReconcilePlan{Action: event.PlanActionSuspended, Existing: existing}, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != "sub_susp" || created {
		t.Errorf("got id=%q created=%v, want sub_susp/false (reactivation is not \"created by this attempt\")", id, created)
	}
	if fake.reactivateCalls != 1 || fake.createCalls != 0 {
		t.Errorf("suspended must call Reactivate exactly once, never Create; got create=%d reactivate=%d", fake.createCalls, fake.reactivateCalls)
	}
}

// ---- encrypted create (key-gen atomicity + no-key-on-non-create) ----

// TestApplyRemoteSubscriptionPlan_EncryptedCreate_GeneratesKeyExactlyOnce locks
// that an encrypted create (includeResourceData=true) generates a fresh key via
// newEncryptKeyFunc EXACTLY once and still creates the subscription. The key is
// never returned by applyRemoteSubscriptionPlan (only the remote id is).
func TestApplyRemoteSubscriptionPlan_EncryptedCreate_GeneratesKeyExactlyOnce(t *testing.T) {
	var keyGenCalls int
	restore := newEncryptKeyFunc
	newEncryptKeyFunc = func() (string, error) { keyGenCalls++; return "GENERATED_KEY", nil }
	defer func() { newEncryptKeyFunc = restore }()

	fake := &fakeApplyAPI{createResp: &larkeventv1.CreateSubscriptionResp{
		Data: &larkeventv1.CreateSubscriptionRespData{Subscription: &larkeventv1.SubscriptionDetail{SubscriptionId: strPtr("sub_enc")}},
	}}
	id, created, err := applyRemoteSubscriptionPlan(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", event.ReconcilePlan{Action: event.PlanActionCreate}, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != "sub_enc" || !created {
		t.Errorf("got id=%q created=%v, want sub_enc/true", id, created)
	}
	if keyGenCalls != 1 {
		t.Errorf("newEncryptKeyFunc called %d times, want exactly 1 for an encrypted create", keyGenCalls)
	}
	if fake.createCalls != 1 {
		t.Errorf("createCalls=%d, want 1", fake.createCalls)
	}
}

// TestApplyRemoteSubscriptionPlan_PlaintextAndReuse_NeverGenerateKey proves the
// key is generated ONLY on an encrypted create — never for a plaintext create,
// a reuse, or a suspended reactivate.
func TestApplyRemoteSubscriptionPlan_PlaintextAndReuse_NeverGenerateKey(t *testing.T) {
	var keyGenCalls int
	restore := newEncryptKeyFunc
	newEncryptKeyFunc = func() (string, error) { keyGenCalls++; return "SHOULD_NOT_HAPPEN", nil }
	defer func() { newEncryptKeyFunc = restore }()

	// plaintext create
	fake := &fakeApplyAPI{createResp: &larkeventv1.CreateSubscriptionResp{
		Data: &larkeventv1.CreateSubscriptionRespData{Subscription: &larkeventv1.SubscriptionDetail{SubscriptionId: strPtr("sub_plain")}},
	}}
	if _, _, err := applyRemoteSubscriptionPlan(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", event.ReconcilePlan{Action: event.PlanActionCreate}, false); err != nil {
		t.Fatalf("plaintext create: unexpected error: %v", err)
	}
	// reuse (even with includeResourceData=true: no create happens, no key)
	existing := &larkeventv1.SubscriptionDetail{SubscriptionId: strPtr("sub_reuse")}
	if _, _, err := applyRemoteSubscriptionPlan(context.Background(), &fakeApplyAPI{}, "im.message.created_v1", "im.message?chat_id=oc_aaa", event.ReconcilePlan{Action: event.PlanActionReuse, Existing: existing}, true); err != nil {
		t.Fatalf("reuse: unexpected error: %v", err)
	}
	if keyGenCalls != 0 {
		t.Errorf("newEncryptKeyFunc called %d times, want 0 (no key for plaintext create or reuse)", keyGenCalls)
	}
}

// TestApplyRemoteSubscriptionPlan_EncryptedCreate_KeyGenFailure_FailClosed:
// a key-gen failure aborts the create — never a plaintext fallback.
func TestApplyRemoteSubscriptionPlan_EncryptedCreate_KeyGenFailure_FailClosed(t *testing.T) {
	restore := newEncryptKeyFunc
	newEncryptKeyFunc = func() (string, error) { return "", errors.New("csprng unavailable") }
	defer func() { newEncryptKeyFunc = restore }()

	fake := &fakeApplyAPI{}
	_, _, err := applyRemoteSubscriptionPlan(context.Background(), fake, "im.message.created_v1", "im.message?chat_id=oc_aaa", event.ReconcilePlan{Action: event.PlanActionCreate}, true)
	if err == nil {
		t.Fatal("expected key-gen failure to abort the create, got nil")
	}
	if fake.createCalls != 0 {
		t.Errorf("createCalls=%d, want 0: a key-gen failure must never fall back to a plaintext Create", fake.createCalls)
	}
}

// TestBuildRefinedCreateBody_AtomicEncrypt proves the encrypt_key is set on the
// SAME body as include_resource_data (atomicity), and that a
// plaintext body carries no encrypt block.
func TestBuildRefinedCreateBody_AtomicEncrypt(t *testing.T) {
	enc := buildRefinedCreateBody("im.message.created_v1", "im.message?chat_id=oc_aaa", true, "THE_KEY")
	if enc.PayloadOptions == nil || enc.PayloadOptions.IncludeResourceData == nil || !*enc.PayloadOptions.IncludeResourceData {
		t.Fatalf("encrypted body must set include_resource_data=true, got %+v", enc.PayloadOptions)
	}
	if enc.PayloadOptions.Encrypt == nil || enc.PayloadOptions.Encrypt.EncryptKey == nil || *enc.PayloadOptions.Encrypt.EncryptKey != "THE_KEY" {
		t.Fatalf("encrypted body must carry the encrypt_key atomically, got %+v", enc.PayloadOptions.Encrypt)
	}

	plain := buildRefinedCreateBody("im.message.created_v1", "im.message?chat_id=oc_aaa", false, "")
	if plain.PayloadOptions == nil || plain.PayloadOptions.Encrypt != nil {
		t.Fatalf("plaintext body must carry no encrypt block, got %+v", plain.PayloadOptions)
	}
}

// TestProdRefinedDeps_Plan_EncryptedActiveMatch_ReusesWithZeroGetEncryptKeyCalls
// locks #25a: for an encrypted (IncludeResourceData=true) consume, the plan
// stage reuses an active include_resource_data=true match WITHOUT the front-end
// ever calling GetEncryptKey — key confirmation is deferred to the bus Hello.
func TestProdRefinedDeps_Plan_EncryptedActiveMatch_ReusesWithZeroGetEncryptKeyCalls(t *testing.T) {
	resolved := refinedFixture()
	fake := &fakeApplyAPI{
		listResp: activeEncryptedListResp("sub_enc", "user"),
		// If the front-end ever probed, this would be its response — it must
		// stay untouched.
		getEncryptKeyResp: &larkeventv1.GetEncryptKeySubscriptionResp{
			Data: &larkeventv1.GetEncryptKeySubscriptionRespData{EncryptKey: strPtr("SHOULD_NOT_BE_FETCHED")},
		},
	}
	opts := RefinedOptions{Identity: core.AsUser, IncludeResourceData: true, SubClient: fake}
	deps := prodRefinedDeps(failDialTransport{}, "cli_x", "test-profile", "", resolved, opts)

	plan, err := deps.plan(context.Background())
	if err != nil {
		t.Fatalf("plan err = %v, want nil", err)
	}
	if plan.Action != event.PlanActionReuse {
		t.Errorf("plan.Action = %q, want %q", plan.Action, event.PlanActionReuse)
	}
	if fake.getEncryptKeyCalls != 0 {
		t.Errorf("front-end GetEncryptKey calls = %d, want 0 (key confirmation is deferred to the bus Hello)", fake.getEncryptKeyCalls)
	}
	if fake.listCalls != 1 {
		t.Errorf("List calls = %d, want 1 (plan reconciles remote state once)", fake.listCalls)
	}
}

// TestProdRefinedDeps_Plan_PaginationCapped_WarnsOnErrOut locks the
// production wiring for the refined consume startup chain's plan stage: when
// the List scan hits the page cap without finding a match, it must warn on
// ErrOut rather than silently proceeding as if that were a confirmed
// not-exist, while still returning the same PlanActionCreate a genuine
// not-found would.
func TestProdRefinedDeps_Plan_PaginationCapped_WarnsOnErrOut(t *testing.T) {
	resolved := refinedFixture()
	fake := &fakeApplyAPI{
		listPageAt: func(call int) *larkeventv1.ListSubscriptionResp {
			// Every page: has_more=true, no matching authority item — an
			// unbounded scan would run forever.
			return &larkeventv1.ListSubscriptionResp{Data: &larkeventv1.ListSubscriptionRespData{
				Items: []*larkeventv1.SubscriptionDetail{{
					SubscriptionId: strPtr("sub_other"),
					Authority:      &larkeventv1.Authority{Type: strPtr("app")},
					State:          strPtr("active"),
				}},
				HasMore:   boolPtr(true),
				PageToken: strPtr(fmt.Sprintf("token-%d", call+1)),
			}}
		},
	}
	var stderr bytes.Buffer
	opts := RefinedOptions{ErrOut: &stderr, Identity: core.AsUser, SubClient: fake}
	deps := prodRefinedDeps(failDialTransport{}, "cli_x", "test-profile", "", resolved, opts)

	plan, err := deps.plan(context.Background())
	if err != nil {
		t.Fatalf("plan err = %v, want nil", err)
	}
	if plan.Action != event.PlanActionCreate {
		t.Errorf("plan.Action = %q, want %q", plan.Action, event.PlanActionCreate)
	}
	if !plan.PaginationCapped {
		t.Fatal("plan.PaginationCapped = false, want true")
	}
	if !strings.Contains(stderr.String(), "warning") {
		t.Errorf("ErrOut = %q, want a warning about the capped pagination scan", stderr.String())
	}
}

// ---- runRefinedChain: strict order + dry-run + conflict + apply-ok/startBus-fail ----

// orderRecorder is a tiny, mutex-guarded call-order recorder shared by the
// refinedDeps fakes below -- mirrors cmd/event/subscription/create_test.go's
// fakeCreateAPI call-recording pattern, generalized to all 5 stages.
type orderRecorder struct {
	mu    sync.Mutex
	order []string
}

func (r *orderRecorder) record(stage string) {
	r.mu.Lock()
	r.order = append(r.order, stage)
	r.mu.Unlock()
}

func (r *orderRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.order...)
}

func TestRunRefinedChain_StrictOrder_ProbePlanApplyStartBusHello(t *testing.T) {
	rec := &orderRecorder{}

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	events := []*protocol.Event{
		protocol.NewEvent("im.message.created_v1", "e1", "", 1, json.RawMessage(`{"ok":true}`)),
	}
	go busSide(t, server, events, true)

	deps := refinedDeps{
		probe: func(context.Context) error {
			rec.record("probe")
			return nil
		},
		plan: func(context.Context) (event.ReconcilePlan, error) {
			rec.record("plan")
			return event.ReconcilePlan{Action: event.PlanActionCreate}, nil
		},
		apply: func(context.Context, event.ReconcilePlan) (string, bool, error) {
			rec.record("apply")
			return "sub_new", true, nil
		},
		startBus: func(context.Context) (net.Conn, error) {
			rec.record("startBus")
			return client, nil
		},
		hello: func(_ context.Context, conn net.Conn, remoteSubscriptionID string) (*protocol.HelloAck, *bufio.Reader, error) {
			rec.record("hello")
			if remoteSubscriptionID != "sub_new" {
				t.Errorf("hello received remoteSubscriptionID=%q, want %q (apply's output)", remoteSubscriptionID, "sub_new")
			}
			return &protocol.HelloAck{Type: protocol.MsgTypeHelloAck, FirstForKey: true}, bufio.NewReader(conn), nil
		},
	}

	resolved := refinedFixture()
	opts := RefinedOptions{
		Quiet:     true,
		ErrOut:    io.Discard,
		Out:       io.Discard,
		MaxEvents: 1,
		Identity:  core.AsBot,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := runRefinedChain(ctx, resolved, opts, deps); err != nil {
		t.Fatalf("runRefinedChain: unexpected error: %v", err)
	}

	got := rec.snapshot()
	want := []string{"probe", "plan", "apply", "startBus", "hello"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("call order = %v, want %v", got, want)
	}
}

func TestRunRefinedChain_DryRun_OnlyProbeAndPlanRun_NoApplyNoBusNoWrite(t *testing.T) {
	rec := &orderRecorder{}
	deps := refinedDeps{
		probe: func(context.Context) error {
			rec.record("probe")
			return nil
		},
		plan: func(context.Context) (event.ReconcilePlan, error) {
			rec.record("plan")
			return event.ReconcilePlan{Action: event.PlanActionCreate}, nil
		},
		apply: func(context.Context, event.ReconcilePlan) (string, bool, error) {
			rec.record("apply")
			t.Error("apply must never run under --dry-run")
			return "should-not-happen", true, nil
		},
		startBus: func(context.Context) (net.Conn, error) {
			rec.record("startBus")
			t.Error("startBus must never run under --dry-run")
			return nil, errors.New("must not be called")
		},
		hello: func(context.Context, net.Conn, string) (*protocol.HelloAck, *bufio.Reader, error) {
			rec.record("hello")
			t.Error("hello must never run under --dry-run")
			return nil, nil, errors.New("must not be called")
		},
	}

	var stdout, stderr bytes.Buffer
	opts := RefinedOptions{DryRun: true, Out: &stdout, ErrOut: &stderr, Identity: core.AsUser}
	resolved := refinedFixture()

	if err := runRefinedChain(context.Background(), resolved, opts, deps); err != nil {
		t.Fatalf("dry-run: unexpected error: %v", err)
	}

	got := rec.snapshot()
	want := []string{"probe", "plan"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("dry-run call order = %v, want ONLY %v", got, want)
	}
	if stdout.Len() != 0 {
		t.Errorf("dry-run must never write to stdout (business-event NDJSON channel is reserved); got: %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "dry-run") || !strings.Contains(stderr.String(), resolved.MaterializedKey) {
		t.Errorf("dry-run should describe the plan (mentioning the materialized key) on stderr; got: %q", stderr.String())
	}
}

func TestRunRefinedChain_DryRun_EvenOnConflictingPlan_ReportsInformationallyNoError(t *testing.T) {
	// Mirrors event.subscription create --dry-run's own precedent:
	// dry-run ALWAYS reports the plan informationally, even
	// conflict/suspended -- only preflight itself can fail a dry-run. Only a
	// REAL (non-dry-run) run turns a conflict into a typed error.
	existing := &larkeventv1.SubscriptionDetail{SubscriptionId: strPtr("sub_conflict")}
	deps := refinedDeps{
		probe: func(context.Context) error { return nil },
		plan: func(context.Context) (event.ReconcilePlan, error) {
			return event.ReconcilePlan{Action: event.PlanActionConflict, Existing: existing}, nil
		},
		apply: func(context.Context, event.ReconcilePlan) (string, bool, error) {
			t.Fatal("apply must not run under dry-run")
			return "", false, nil
		},
		startBus: func(context.Context) (net.Conn, error) {
			t.Fatal("startBus must not run under dry-run")
			return nil, nil
		},
		hello: func(context.Context, net.Conn, string) (*protocol.HelloAck, *bufio.Reader, error) {
			t.Fatal("hello must not run under dry-run")
			return nil, nil, nil
		},
	}
	var stderr bytes.Buffer
	opts := RefinedOptions{DryRun: true, Out: io.Discard, ErrOut: &stderr, Identity: core.AsUser}

	if err := runRefinedChain(context.Background(), refinedFixture(), opts, deps); err != nil {
		t.Fatalf("dry-run must report a conflicting plan informationally, not error: %v", err)
	}
}

func TestRunRefinedChain_PlanConflict_NonDryRun_ReturnsTypedErrorBeforeApply(t *testing.T) {
	var applyCalled, startBusCalled, helloCalled bool
	existing := &larkeventv1.SubscriptionDetail{SubscriptionId: strPtr("sub_conflict")}
	deps := refinedDeps{
		probe: func(context.Context) error { return nil },
		plan: func(context.Context) (event.ReconcilePlan, error) {
			return event.ReconcilePlan{
				Action:         event.PlanActionConflict,
				Existing:       existing,
				ConflictFields: []errs.InvalidParam{{Name: "include_resource_data", Reason: "mismatch"}},
			}, nil
		},
		apply: func(context.Context, event.ReconcilePlan) (string, bool, error) {
			applyCalled = true
			return "", false, nil
		},
		startBus: func(context.Context) (net.Conn, error) { startBusCalled = true; return nil, nil },
		hello: func(context.Context, net.Conn, string) (*protocol.HelloAck, *bufio.Reader, error) {
			helloCalled = true
			return nil, nil, nil
		},
	}
	opts := RefinedOptions{ErrOut: io.Discard, Out: io.Discard, Identity: core.AsUser}

	err := runRefinedChain(context.Background(), refinedFixture(), opts, deps)
	if err == nil {
		t.Fatal("expected a typed conflict error on a real (non-dry-run) run")
	}
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	if len(ve.Params) == 0 {
		t.Error("expected ConflictFields to be carried through as Params")
	}
	if applyCalled || startBusCalled || helloCalled {
		t.Error("a conflict must short-circuit before apply/startBus/hello")
	}
}

func TestRunRefinedChain_ApplyOkStartBusFails_InternalErrorWithHint_NoDelete(t *testing.T) {
	var helloCalled bool
	deps := refinedDeps{
		probe: func(context.Context) error { return nil },
		plan: func(context.Context) (event.ReconcilePlan, error) {
			return event.ReconcilePlan{Action: event.PlanActionCreate}, nil
		},
		apply: func(context.Context, event.ReconcilePlan) (string, bool, error) {
			return "sub_new_123", true, nil
		},
		startBus: func(context.Context) (net.Conn, error) {
			return nil, errors.New("dial refused")
		},
		hello: func(context.Context, net.Conn, string) (*protocol.HelloAck, *bufio.Reader, error) {
			helloCalled = true
			return nil, nil, nil
		},
	}
	opts := RefinedOptions{ErrOut: io.Discard, Out: io.Discard, Identity: core.AsBot}

	err := runRefinedChain(context.Background(), refinedFixture(), opts, deps)
	if err == nil {
		t.Fatal("expected an error when startBus fails after a successful apply")
	}
	var ie *errs.InternalError
	if !errors.As(err, &ie) {
		t.Fatalf("expected *errs.InternalError, got %T: %v", err, err)
	}
	if !strings.Contains(ie.Hint, "remote_subscription_id=sub_new_123") {
		t.Errorf("Hint missing remote_subscription_id, got: %q", ie.Hint)
	}
	if !strings.Contains(ie.Hint, "created_by_this_attempt=true") {
		t.Errorf("Hint missing created_by_this_attempt, got: %q", ie.Hint)
	}
	if !strings.Contains(ie.Hint, "next_action") {
		t.Errorf("Hint missing next_action guidance, got: %q", ie.Hint)
	}
	if helloCalled {
		t.Error("hello must not be called when startBus fails")
	}
	// Structural guarantee, not just a runtime assertion: refinedDeps has no
	// delete/cleanup seam at all, and subscriptionApplyAPI exposes no
	// Delete method -- there is no code path by which this failure could
	// reach a remote delete call.
}

// TestRunRefinedChain_ApplyOkHelloFails_InternalErrorWithHint_NoDelete is
// review Fix 3 (Minor #2): a HelloV2 transport/decode error AFTER Apply
// already succeeded must carry the SAME remote_subscription_id /
// created_by_this_attempt / next_action recovery Hint as
// TestRunRefinedChain_ApplyOkStartBusFails_InternalErrorWithHint_NoDelete's
// startBus-fail case -- both leave a remote subscription behind (no-delete
// correctly holds), so both must be equally actionable for retry/reuse.
// Before the fix, this path returned a generic InternalError with no Hint
// at all.
func TestRunRefinedChain_ApplyOkHelloFails_InternalErrorWithHint_NoDelete(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	deps := refinedDeps{
		probe: func(context.Context) error { return nil },
		plan: func(context.Context) (event.ReconcilePlan, error) {
			return event.ReconcilePlan{Action: event.PlanActionCreate}, nil
		},
		apply: func(context.Context, event.ReconcilePlan) (string, bool, error) {
			return "sub_new_456", true, nil
		},
		startBus: func(context.Context) (net.Conn, error) {
			return client, nil
		},
		hello: func(context.Context, net.Conn, string) (*protocol.HelloAck, *bufio.Reader, error) {
			return nil, nil, errors.New("handshake reset")
		},
	}
	opts := RefinedOptions{ErrOut: io.Discard, Out: io.Discard, Identity: core.AsUser}

	err := runRefinedChain(context.Background(), refinedFixture(), opts, deps)
	if err == nil {
		t.Fatal("expected an error when hello fails after a successful apply")
	}
	var ie *errs.InternalError
	if !errors.As(err, &ie) {
		t.Fatalf("expected *errs.InternalError, got %T: %v", err, err)
	}
	if !strings.Contains(ie.Hint, "remote_subscription_id=sub_new_456") {
		t.Errorf("Hint missing remote_subscription_id, got: %q", ie.Hint)
	}
	if !strings.Contains(ie.Hint, "created_by_this_attempt=true") {
		t.Errorf("Hint missing created_by_this_attempt, got: %q", ie.Hint)
	}
	if !strings.Contains(ie.Hint, "next_action") {
		t.Errorf("Hint missing next_action guidance, got: %q", ie.Hint)
	}
}

// TestRunRefinedChain_ApplyOkHelloRejected_HintCarriesRecoveryInfo is review
// Fix 3's bus-rejection counterpart: a rejected hello_ack (e.g.
// SingleConsumer already running) AFTER Apply already succeeded must ALSO
// carry the recovery Hint, on top of (not instead of) rejectionError's own
// pre-existing single-consumer guidance -- both pieces of information are
// useful, and neither call site (legacy Run vs refined) should lose what it
// already had.
func TestRunRefinedChain_ApplyOkHelloRejected_HintCarriesRecoveryInfo(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	deps := refinedDeps{
		probe: func(context.Context) error { return nil },
		plan: func(context.Context) (event.ReconcilePlan, error) {
			return event.ReconcilePlan{Action: event.PlanActionCreate}, nil
		},
		apply: func(context.Context, event.ReconcilePlan) (string, bool, error) {
			return "sub_new_789", true, nil
		},
		startBus: func(context.Context) (net.Conn, error) {
			return client, nil
		},
		hello: func(context.Context, net.Conn, string) (*protocol.HelloAck, *bufio.Reader, error) {
			return &protocol.HelloAck{Type: protocol.MsgTypeHelloAck, Rejected: true, RejectReason: "another consumer (pid 9) is already running"}, bufio.NewReader(client), nil
		},
	}
	opts := RefinedOptions{ErrOut: io.Discard, Out: io.Discard, Identity: core.AsBot}

	err := runRefinedChain(context.Background(), refinedFixture(), opts, deps)
	if err == nil {
		t.Fatal("expected an error when the bus rejects the hello handshake")
	}
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *errs.ValidationError, got %T: %v", err, err)
	}
	if ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Errorf("subtype = %s, want %s", ve.Subtype, errs.SubtypeFailedPrecondition)
	}
	if !strings.Contains(ve.Hint, "remote_subscription_id=sub_new_789") {
		t.Errorf("Hint missing remote_subscription_id, got: %q", ve.Hint)
	}
	if !strings.Contains(ve.Hint, "created_by_this_attempt=true") {
		t.Errorf("Hint missing created_by_this_attempt, got: %q", ve.Hint)
	}
	if !strings.Contains(ve.Hint, "next_action") {
		t.Errorf("Hint missing next_action guidance, got: %q", ve.Hint)
	}
	// The original single-consumer guidance (rejectionError's own Hint --
	// the reject reason itself, e.g. "already running", lives in
	// ve.Error()/Message, not Hint) must survive alongside the appended
	// recovery hint, not be clobbered by it.
	if !strings.Contains(ve.Hint, "allows only one consumer") {
		t.Errorf("Hint dropped the original reject guidance, got: %q", ve.Hint)
	}
	if !strings.Contains(ve.Error(), "already running") {
		t.Errorf("Error() dropped the reject reason, got: %q", ve.Error())
	}
}

func TestRunRefinedChain_Suspended_NonDryRun_AppliesReactivateNotError(t *testing.T) {
	// Suspended proceeds to Apply (which Reactivates) instead of failing --
	// distinct from Conflict, which always fails before Apply.
	var appliedAction string
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	go busSide(t, server, nil, true)

	deps := refinedDeps{
		probe: func(context.Context) error { return nil },
		plan: func(context.Context) (event.ReconcilePlan, error) {
			return event.ReconcilePlan{Action: event.PlanActionSuspended, Existing: &larkeventv1.SubscriptionDetail{SubscriptionId: strPtr("sub_susp")}}, nil
		},
		apply: func(_ context.Context, plan event.ReconcilePlan) (string, bool, error) {
			appliedAction = plan.Action
			return "sub_susp", false, nil
		},
		startBus: func(context.Context) (net.Conn, error) { return client, nil },
		hello: func(_ context.Context, conn net.Conn, _ string) (*protocol.HelloAck, *bufio.Reader, error) {
			return &protocol.HelloAck{Type: protocol.MsgTypeHelloAck, FirstForKey: true}, bufio.NewReader(conn), nil
		},
	}
	opts := RefinedOptions{Quiet: true, ErrOut: io.Discard, Out: io.Discard, Identity: core.AsUser, Timeout: 500 * time.Millisecond}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runRefinedChain(ctx, refinedFixture(), opts, deps); err != nil {
		t.Fatalf("suspended plan should reach Apply/consume, not error: %v", err)
	}
	if appliedAction != event.PlanActionSuspended {
		t.Errorf("apply saw action %q, want %q", appliedAction, event.PlanActionSuspended)
	}
}

// ---- Hello rejection: decrypt_key_unavailable ----
//
// The consume side no longer fetches the encrypt_key itself (no front-end
// prewarm). The BUS fetches it once at Hello time; if that fails it rejects the
// Hello with a fixed decrypt_key_unavailable reason. These tests prove the
// consume side turns that rejection into the right typed error and never
// readies.

// A decrypt_key_unavailable rejection becomes a failed_precondition guiding the
// operator to fix scope/identity — never the single-consumer hint — and the
// consumer never emits the ready marker. There is nothing to roll back: the bus
// rejected before the consumer registered.
func TestRunRefinedChain_HelloRejectedDecryptKeyUnavailable_TypedError_NotReady(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	var stderr bytes.Buffer
	deps := refinedDeps{
		probe: func(context.Context) error { return nil },
		plan: func(context.Context) (event.ReconcilePlan, error) {
			return event.ReconcilePlan{Action: event.PlanActionCreate}, nil
		},
		apply:    func(context.Context, event.ReconcilePlan) (string, bool, error) { return "sub_enc", true, nil },
		startBus: func(context.Context) (net.Conn, error) { return client, nil },
		hello: func(_ context.Context, conn net.Conn, _ string) (*protocol.HelloAck, *bufio.Reader, error) {
			// The bus rejects an encrypted consumer whose key fetch failed.
			return &protocol.HelloAck{
				Type:         protocol.MsgTypeHelloAck,
				Rejected:     true,
				RejectReason: protocol.RejectReasonDecryptKeyUnavailable,
			}, bufio.NewReader(conn), nil
		},
	}
	// Quiet=false so the ready marker WOULD be written if the chain reached it.
	opts := RefinedOptions{Quiet: false, ErrOut: &stderr, Out: io.Discard, Identity: core.AsUser, IncludeResourceData: true}

	err := runRefinedChain(context.Background(), refinedFixture(), opts, deps)
	if err == nil {
		t.Fatal("expected a decrypt_key_unavailable rejection error, got nil")
	}
	var ve *errs.ValidationError
	if !errors.As(err, &ve) || ve.Subtype != errs.SubtypeFailedPrecondition {
		t.Fatalf("expected a failed_precondition, got %T: %v", err, err)
	}
	if !strings.Contains(ve.Error(), "decrypt_key_unavailable") {
		t.Errorf("error must be classified decrypt_key_unavailable, got: %v", ve.Error())
	}
	if !strings.Contains(ve.Hint, "event:encrypt_key:read") {
		t.Errorf("hint must guide the operator to the encrypt_key scope/identity, got: %v", ve.Hint)
	}
	if strings.Contains(ve.Hint, "only one consumer") {
		t.Errorf("a decrypt_key_unavailable rejection must NOT reuse the single-consumer hint, got: %v", ve.Hint)
	}
	if strings.Contains(stderr.String(), "ready") {
		t.Errorf("consumer must NOT emit the ready marker on a decrypt_key_unavailable rejection; stderr:\n%s", stderr.String())
	}
}

// A NON-decrypt rejection (e.g. a SingleConsumer conflict) still flows through
// the generic rejection path with its own recovery hint — a regression guard
// that the decrypt-specific branch didn't swallow every rejection.
func TestRunRefinedChain_HelloRejectedOther_UsesGenericRejectionPath(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	deps := refinedDeps{
		probe: func(context.Context) error { return nil },
		plan: func(context.Context) (event.ReconcilePlan, error) {
			return event.ReconcilePlan{Action: event.PlanActionCreate}, nil
		},
		apply:    func(context.Context, event.ReconcilePlan) (string, bool, error) { return "sub_enc", true, nil },
		startBus: func(context.Context) (net.Conn, error) { return client, nil },
		hello: func(_ context.Context, conn net.Conn, _ string) (*protocol.HelloAck, *bufio.Reader, error) {
			return &protocol.HelloAck{
				Type:         protocol.MsgTypeHelloAck,
				Rejected:     true,
				RejectReason: "an EventKey consumer is already running",
			}, bufio.NewReader(conn), nil
		},
	}
	opts := RefinedOptions{Quiet: true, ErrOut: io.Discard, Out: io.Discard, Identity: core.AsUser, IncludeResourceData: true}

	err := runRefinedChain(context.Background(), refinedFixture(), opts, deps)
	if err == nil {
		t.Fatal("expected a rejection error, got nil")
	}
	if strings.Contains(err.Error(), "decrypt_key_unavailable") {
		t.Errorf("a non-decrypt rejection must NOT be classified decrypt_key_unavailable, got: %v", err)
	}
}

// ---- prodRefinedDeps: the real production wiring behind RunRefined ----

// TestProdRefinedDeps_HelloClosure_ResolvesRealCurrentIdentity exercises the
// ONE piece of prodRefinedDeps's wiring that runRefinedChain's fake-deps
// tests above cannot reach: the hello closure's own
// resolveCurrentProfileIdentity() call, which reads config.json fresh
// (mirroring internal/event/bus/identity.go's resolveCurrentIdentity — see
// buildHelloV2's own doc comment for why that mirroring matters). Sandboxed
// via LARKSUITE_CLI_CONFIG_DIR, same pattern as
// cmd/event/consume_test.go's newRefinedConsumeTestFactory — never touches
// the real host config.
func TestProdRefinedDeps_HelloClosure_ResolvesRealCurrentIdentity(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("LARKSUITE_CLI_CONFIG_DIR", dir)
	configJSON := `{
		"currentApp": "test-profile",
		"apps": [
			{"name": "test-profile", "appId": "cli_hello_test", "users": [{"userOpenId": "ou_hello_1"}]}
		]
	}`
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(configJSON), 0600); err != nil {
		t.Fatal(err)
	}

	resolved := refinedFixture()
	opts := RefinedOptions{Identity: core.AsUser}
	deps := prodRefinedDeps(failDialTransport{}, "cli_hello_test", "test-profile", "", resolved, opts)

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	recvCh := make(chan *protocol.Hello, 1)
	go func() {
		br := bufio.NewReader(server)
		line, err := protocol.ReadFrame(br)
		if err != nil {
			return
		}
		msg, err := protocol.Decode(bytes.TrimRight(line, "\n"))
		if err != nil {
			return
		}
		if h, ok := msg.(*protocol.Hello); ok {
			recvCh <- h
		}
		_ = protocol.Encode(server, protocol.NewHelloAck("test-bus", true))
	}()

	ack, _, err := deps.hello(context.Background(), client, "sub_remote_prod")
	if err != nil {
		t.Fatalf("hello closure: unexpected error: %v", err)
	}
	if ack == nil || !ack.FirstForKey {
		t.Fatalf("expected a FirstForKey ack, got %+v", ack)
	}

	select {
	case got := <-recvCh:
		if got.Profile != "test-profile" {
			t.Errorf("Profile = %q, want %q (resolved from the sandboxed config.json)", got.Profile, "test-profile")
		}
		if got.UserOpenID != "ou_hello_1" {
			t.Errorf("UserOpenID = %q, want %q", got.UserOpenID, "ou_hello_1")
		}
		if got.RemoteSubscriptionID != "sub_remote_prod" {
			t.Errorf("RemoteSubscriptionID = %q, want %q", got.RemoteSubscriptionID, "sub_remote_prod")
		}
		wantScope := computeConsumerScopeID(resolved.Definition.Key, resolved.MaterializedKey, "user", "cli_hello_test", "ou_hello_1")
		if got.ConsumerScopeID != wantScope {
			t.Errorf("ConsumerScopeID = %q, want %q (must match computeConsumerScopeID fed the same real-resolved inputs)", got.ConsumerScopeID, wantScope)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("bus side never received the Hello frame")
	}
}

// TestProdRefinedDeps_HelloClosure_BotIdentity_ConsumerScopeIDIndependentOfUserOpenID
// is review Fix 2 (Minor #3): prodRefinedDeps's hello closure must feed
// computeConsumerScopeID an EMPTY user_open_id for a bot identity, matching
// what buildHelloV2 itself puts on the wire (it drops UserOpenID for a bot —
// TestBuildHelloV2_BotIdentity_NeverCarriesUserOpenID above). Before the fix,
// the closure passed resolveCurrentProfileIdentity()'s real userOpenID
// straight into computeConsumerScopeID regardless of identity, so a bot
// consumer's scope id varied with whoever happened to be logged in on this
// host (nondeterministic, fragments app-level fan-out). Proven here by
// resolving the SAME bot Hello twice against two configs that differ ONLY in
// user_open_id and asserting the resulting ConsumerScopeID is identical, and
// that it equals computeConsumerScopeID fed "" — never the real id.
func TestProdRefinedDeps_HelloClosure_BotIdentity_ConsumerScopeIDIndependentOfUserOpenID(t *testing.T) {
	resolved := refinedFixture()

	runOnce := func(t *testing.T, configJSON string) string {
		t.Helper()
		dir := t.TempDir()
		t.Setenv("LARKSUITE_CLI_CONFIG_DIR", dir)
		if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(configJSON), 0600); err != nil {
			t.Fatal(err)
		}

		opts := RefinedOptions{Identity: core.AsBot}
		deps := prodRefinedDeps(failDialTransport{}, "cli_hello_test", "test-profile", "", resolved, opts)

		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()

		recvCh := make(chan *protocol.Hello, 1)
		go func() {
			br := bufio.NewReader(server)
			line, err := protocol.ReadFrame(br)
			if err != nil {
				return
			}
			msg, err := protocol.Decode(bytes.TrimRight(line, "\n"))
			if err != nil {
				return
			}
			if h, ok := msg.(*protocol.Hello); ok {
				recvCh <- h
			}
			_ = protocol.Encode(server, protocol.NewHelloAck("test-bus", true))
		}()

		if _, _, err := deps.hello(context.Background(), client, "sub_remote_prod"); err != nil {
			t.Fatalf("hello closure: unexpected error: %v", err)
		}

		var scopeID string
		select {
		case got := <-recvCh:
			if got.UserOpenID != "" {
				t.Errorf("bot Hello must never carry UserOpenID, got %q", got.UserOpenID)
			}
			scopeID = got.ConsumerScopeID
		case <-time.After(2 * time.Second):
			t.Fatal("bus side never received the Hello frame")
		}
		return scopeID
	}

	scopeA := runOnce(t, `{
		"currentApp": "test-profile",
		"apps": [
			{"name": "test-profile", "appId": "cli_hello_test", "users": [{"userOpenId": "ou_AAAA"}]}
		]
	}`)
	scopeB := runOnce(t, `{
		"currentApp": "test-profile",
		"apps": [
			{"name": "test-profile", "appId": "cli_hello_test", "users": [{"userOpenId": "ou_BBBB"}]}
		]
	}`)

	if scopeA != scopeB {
		t.Errorf("bot ConsumerScopeID must be independent of user_open_id: %q (ou_AAAA) vs %q (ou_BBBB)", scopeA, scopeB)
	}

	want := computeConsumerScopeID(resolved.Definition.Key, resolved.MaterializedKey, "app", "cli_hello_test", "")
	if scopeA != want {
		t.Errorf("bot ConsumerScopeID = %q, want %q (computed with empty user_open_id, matching buildHelloV2's bot-drops-UserOpenID rule)", scopeA, want)
	}
}
