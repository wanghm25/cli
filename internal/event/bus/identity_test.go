// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/larksuite/cli/internal/event"
)

// --- Task 14: real-time identity gate + BindUser (spec §4.4) ---
//
// These tests exercise identityGate.onConnReady directly (the BIND gate,
// invoked from the WS's OnReady/OnReconnected callbacks). Injected
// resolveCurrent/resolveUAT fakes mean no disk/keychain access is ever
// touched, and a fake bindUser records every call so tests can assert
// exactly which consumers were (or were NOT) bound. Hub.Publish's identity
// gate (the DELIVERY gate — catches a mismatch on every event fan-out, not
// just at connect time) is covered in hub_test.go alongside Publish's other
// tests.

func discardTestLogger() *log.Logger { return log.New(io.Discard, "", 0) }

// newIdentityTestConn builds a real *Conn (net.Pipe-backed, like the rest of
// this package's Hub tests) with its owner identity fixed, ready to register
// with a Hub for identity-gate tests.
func newIdentityTestConn(t *testing.T, pid int, subID, ownerIdentity, ownerAppID, ownerUserOpenID string) *Conn {
	t.Helper()
	server, client := net.Pipe()
	t.Cleanup(func() { server.Close(); client.Close() })
	c := NewConn(server, nil, "test.key", []string{"test.type"}, pid, subID)
	c.SetOwnerIdentity(ownerIdentity, ownerAppID, ownerUserOpenID)
	return c
}

// fakeBindUser records every invocation (the uat it was called with) so
// tests can assert exactly how many times — and with what token — bindUser
// was invoked, and can be configured to fail on specific call numbers to
// simulate an isolated per-consumer failure.
type fakeBindUser struct {
	mu       sync.Mutex
	calls    []string
	failCall map[int]error // 1-indexed call number -> error to return
}

func (f *fakeBindUser) bind(_ context.Context, uat string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, uat)
	if f.failCall != nil {
		if err, ok := f.failCall[len(f.calls)]; ok {
			return err
		}
	}
	return nil
}

func (f *fakeBindUser) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// fakeUATResolver tracks call count and can fail on specific call numbers,
// mirroring fakeBindUser's shape for the OTHER failure point in onConnReady's
// per-consumer loop (spec §4.4 distinguishes "UAT couldn't be minted" from
// "BindUser API call failed" — both must isolate to just that consumer).
type fakeUATResolver struct {
	mu       sync.Mutex
	calls    int
	failCall map[int]error
	uat      string
}

func (f *fakeUATResolver) resolve(_ context.Context, _ string, _ string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.failCall != nil {
		if err, ok := f.failCall[f.calls]; ok {
			return "", err
		}
	}
	return f.uat, nil
}

func (f *fakeUATResolver) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// staticCurrent builds a resolveCurrent fake that always returns the same
// identity — the common case for tests that don't care about freshness.
func staticCurrent(appID, userOpenID string) func() (currentIdentity, error) {
	return func() (currentIdentity, error) { return currentIdentity{appID: appID, userOpenID: userOpenID}, nil }
}

// owner == current: bindUser is called with the resolved UAT, and the
// consumer ends up bound on connID.
func TestIdentityGate_OnConnReady_BindsMatchingOwner(t *testing.T) {
	h := NewHub()
	c := newIdentityTestConn(t, 100, "", "user", "app1", "ou_alice")
	if !h.RegisterAndIsFirst(c) {
		t.Fatal("expected first registration")
	}

	uat := &fakeUATResolver{uat: "uat-for-alice"}
	fb := &fakeBindUser{}
	gate := newIdentityGate(h, staticCurrent("app1", "ou_alice"), uat.resolve, discardTestLogger())

	gate.onConnReady(context.Background(), "conn-1", fb.bind)

	if got := c.BoundConnID(); got != "conn-1" {
		t.Errorf("BoundConnID() = %q, want %q", got, "conn-1")
	}
	if c.StaleIdentity() {
		t.Error("matching owner must not be marked stale_identity")
	}
	if got := c.DegradedReason(); got != "" {
		t.Errorf("DegradedReason() = %q, want \"\" (bind succeeded)", got)
	}
	if fb.callCount() != 1 {
		t.Fatalf("bindUser call count = %d, want 1", fb.callCount())
	}
	if fb.calls[0] != "uat-for-alice" {
		t.Errorf("bindUser called with uat=%q, want %q", fb.calls[0], "uat-for-alice")
	}
}

// owner != current: NO bind, NO UAT load for the (historical) owner, marked
// stale_identity. This is the core security invariant of the bind gate.
func TestIdentityGate_OnConnReady_SkipsMismatchedOwner(t *testing.T) {
	h := NewHub()
	c := newIdentityTestConn(t, 100, "", "user", "app1", "ou_alice")
	h.RegisterAndIsFirst(c)

	uat := &fakeUATResolver{uat: "uat-for-someone"}
	fb := &fakeBindUser{}
	// current is a DIFFERENT user than the owner fixed at registration.
	gate := newIdentityGate(h, staticCurrent("app1", "ou_bob"), uat.resolve, discardTestLogger())

	gate.onConnReady(context.Background(), "conn-1", fb.bind)

	if !c.StaleIdentity() {
		t.Error("owner!=current must be marked stale_identity")
	}
	if got := c.BoundConnID(); got != "" {
		t.Errorf("BoundConnID() = %q, want \"\" (must never bind a stale consumer)", got)
	}
	if fb.callCount() != 0 {
		t.Errorf("bindUser call count = %d, want 0 (must never bind owner!=current)", fb.callCount())
	}
	if uat.callCount() != 0 {
		t.Errorf("resolveUAT call count = %d, want 0 (must NEVER load a UAT for a mismatched/historical owner)", uat.callCount())
	}
}

// Same user already bound on THIS connID: no duplicate Bind.
func TestIdentityGate_OnConnReady_SameConnID_NoDuplicateBind(t *testing.T) {
	h := NewHub()
	c := newIdentityTestConn(t, 100, "", "user", "app1", "ou_alice")
	h.RegisterAndIsFirst(c)

	uat := &fakeUATResolver{uat: "uat-1"}
	fb := &fakeBindUser{}
	gate := newIdentityGate(h, staticCurrent("app1", "ou_alice"), uat.resolve, discardTestLogger())

	gate.onConnReady(context.Background(), "conn-1", fb.bind)
	gate.onConnReady(context.Background(), "conn-1", fb.bind) // same connID again

	if fb.callCount() != 1 {
		t.Errorf("bindUser call count across two onConnReady calls with the SAME connID = %d, want 1 (no duplicate Bind)", fb.callCount())
	}
}

// A reconnect rotates connID -> the next onConnReady call must rebind (the
// old binding is no longer assumed valid).
func TestIdentityGate_OnConnReady_NewConnID_Rebinds(t *testing.T) {
	h := NewHub()
	c := newIdentityTestConn(t, 100, "", "user", "app1", "ou_alice")
	h.RegisterAndIsFirst(c)

	uat := &fakeUATResolver{uat: "uat-1"}
	fb := &fakeBindUser{}
	gate := newIdentityGate(h, staticCurrent("app1", "ou_alice"), uat.resolve, discardTestLogger())

	gate.onConnReady(context.Background(), "conn-1", fb.bind)
	gate.onConnReady(context.Background(), "conn-2", fb.bind) // reconnect: new connID

	if fb.callCount() != 2 {
		t.Errorf("bindUser call count across a reconnect (different connID) = %d, want 2 (must rebind)", fb.callCount())
	}
	if got := c.BoundConnID(); got != "conn-2" {
		t.Errorf("BoundConnID() after reconnect = %q, want %q", got, "conn-2")
	}
}

// A single consumer's BindUser API failure must degrade ONLY that consumer;
// with two matching consumers, the loop must still attempt (and succeed for)
// the other rather than short-circuiting on the first failure.
func TestIdentityGate_OnConnReady_BindFailureIsolatesOnlyThatConsumer(t *testing.T) {
	h := NewHub()
	connX := newIdentityTestConn(t, 100, "test.key:x", "user", "app1", "ou_alice")
	connY := newIdentityTestConn(t, 200, "test.key:y", "user", "app1", "ou_alice")
	h.RegisterAndIsFirst(connX)
	h.RegisterAndIsFirst(connY)

	uat := &fakeUATResolver{uat: "uat-1"}
	fb := &fakeBindUser{failCall: map[int]error{1: errors.New("bind_user: 500 internal error")}}
	gate := newIdentityGate(h, staticCurrent("app1", "ou_alice"), uat.resolve, discardTestLogger())

	gate.onConnReady(context.Background(), "conn-1", fb.bind)

	if fb.callCount() != 2 {
		t.Fatalf("bindUser call count = %d, want 2 (both matching consumers must be attempted — one failure must not skip the other)", fb.callCount())
	}

	var degraded, bound int
	for _, c := range []*Conn{connX, connY} {
		if c.DegradedReason() != "" {
			degraded++
			if got := c.BoundConnID(); got != "" {
				t.Errorf("degraded consumer pid=%d must not also be bound, got BoundConnID=%q", c.PID(), got)
			}
		}
		if c.BoundConnID() == "conn-1" {
			bound++
			if got := c.DegradedReason(); got != "" {
				t.Errorf("bound consumer pid=%d must not also be degraded, got DegradedReason=%q", c.PID(), got)
			}
		}
	}
	if degraded != 1 {
		t.Errorf("degraded consumer count = %d, want exactly 1", degraded)
	}
	if bound != 1 {
		t.Errorf("bound consumer count = %d, want exactly 1", bound)
	}
	// Neither must ever be marked stale_identity — both owners matched
	// current; this is a bind FAILURE, not an identity mismatch.
	if connX.StaleIdentity() || connY.StaleIdentity() {
		t.Error("a bind failure must never be conflated with stale_identity (owner!=current)")
	}
}

// A single consumer's UAT-resolution failure (distinct from a BindUser API
// failure) must ALSO isolate to just that consumer.
func TestIdentityGate_OnConnReady_UATResolutionFailureIsolatesOnlyThatConsumer(t *testing.T) {
	h := NewHub()
	connX := newIdentityTestConn(t, 100, "test.key:x", "user", "app1", "ou_alice")
	connY := newIdentityTestConn(t, 200, "test.key:y", "user", "app1", "ou_alice")
	h.RegisterAndIsFirst(connX)
	h.RegisterAndIsFirst(connY)

	uat := &fakeUATResolver{uat: "uat-1", failCall: map[int]error{1: errors.New("uat store: locked")}}
	fb := &fakeBindUser{}
	gate := newIdentityGate(h, staticCurrent("app1", "ou_alice"), uat.resolve, discardTestLogger())

	gate.onConnReady(context.Background(), "conn-1", fb.bind)

	if uat.callCount() != 2 {
		t.Fatalf("resolveUAT call count = %d, want 2 (both matching consumers must be attempted)", uat.callCount())
	}
	if fb.callCount() != 1 {
		t.Errorf("bindUser call count = %d, want 1 (the consumer whose UAT resolution failed must never reach bindUser)", fb.callCount())
	}

	var degraded, bound int
	for _, c := range []*Conn{connX, connY} {
		if c.DegradedReason() != "" {
			degraded++
		}
		if c.BoundConnID() == "conn-1" {
			bound++
		}
	}
	if degraded != 1 {
		t.Errorf("degraded consumer count = %d, want exactly 1", degraded)
	}
	if bound != 1 {
		t.Errorf("bound consumer count = %d, want exactly 1", bound)
	}
}

// BOT consumers are invisible to onConnReady entirely (Hub.userConns filters
// them out) — never bound, never mutated in any way.
func TestIdentityGate_OnConnReady_BotNeverGatedNeverBound(t *testing.T) {
	h := NewHub()
	userConn := newIdentityTestConn(t, 100, "test.key:user", "user", "app1", "ou_alice")
	botConn := newIdentityTestConn(t, 200, "test.key:bot", "bot", "app1", "")
	h.RegisterAndIsFirst(userConn)
	h.RegisterAndIsFirst(botConn)

	uat := &fakeUATResolver{uat: "uat-1"}
	fb := &fakeBindUser{}
	gate := newIdentityGate(h, staticCurrent("app1", "ou_alice"), uat.resolve, discardTestLogger())

	gate.onConnReady(context.Background(), "conn-1", fb.bind)

	if fb.callCount() != 1 {
		t.Fatalf("bindUser call count = %d, want 1 (only the user consumer)", fb.callCount())
	}
	if got := userConn.BoundConnID(); got != "conn-1" {
		t.Errorf("user consumer BoundConnID() = %q, want %q", got, "conn-1")
	}
	if got := botConn.BoundConnID(); got != "" {
		t.Errorf("bot consumer BoundConnID() = %q, want \"\" (bots are never bound)", got)
	}
	if botConn.StaleIdentity() {
		t.Error("bot consumer must never be marked stale_identity")
	}
	if got := botConn.DegradedReason(); got != "" {
		t.Errorf("bot consumer DegradedReason() = %q, want \"\" (bots are never touched by the gate)", got)
	}
}

// A resolveCurrent failure must degrade EVERY user consumer (fail-closed —
// never bind under an unresolved identity) while leaving bot consumers
// completely untouched, and must not crash/block the WS callback.
func TestIdentityGate_OnConnReady_ResolveCurrentErrorDegradesAllUsersButNotBots(t *testing.T) {
	h := NewHub()
	userConn := newIdentityTestConn(t, 100, "test.key:user", "user", "app1", "ou_alice")
	botConn := newIdentityTestConn(t, 200, "test.key:bot", "bot", "app1", "")
	h.RegisterAndIsFirst(userConn)
	h.RegisterAndIsFirst(botConn)

	uat := &fakeUATResolver{uat: "uat-1"}
	fb := &fakeBindUser{}
	failingResolveCurrent := func() (currentIdentity, error) {
		return currentIdentity{}, errors.New("config.json unreadable")
	}
	gate := newIdentityGate(h, failingResolveCurrent, uat.resolve, discardTestLogger())

	gate.onConnReady(context.Background(), "conn-1", fb.bind)

	if fb.callCount() != 0 {
		t.Errorf("bindUser call count = %d, want 0 (must never bind under an unresolved identity)", fb.callCount())
	}
	if uat.callCount() != 0 {
		t.Errorf("resolveUAT call count = %d, want 0", uat.callCount())
	}
	if got := userConn.DegradedReason(); got == "" {
		t.Error("user consumer should be marked degraded when resolveCurrent errors")
	}
	if got := botConn.BoundConnID(); got != "" || botConn.DegradedReason() != "" || botConn.StaleIdentity() {
		t.Error("bot consumer must be completely unaffected by a resolveCurrent error")
	}
}

// resolveCurrent must be re-resolved FRESH on every onConnReady call — never
// a value cached from a previous call (which would stand in for the
// bus-startup cache the spec explicitly forbids reusing). A fake that
// returns a DIFFERENT identity on each call proves the gate's behavior
// tracks it: bound on call 1, stale on call 2 once "current" changed
// underneath the SAME registered owner.
func TestIdentityGate_ResolveCurrent_FreshAcrossCalls(t *testing.T) {
	h := NewHub()
	c := newIdentityTestConn(t, 100, "", "user", "app1", "ou_alice")
	h.RegisterAndIsFirst(c)

	identities := []currentIdentity{
		{appID: "app1", userOpenID: "ou_alice"}, // call 1: matches -> bind
		{appID: "app1", userOpenID: "ou_bob"},   // call 2: current changed -> mismatch -> stale
	}
	var resolveCalls int
	resolveCurrent := func() (currentIdentity, error) {
		id := identities[resolveCalls]
		resolveCalls++
		return id, nil
	}
	uat := &fakeUATResolver{uat: "uat-1"}
	fb := &fakeBindUser{}
	gate := newIdentityGate(h, resolveCurrent, uat.resolve, discardTestLogger())

	gate.onConnReady(context.Background(), "conn-1", fb.bind)
	if got := c.BoundConnID(); got != "conn-1" {
		t.Fatalf("after call 1 (owner==current): BoundConnID() = %q, want %q", got, "conn-1")
	}
	if c.StaleIdentity() {
		t.Fatal("after call 1: must not be stale")
	}

	gate.onConnReady(context.Background(), "conn-1", fb.bind) // same connID, but current changed underneath
	if !c.StaleIdentity() {
		t.Fatal("after call 2 (owner!=NEW current): expected stale_identity — proves resolveCurrent was re-resolved fresh, not cached from call 1")
	}

	if resolveCalls != 2 {
		t.Errorf("resolveCurrent call count = %d, want 2 (once per onConnReady call)", resolveCalls)
	}
	if fb.callCount() != 1 {
		t.Errorf("bindUser call count = %d, want 1 (call 2's now-mismatched owner must NOT trigger a new bind)", fb.callCount())
	}
}

// Dedicated identity.go race coverage: onConnReady (bind gate, its own
// goroutine in production via the WS callback) and Hub.Publish (delivery
// gate, the source's emit goroutine) run concurrently against the SAME
// registered Conn. Run with -race.
func TestIdentityGate_OnConnReadyConcurrentWithPublish_Race(t *testing.T) {
	h := NewHub()
	c := newIdentityTestConn(t, 100, "", "user", "app1", "ou_alice")
	h.RegisterAndIsFirst(c)
	c.sendCh = make(chan interface{}, 8) // small: this test only cares about race-freedom, not delivery

	uat := &fakeUATResolver{uat: "uat-1"}
	fb := &fakeBindUser{}
	gate := newIdentityGate(h, staticCurrent("app1", "ou_alice"), uat.resolve, discardTestLogger())
	h.SetCurrentResolver(staticCurrent("app1", "ou_alice"))

	var wg sync.WaitGroup
	stop := make(chan struct{})
	deadline := time.Now().Add(500 * time.Millisecond)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for time.Now().Before(deadline) {
			select {
			case <-stop:
				return
			default:
			}
			gate.onConnReady(context.Background(), "conn-race", fb.bind)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for time.Now().Before(deadline) {
			select {
			case <-stop:
				return
			default:
			}
			h.Publish(&event.RawEvent{
				EventID:   strconv.Itoa(i),
				EventType: "test.type",
				Payload:   json.RawMessage(`{}`),
			})
			i++
		}
	}()

	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
}
