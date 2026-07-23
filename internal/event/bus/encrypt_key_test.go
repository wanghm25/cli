// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/core"
)

// ---- test doubles ----------------------------------------------------------

func ekStrPtr(s string) *string { return &s }

// fakeEncryptKeyClient is a network-free encryptKeyClient. It counts calls, can
// return a key or an error, and can block (to exercise the fetch timeout).
type fakeEncryptKeyClient struct {
	mu    sync.Mutex
	calls int

	key   string
	err   error
	block chan struct{} // if non-nil, GetEncryptKey waits on it (or ctx) first
}

func (f *fakeEncryptKeyClient) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeEncryptKeyClient) GetEncryptKey(ctx context.Context, _ *larkeventv1.GetEncryptKeySubscriptionReq) (*larkeventv1.GetEncryptKeySubscriptionResp, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	return &larkeventv1.GetEncryptKeySubscriptionResp{
		ApiResp: &larkcore.ApiResp{RawBody: []byte(`{"code":0}`)},
		Data:    &larkeventv1.GetEncryptKeySubscriptionRespData{EncryptKey: ekStrPtr(f.key)},
	}, nil
}

// ekFactory records the (identity, uat) each newClient call was made with — the
// redaction/identity assertions hinge on these.
type ekFactory struct {
	mu      sync.Mutex
	cli     encryptKeyClient
	err     error
	asSeen  []core.Identity
	uatSeen []string
}

func (fac *ekFactory) make(as core.Identity, uat string) (encryptKeyClient, error) {
	fac.mu.Lock()
	fac.asSeen = append(fac.asSeen, as)
	fac.uatSeen = append(fac.uatSeen, uat)
	fac.mu.Unlock()
	if fac.err != nil {
		return nil, fac.err
	}
	return fac.cli, nil
}

func (fac *ekFactory) uats() []string {
	fac.mu.Lock()
	defer fac.mu.Unlock()
	out := make([]string, len(fac.uatSeen))
	copy(out, fac.uatSeen)
	return out
}

// ekOwnerConn builds a refined *Conn bound to remote subscription subID with a
// fixed owner identity, exactly as handleHello fixes it at registration — but
// WITHOUT hub registration: fetchAndSet takes the owner conn directly, it never
// looks the owner up in the hub.
func ekOwnerConn(t *testing.T, subID, identity, appID, userOpenID string) *Conn {
	t.Helper()
	server, _ := net.Pipe()
	t.Cleanup(func() { server.Close() })
	c := NewConn(server, nil, "im.msg/chat-id/oc", []string{"im.message.receive_v1"}, 4242, "")
	c.SetRemoteSubscriptionID(subID)
	c.SetOwnerIdentity(identity, appID, userOpenID)
	return c
}

// gateWith builds an identityGate whose resolveCurrent/resolveUAT are the given
// closures (the provider only uses those two; hub/logger are inert here).
func gateWith(h *Hub, resolveCurrent func() (currentIdentity, error), resolveUAT func(ctx context.Context, appID, userOpenID string) (string, error)) *identityGate {
	return newIdentityGate(h, resolveCurrent, resolveUAT, nil)
}

// ---- invariant: the dispatcher provider is a pure static cache -------------

// The provider handed to the SDK dispatcher is the plain StaticEncryptKeyProvider:
// a miss returns ("",false) → SDK fail-closed, with NO fetch path whatsoever
// (zero runtime remote calls on the event hot path).
func TestEncryptKeyProvider_DispatcherProvider_IsPureStaticCache_NoFetchOnMiss(t *testing.T) {
	fac := &ekFactory{cli: &fakeEncryptKeyClient{key: "K"}}
	p := newEncryptKeyProvider(nil)
	p.setNewClient(fac.make)

	dp := p.dispatcherProvider()
	if _, isStatic := dp.(*larkevent.StaticEncryptKeyProvider); !isStatic {
		t.Fatalf("dispatcherProvider() type = %T, want *larkevent.StaticEncryptKeyProvider (no custom fetch path)", dp)
	}

	// A miss on the dispatcher provider returns false and NEVER fetches.
	if key, ok := dp.EncryptKey(context.Background(), "sub-unknown"); ok || key != "" {
		t.Fatalf("dispatcher EncryptKey(miss) = (%q,%v), want (\"\",false) → SDK fail-closed", key, ok)
	}
	if got := fac.cli.(*fakeEncryptKeyClient).callCount(); got != 0 {
		t.Errorf("GetEncryptKey calls = %d, want 0: the dispatcher provider must have NO runtime remote fetch", got)
	}

	// A hit (populated by fetchAndSet) is a pure lookup.
	p.static.Set("sub-hit", "CACHED")
	if key, ok := dp.EncryptKey(context.Background(), "sub-hit"); !ok || key != "CACHED" {
		t.Errorf("dispatcher EncryptKey(hit) = (%q,%v), want (CACHED,true)", key, ok)
	}
	if got := fac.cli.(*fakeEncryptKeyClient).callCount(); got != 0 {
		t.Errorf("GetEncryptKey calls after a hit = %d, want 0", got)
	}
}

// ---- fetchAndSet: the ONE way a key enters the cache (Hello time) ----------

func TestEncryptKeyProvider_FetchAndSet_Bot_FetchesAsBotNoUAT_Caches(t *testing.T) {
	c := ekOwnerConn(t, "sub-1", "bot", "cli_x", "") // bot: OwnerUserOpenID==""
	fake := &fakeEncryptKeyClient{key: "BOT_KEY"}
	fac := &ekFactory{cli: fake}
	p := newEncryptKeyProvider(nil)
	p.setNewClient(fac.make)

	if err := p.fetchAndSet(context.Background(), "sub-1", c); err != nil {
		t.Fatalf("fetchAndSet err = %v, want nil", err)
	}
	if got := fake.callCount(); got != 1 {
		t.Fatalf("GetEncryptKey calls = %d, want 1", got)
	}
	// Cached: the dispatcher provider now hits with zero further fetch.
	if key, ok := p.dispatcherProvider().EncryptKey(context.Background(), "sub-1"); !ok || key != "BOT_KEY" {
		t.Errorf("cached key = (%q,%v), want (BOT_KEY,true)", key, ok)
	}
	// Bot identity, no UAT ever.
	if len(fac.asSeen) != 1 || fac.asSeen[0] != core.AsBot {
		t.Errorf("factory identity = %v, want [bot]", fac.asSeen)
	}
	if us := fac.uats(); len(us) != 1 || us[0] != "" {
		t.Errorf("factory uat = %v, want [\"\"] for bot", us)
	}
	if c.DecryptState() != decryptStateDecrypted {
		t.Errorf("decrypt_state = %q, want decrypted", c.DecryptState())
	}
}

func TestEncryptKeyProvider_FetchAndSet_UserOwnerMatch_FreshUAT_Caches(t *testing.T) {
	h := NewHub()
	c := ekOwnerConn(t, "sub-1", "user", "cli_x", "ou_me")
	fake := &fakeEncryptKeyClient{key: "USER_KEY"}
	fac := &ekFactory{cli: fake}
	var uatCalls int
	gate := gateWith(h,
		func() (currentIdentity, error) { return currentIdentity{appID: "cli_x", userOpenID: "ou_me"}, nil },
		func(_ context.Context, appID, userOpenID string) (string, error) {
			uatCalls++
			if appID != "cli_x" || userOpenID != "ou_me" {
				t.Errorf("resolveUAT(%q,%q), want (cli_x,ou_me)", appID, userOpenID)
			}
			return "uat-fresh", nil
		})
	p := newEncryptKeyProvider(nil)
	p.setIdentityGate(gate)
	p.setNewClient(fac.make)

	if err := p.fetchAndSet(context.Background(), "sub-1", c); err != nil {
		t.Fatalf("fetchAndSet err = %v, want nil", err)
	}
	if uatCalls != 1 {
		t.Errorf("resolveUAT calls = %d, want 1", uatCalls)
	}
	if len(fac.asSeen) != 1 || fac.asSeen[0] != core.AsUser {
		t.Errorf("factory identity = %v, want [user]", fac.asSeen)
	}
	if us := fac.uats(); len(us) != 1 || us[0] != "uat-fresh" {
		t.Errorf("factory uat = %v, want [uat-fresh]", us)
	}
	if key, ok := p.dispatcherProvider().EncryptKey(context.Background(), "sub-1"); !ok || key != "USER_KEY" {
		t.Errorf("cached key = (%q,%v), want (USER_KEY,true)", key, ok)
	}
	if c.DecryptState() != decryptStateDecrypted {
		t.Errorf("decrypt_state = %q, want decrypted", c.DecryptState())
	}
}

// owner != current: NO fetch, NO historical UAT ever loaded (§8 red line).
func TestEncryptKeyProvider_FetchAndSet_UserOwnerMismatch_NoFetch_NoHistoricalUAT(t *testing.T) {
	h := NewHub()
	c := ekOwnerConn(t, "sub-1", "user", "cli_x", "ou_owner")
	fake := &fakeEncryptKeyClient{key: "SHOULD_NOT_FETCH"}
	fac := &ekFactory{cli: fake}
	var uatCalls int
	gate := gateWith(h,
		func() (currentIdentity, error) { return currentIdentity{appID: "cli_x", userOpenID: "ou_current"}, nil },
		func(_ context.Context, _, _ string) (string, error) { uatCalls++; return "uat-historical", nil })
	p := newEncryptKeyProvider(nil)
	p.setIdentityGate(gate)
	p.setNewClient(fac.make)

	err := p.fetchAndSet(context.Background(), "sub-1", c)
	if !errors.Is(err, errEncryptKeyOwnerMismatch) {
		t.Fatalf("fetchAndSet err = %v, want errEncryptKeyOwnerMismatch", err)
	}
	if got := fake.callCount(); got != 0 {
		t.Errorf("GetEncryptKey calls = %d, want 0: never fetch for a mismatched owner", got)
	}
	if uatCalls != 0 {
		t.Errorf("resolveUAT calls = %d, want 0: NEVER load a historical owner's UAT", uatCalls)
	}
	if _, ok := p.dispatcherProvider().EncryptKey(context.Background(), "sub-1"); ok {
		t.Errorf("a mismatched owner must never cache a key")
	}
}

func TestEncryptKeyProvider_FetchAndSet_GenuineFailure_ReturnsError_NoCache(t *testing.T) {
	c := ekOwnerConn(t, "sub-1", "bot", "cli_x", "")
	fake := &fakeEncryptKeyClient{err: errors.New("permission denied: missing event:encrypt_key:read")}
	fac := &ekFactory{cli: fake}
	p := newEncryptKeyProvider(nil)
	p.setNewClient(fac.make)

	if err := p.fetchAndSet(context.Background(), "sub-1", c); err == nil {
		t.Fatal("fetchAndSet err = nil, want a fetch failure")
	}
	if _, ok := p.dispatcherProvider().EncryptKey(context.Background(), "sub-1"); ok {
		t.Errorf("a failed fetch must never cache a key")
	}
}

func TestEncryptKeyProvider_FetchAndSet_EmptyKey_ReturnsError(t *testing.T) {
	c := ekOwnerConn(t, "sub-1", "bot", "cli_x", "")
	fake := &fakeEncryptKeyClient{key: ""} // success, but no key
	fac := &ekFactory{cli: fake}
	p := newEncryptKeyProvider(nil)
	p.setNewClient(fac.make)

	if err := p.fetchAndSet(context.Background(), "sub-1", c); !errors.Is(err, errEncryptKeyEmpty) {
		t.Fatalf("fetchAndSet err = %v, want errEncryptKeyEmpty", err)
	}
}

func TestEncryptKeyProvider_FetchAndSet_NoClientConfigured_Error(t *testing.T) {
	c := ekOwnerConn(t, "sub-1", "bot", "cli_x", "")
	p := newEncryptKeyProvider(nil) // no setNewClient

	if err := p.fetchAndSet(context.Background(), "sub-1", c); !errors.Is(err, errEncryptKeyNoClient) {
		t.Fatalf("fetchAndSet err = %v, want errEncryptKeyNoClient", err)
	}
}

func TestEncryptKeyProvider_FetchAndSet_UserSubNoGate_Error_NoFetch(t *testing.T) {
	c := ekOwnerConn(t, "sub-1", "user", "cli_x", "ou_me")
	fake := &fakeEncryptKeyClient{key: "K"}
	fac := &ekFactory{cli: fake}
	p := newEncryptKeyProvider(nil)
	p.setNewClient(fac.make) // client set, but NO identity gate

	if err := p.fetchAndSet(context.Background(), "sub-1", c); !errors.Is(err, errEncryptKeyNoGate) {
		t.Fatalf("fetchAndSet err = %v, want errEncryptKeyNoGate", err)
	}
	if got := fake.callCount(); got != 0 {
		t.Errorf("GetEncryptKey calls = %d, want 0: cannot resolve a user identity without a gate", got)
	}
}

func TestEncryptKeyProvider_FetchAndSet_EmptySubID_Error(t *testing.T) {
	c := ekOwnerConn(t, "", "bot", "cli_x", "")
	p := newEncryptKeyProvider(nil)
	p.setNewClient((&ekFactory{cli: &fakeEncryptKeyClient{key: "K"}}).make)
	if err := p.fetchAndSet(context.Background(), "", c); !errors.Is(err, errEncryptKeyNoSubID) {
		t.Fatalf("fetchAndSet err = %v, want errEncryptKeyNoSubID", err)
	}
}

// A blocking GetEncryptKey is bounded by the fetch timeout (a hung remote must
// not hang the Hello handler forever).
func TestEncryptKeyProvider_FetchAndSet_Timeout_Bounded(t *testing.T) {
	c := ekOwnerConn(t, "sub-1", "bot", "cli_x", "")
	block := make(chan struct{})
	defer close(block)
	fake := &fakeEncryptKeyClient{key: "K", block: block}
	fac := &ekFactory{cli: fake}
	p := newEncryptKeyProvider(nil)
	p.setNewClient(fac.make)
	p.fetchTimeout = 20 * time.Millisecond

	start := time.Now()
	if err := p.fetchAndSet(context.Background(), "sub-1", c); err == nil {
		t.Fatal("fetchAndSet err = nil, want a timeout error")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("fetchAndSet blocked %v, want bounded by fetchTimeout", elapsed)
	}
}

// ---- Remove ----------------------------------------------------------------

func TestEncryptKeyProvider_Remove_EvictsCachedKey(t *testing.T) {
	p := newEncryptKeyProvider(nil)
	p.static.Set("sub-1", "CACHED")
	p.Remove("sub-1")
	if _, ok := p.dispatcherProvider().EncryptKey(context.Background(), "sub-1"); ok {
		t.Errorf("Remove must evict the cached key")
	}
	p.Remove("")        // empty id is a no-op, never panics
	p.Remove("sub-abc") // unknown id is a no-op
}

// ---- classification is key-free --------------------------------------------

func TestEncryptKeyFailureClass_KeyFreeTokens(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{errEncryptKeyOwnerMismatch, "owner_mismatch"},
		{errEncryptKeyNoGate, "no_identity_gate"},
		{errEncryptKeyNoClient, "no_subscription_client"},
		{errEncryptKeyEmpty, "no_key_returned"},
		{errs.NewPermissionError(errs.SubtypePermissionDenied, "forbidden"), "missing_scopes"},
		{errors.New("some transport error"), "unavailable"},
	}
	for _, tc := range cases {
		if got := encryptKeyFailureClass(tc.err); got != tc.want {
			t.Errorf("encryptKeyFailureClass(%v) = %q, want %q", tc.err, got, tc.want)
		}
	}
}

// ---- redaction (§4.7): the key never appears in any log line ---------------

func TestEncryptKeyProvider_KeyNeverLogged(t *testing.T) {
	const secret = "SUPER_SECRET_ENCRYPT_KEY_do_not_log"
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	okConn := ekOwnerConn(t, "sub-ok", "bot", "cli_x", "")
	failConn := ekOwnerConn(t, "sub-fail", "bot", "cli_x", "")

	p := newEncryptKeyProvider(logger)

	// Success path caches the secret.
	p.setNewClient(func(as core.Identity, uat string) (encryptKeyClient, error) {
		return &fakeEncryptKeyClient{key: secret}, nil
	})
	if err := p.fetchAndSet(context.Background(), "sub-ok", okConn); err != nil {
		t.Fatalf("expected a successful fetch: %v", err)
	}

	// Failure path (the reject classification must be key-free too).
	p.setNewClient(func(as core.Identity, uat string) (encryptKeyClient, error) {
		return &fakeEncryptKeyClient{err: errors.New("boom")}, nil
	})
	err := p.fetchAndSet(context.Background(), "sub-fail", failConn)
	_ = encryptKeyFailureClass(err) // what handleHello logs

	if strings.Contains(buf.String(), secret) {
		t.Fatalf("log output contains the encrypt_key; RED LINE violated. log:\n%s", buf.String())
	}
}
