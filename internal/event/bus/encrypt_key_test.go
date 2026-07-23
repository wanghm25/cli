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
	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/internal/core"
)

// ---- test doubles ----------------------------------------------------------

func ekStrPtr(s string) *string { return &s }

// fakeEncryptKeyClient is a network-free encryptKeyClient. It counts calls, can
// return a key or an error, and can block (to exercise singleflight / timeout).
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

// regEKConn registers a refined *Conn bound to remote subscription subID with a
// fixed owner identity, mirroring how handleHello fixes owner at registration.
func regEKConn(t *testing.T, h *Hub, subID, identity, appID, userOpenID string) *Conn {
	t.Helper()
	server, _ := net.Pipe()
	c := NewConn(server, nil, "im.msg/chat-id/oc", []string{"im.message.receive_v1"}, 4242, "")
	c.SetRemoteSubscriptionID(subID)
	c.SetOwnerIdentity(identity, appID, userOpenID)
	h.RegisterAndIsFirst(c)
	return c
}

// gateWith builds an identityGate whose resolveCurrent/resolveUAT are the given
// closures (the provider only uses those two; hub/logger are inert here).
func gateWith(h *Hub, resolveCurrent func() (currentIdentity, error), resolveUAT func(ctx context.Context, appID, userOpenID string) (string, error)) *identityGate {
	return newIdentityGate(h, resolveCurrent, resolveUAT, nil)
}

// ---- tests -----------------------------------------------------------------

func TestEncryptKeyProvider_CacheHit_NoFetch(t *testing.T) {
	h := NewHub()
	fac := &ekFactory{cli: &fakeEncryptKeyClient{key: "K"}}
	p := newEncryptKeyProvider(h, nil)
	p.setNewClient(fac.make)
	p.static.Set("sub-1", "CACHED_KEY")

	key, ok := p.EncryptKey(context.Background(), "sub-1")
	if !ok || key != "CACHED_KEY" {
		t.Fatalf("EncryptKey = (%q,%v), want (CACHED_KEY,true)", key, ok)
	}
	if got := fac.cli.(*fakeEncryptKeyClient).callCount(); got != 0 {
		t.Errorf("GetEncryptKey calls = %d, want 0 on a cache hit", got)
	}
}

func TestEncryptKeyProvider_BotMiss_FetchesAndBackfills(t *testing.T) {
	h := NewHub()
	c := regEKConn(t, h, "sub-1", "bot", "cli_x", "") // bot: OwnerUserOpenID==""
	fake := &fakeEncryptKeyClient{key: "BOT_KEY"}
	fac := &ekFactory{cli: fake}
	p := newEncryptKeyProvider(h, nil)
	p.setNewClient(fac.make)

	key, ok := p.EncryptKey(context.Background(), "sub-1")
	if !ok || key != "BOT_KEY" {
		t.Fatalf("EncryptKey = (%q,%v), want (BOT_KEY,true)", key, ok)
	}
	if got := fake.callCount(); got != 1 {
		t.Fatalf("GetEncryptKey calls = %d, want 1", got)
	}
	// Backfilled: a second call hits the SDK cache, no new fetch.
	if key2, ok2 := p.EncryptKey(context.Background(), "sub-1"); !ok2 || key2 != "BOT_KEY" {
		t.Errorf("second EncryptKey = (%q,%v), want (BOT_KEY,true)", key2, ok2)
	}
	if got := fake.callCount(); got != 1 {
		t.Errorf("GetEncryptKey calls after backfill = %d, want still 1", got)
	}
	// Bot identity, no UAT ever.
	for _, as := range fac.asSeen {
		if as != core.AsBot {
			t.Errorf("factory identity = %q, want bot", as)
		}
	}
	for _, u := range fac.uats() {
		if u != "" {
			t.Errorf("factory uat = %q, want empty for bot", u)
		}
	}
	if c.DecryptState() != decryptStateDecrypted {
		t.Errorf("decrypt_state = %q, want decrypted", c.DecryptState())
	}
}

func TestEncryptKeyProvider_UserOwnerMatch_FetchesWithFreshUAT(t *testing.T) {
	h := NewHub()
	c := regEKConn(t, h, "sub-1", "user", "cli_x", "ou_me")
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
	p := newEncryptKeyProvider(h, nil)
	p.setIdentityGate(gate)
	p.setNewClient(fac.make)

	key, ok := p.EncryptKey(context.Background(), "sub-1")
	if !ok || key != "USER_KEY" {
		t.Fatalf("EncryptKey = (%q,%v), want (USER_KEY,true)", key, ok)
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
	if c.DecryptState() != decryptStateDecrypted {
		t.Errorf("decrypt_state = %q, want decrypted", c.DecryptState())
	}
}

func TestEncryptKeyProvider_UserOwnerMismatch_NoFetch_NoHistoricalUAT(t *testing.T) {
	h := NewHub()
	c := regEKConn(t, h, "sub-1", "user", "cli_x", "ou_owner")
	fake := &fakeEncryptKeyClient{key: "SHOULD_NOT_FETCH"}
	fac := &ekFactory{cli: fake}
	var uatCalls int
	gate := gateWith(h,
		// current is a DIFFERENT user than the owner.
		func() (currentIdentity, error) { return currentIdentity{appID: "cli_x", userOpenID: "ou_current"}, nil },
		func(_ context.Context, _, _ string) (string, error) { uatCalls++; return "uat-historical", nil })
	p := newEncryptKeyProvider(h, nil)
	p.setIdentityGate(gate)
	p.setNewClient(fac.make)

	key, ok := p.EncryptKey(context.Background(), "sub-1")
	if ok || key != "" {
		t.Fatalf("EncryptKey = (%q,%v), want (\"\",false) on owner mismatch", key, ok)
	}
	if got := fake.callCount(); got != 0 {
		t.Errorf("GetEncryptKey calls = %d, want 0: never fetch for a mismatched owner", got)
	}
	if uatCalls != 0 {
		t.Errorf("resolveUAT calls = %d, want 0: NEVER load a historical owner's UAT (spec §8)", uatCalls)
	}
	if !c.StaleIdentity() {
		t.Errorf("StaleIdentity = false, want true on owner mismatch")
	}
	if c.DecryptState() != decryptStateKeyUnavailable {
		t.Errorf("decrypt_state = %q, want decrypt_key_unavailable", c.DecryptState())
	}
}

func TestEncryptKeyProvider_GenuineFailure_CooldownSuppressesRefetch(t *testing.T) {
	h := NewHub()
	c := regEKConn(t, h, "sub-1", "bot", "cli_x", "")
	fake := &fakeEncryptKeyClient{err: errors.New("permission denied: missing event:encrypt_key:read")}
	fac := &ekFactory{cli: fake}
	p := newEncryptKeyProvider(h, nil)
	p.setNewClient(fac.make)

	if key, ok := p.EncryptKey(context.Background(), "sub-1"); ok || key != "" {
		t.Fatalf("EncryptKey = (%q,%v), want (\"\",false)", key, ok)
	}
	if c.DecryptState() != decryptStateKeyUnavailable {
		t.Errorf("decrypt_state = %q, want decrypt_key_unavailable", c.DecryptState())
	}
	// Second miss within the cooldown must NOT re-hit the API.
	if _, ok := p.EncryptKey(context.Background(), "sub-1"); ok {
		t.Fatalf("second EncryptKey ok=true, want false")
	}
	if got := fake.callCount(); got != 1 {
		t.Errorf("GetEncryptKey calls = %d, want 1 (cooldown suppresses re-fetch)", got)
	}
}

func TestEncryptKeyProvider_TransientCtxError_NotCachedAsFailure(t *testing.T) {
	h := NewHub()
	regEKConn(t, h, "sub-1", "bot", "cli_x", "")
	fake := &fakeEncryptKeyClient{err: context.DeadlineExceeded}
	fac := &ekFactory{cli: fake}
	p := newEncryptKeyProvider(h, nil)
	p.setNewClient(fac.make)

	if _, ok := p.EncryptKey(context.Background(), "sub-1"); ok {
		t.Fatalf("EncryptKey ok=true, want false on transient error")
	}
	// The core E2-review lesson: a transient (ctx) error is NOT a permanent failure.
	if p.recentlyFailed("sub-1") {
		t.Errorf("recentlyFailed = true, want false: a context error must not be cached as a permanent failure")
	}
}

func TestEncryptKeyProvider_FetchTimeout_Transient(t *testing.T) {
	h := NewHub()
	regEKConn(t, h, "sub-1", "bot", "cli_x", "")
	block := make(chan struct{})
	defer close(block)
	fake := &fakeEncryptKeyClient{key: "K", block: block} // blocks past the timeout
	fac := &ekFactory{cli: fake}
	p := newEncryptKeyProvider(h, nil)
	p.setNewClient(fac.make)
	p.fetchTimeout = 20 * time.Millisecond

	start := time.Now()
	if _, ok := p.EncryptKey(context.Background(), "sub-1"); ok {
		t.Fatalf("EncryptKey ok=true, want false on timeout")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("EncryptKey blocked %v, want bounded by fetchTimeout", elapsed)
	}
	if p.recentlyFailed("sub-1") {
		t.Errorf("recentlyFailed = true, want false: a timeout is transient, not permanent")
	}
}

func TestEncryptKeyProvider_Singleflight_ConcurrentSameSubID_OneFetch(t *testing.T) {
	h := NewHub()
	regEKConn(t, h, "sub-1", "bot", "cli_x", "")
	block := make(chan struct{})
	fake := &fakeEncryptKeyClient{key: "ONE", block: block}
	fac := &ekFactory{cli: fake}
	p := newEncryptKeyProvider(h, nil)
	p.setNewClient(fac.make)

	const n = 12
	var wg sync.WaitGroup
	results := make([]bool, n)
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			keys[i], results[i] = p.EncryptKey(context.Background(), "sub-1")
		}(i)
	}
	// Give all goroutines time to collapse onto the one in-flight fetch, then release.
	time.Sleep(30 * time.Millisecond)
	close(block)
	wg.Wait()

	for i := 0; i < n; i++ {
		if !results[i] || keys[i] != "ONE" {
			t.Errorf("goroutine %d = (%q,%v), want (ONE,true)", i, keys[i], results[i])
		}
	}
	if got := fake.callCount(); got != 1 {
		t.Errorf("GetEncryptKey calls = %d, want exactly 1 (singleflight)", got)
	}
}

func TestEncryptKeyProvider_UnknownSubID_NoFetch(t *testing.T) {
	h := NewHub()
	fake := &fakeEncryptKeyClient{key: "K"}
	fac := &ekFactory{cli: fake}
	p := newEncryptKeyProvider(h, nil)
	p.setNewClient(fac.make)

	if _, ok := p.EncryptKey(context.Background(), "sub-unknown"); ok {
		t.Fatalf("EncryptKey ok=true, want false for an unregistered subscription")
	}
	if got := fake.callCount(); got != 0 {
		t.Errorf("GetEncryptKey calls = %d, want 0: never guess a key for an unknown subscription", got)
	}
}

func TestEncryptKeyProvider_NoClientConfigured_KeyUnavailable(t *testing.T) {
	h := NewHub()
	c := regEKConn(t, h, "sub-1", "bot", "cli_x", "")
	p := newEncryptKeyProvider(h, nil) // no setNewClient

	if _, ok := p.EncryptKey(context.Background(), "sub-1"); ok {
		t.Fatalf("EncryptKey ok=true, want false with no client configured")
	}
	if c.DecryptState() != decryptStateKeyUnavailable {
		t.Errorf("decrypt_state = %q, want decrypt_key_unavailable", c.DecryptState())
	}
}

func TestEncryptKeyProvider_UserSubNoGate_KeyUnavailable(t *testing.T) {
	h := NewHub()
	c := regEKConn(t, h, "sub-1", "user", "cli_x", "ou_me")
	fake := &fakeEncryptKeyClient{key: "K"}
	fac := &ekFactory{cli: fake}
	p := newEncryptKeyProvider(h, nil)
	p.setNewClient(fac.make) // client set, but NO identity gate

	if _, ok := p.EncryptKey(context.Background(), "sub-1"); ok {
		t.Fatalf("EncryptKey ok=true, want false for a user subscription with no identity gate")
	}
	if got := fake.callCount(); got != 0 {
		t.Errorf("GetEncryptKey calls = %d, want 0: cannot resolve a user identity without a gate", got)
	}
	if c.DecryptState() != decryptStateKeyUnavailable {
		t.Errorf("decrypt_state = %q, want decrypt_key_unavailable", c.DecryptState())
	}
}

// TestEncryptKeyProvider_KeyNeverLogged is the E4 redaction assertion: across a
// successful fetch AND failure paths, the encrypt_key must never appear in any
// log line (spec §4.7 敏感信息红线).
func TestEncryptKeyProvider_KeyNeverLogged(t *testing.T) {
	const secret = "SUPER_SECRET_ENCRYPT_KEY_do_not_log"
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	h := NewHub()
	regEKConn(t, h, "sub-ok", "bot", "cli_x", "")
	regEKConn(t, h, "sub-fail", "bot", "cli_x", "")

	okCli := &fakeEncryptKeyClient{key: secret}
	p := newEncryptKeyProvider(h, logger)
	p.setNewClient(func(as core.Identity, uat string) (encryptKeyClient, error) { return okCli, nil })
	if _, ok := p.EncryptKey(context.Background(), "sub-ok"); !ok {
		t.Fatalf("expected a successful fetch")
	}

	// Now a failing subscription (logs a "key unavailable" line).
	failCli := &fakeEncryptKeyClient{err: errors.New("boom")}
	p.setNewClient(func(as core.Identity, uat string) (encryptKeyClient, error) { return failCli, nil })
	_, _ = p.EncryptKey(context.Background(), "sub-fail")

	if strings.Contains(buf.String(), secret) {
		t.Fatalf("log output contains the encrypt_key; RED LINE violated. log:\n%s", buf.String())
	}
}
