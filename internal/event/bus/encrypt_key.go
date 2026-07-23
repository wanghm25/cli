// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/internal/core"
)

// decrypt_state tokens (spec §4.7 "失败语义与可观测性"). A small, stable
// vocabulary a status display can key off (E7). decryptStateFailed is added by
// E6 (the SDK-decrypt-failure path); E4 only ever produces the first two.
const (
	decryptStateDecrypted      = "decrypted"
	decryptStateKeyUnavailable = "decrypt_key_unavailable"
)

// encryptKeyProviderDefaults — internal control-plane housekeeping timings,
// not a scalable data path (mirrors lifecycleExecutor's own rationale). Fields
// on the provider (not consts) so tests shrink them; deliberately not
// env-configurable.
const (
	defaultEncryptKeyFetchTimeout   = 5 * time.Second
	defaultEncryptKeyFailCooldown   = 30 * time.Second
	defaultEncryptKeyMaxConcurrency = 4
)

// encryptKeyClient is the narrow seam encryptKeyProvider needs from
// *eventlib.SubscriptionClient: GetEncryptKey ONLY (spec §4.7 — the provider
// never lists/creates/patches). *eventlib.SubscriptionClient satisfies it
// structurally (it gained GetEncryptKey in task E1), so bus.go's
// SetSubscriptionClient passes one straight through; tests substitute a fake
// with no *lark.Client or network call — the same test-seam idiom as
// subscriptionActionClient (lifecycle.go).
type encryptKeyClient interface {
	GetEncryptKey(ctx context.Context, req *larkeventv1.GetEncryptKeySubscriptionReq) (*larkeventv1.GetEncryptKeySubscriptionResp, error)
}

// encryptKeyProvider is the CLI's larkevent.EncryptKeyProvider implementation
// (spec §4.7 "解密位置与运行时边界"): the SDK EventDispatcher calls
// EncryptKey(ctx, subscription_id) SYNCHRONOUSLY, before parsing/routing an
// encrypted subscription envelope. On a cache miss it fetches the key via
// GetEncryptKey using the OWNER identity resolved from the bus's own consumer
// registry, then backfills the SDK's concurrency-safe StaticEncryptKeyProvider
// so subsequent events for the same subscription decrypt with zero network.
//
// SECURITY (spec §4.7 敏感信息红线): the fetched encrypt_key lives ONLY inside
// the SDK StaticEncryptKeyProvider's in-memory map (released on Remove / bus
// exit). It is NEVER logged, put on the wire (IPC), placed in status, or
// returned anywhere except back to the SDK dispatcher that asked for it. The
// UAT used to fetch it is likewise never logged. owner != current NEVER loads a
// historical owner's UAT (spec §8 red line, reused from the Task 14 gate).
//
// DISPATCH SAFETY (spec §4.7 "缓存 miss 时不可避免会延迟当前事件的 dispatch"):
// because EncryptKey blocks the dispatcher, a miss is guarded by (1) a short
// per-fetch timeout, (2) per-subscription_id singleflight (one in-flight fetch
// per id, concurrent callers share its result), (3) bounded total concurrency,
// and (4) a failure cooldown so a genuine no-key does not hammer the API. There
// is NO infinite retry. A transient failure (context cancel/deadline) is NOT
// cached as a permanent failure — it is a bus-restart / cache-eviction / race
// fallback that a later event re-attempts (the E2-review lesson).
type encryptKeyProvider struct {
	// static is the SDK's own concurrency-safe cache (the authoritative key
	// store). The provider only ever Set()s a freshly fetched key here and
	// Remove()s on lifecycle deletion (E6); it never reads keys back out for
	// any purpose other than answering EncryptKey.
	static *larkevent.StaticEncryptKeyProvider

	// hub is the subscription_id -> owner map (spec §4.7 note 3): the Hub's
	// consumer registry IS that map — connsByRemoteSubscriptionID(subID) yields
	// the refined consumer(s), whose owner{Identity,AppID,UserOpenID} fields
	// (fixed at Hello registration, spec §4.4) are the ONLY identity signal.
	// An unknown subscription (no registered consumer) is never guessed at.
	hub *Hub

	// gate supplies resolveCurrent + resolveUAT (spec §4.4/§8) for a USER
	// subscription's owner==current check and fresh-UAT mint. nil until
	// SetIdentityProviders — a user subscription then cannot be served (returns
	// no key), but a bot subscription (no UAT needed) still can.
	gate *identityGate

	// newClient builds a GetEncryptKey-capable client bound to a resolved
	// identity. nil until SetSubscriptionClient — a nil newClient means the
	// provider can never fetch (every miss returns no key). Guarded by its own
	// RWMutex only because SetSubscriptionClient may run slightly before Run()
	// on the setup goroutine while a test drives EncryptKey; in production it is
	// set-once-before-Run.
	newClientMu sync.RWMutex
	newClient   func(as core.Identity, uat string) (encryptKeyClient, error)

	logger *log.Logger

	// inflight is the singleflight map: subscription_id -> *encryptKeyFetch.
	inflight sync.Map

	// failMu guards failedAt: subscription_id -> time of last GENUINE (non-
	// transient) fetch failure. A subsequent miss within failCooldown returns
	// no key without re-fetching.
	failMu   sync.Mutex
	failedAt map[string]time.Time

	// sem bounds total concurrent in-flight fetches.
	sem chan struct{}

	fetchTimeout time.Duration
	failCooldown time.Duration
}

// encryptKeyFetch is one singleflight in-flight fetch: done is closed when the
// leader finishes, at which point key/ok hold its result for every waiter.
type encryptKeyFetch struct {
	done chan struct{}
	key  string
	ok   bool
}

// newEncryptKeyProvider builds a provider with an empty SDK cache and default
// guard timings. identityGate/newClient are injected post-construction by
// bus.go's SetIdentityProviders/SetSubscriptionClient (same "optional, wire
// before Run()" convention as the lifecycle action's own dependencies).
func newEncryptKeyProvider(hub *Hub, logger *log.Logger) *encryptKeyProvider {
	return &encryptKeyProvider{
		static:       larkevent.NewStaticEncryptKeyProvider(nil),
		hub:          hub,
		logger:       logger,
		failedAt:     make(map[string]time.Time),
		sem:          make(chan struct{}, defaultEncryptKeyMaxConcurrency),
		fetchTimeout: defaultEncryptKeyFetchTimeout,
		failCooldown: defaultEncryptKeyFailCooldown,
	}
}

func (p *encryptKeyProvider) setIdentityGate(g *identityGate) { p.gate = g }

func (p *encryptKeyProvider) setNewClient(fn func(as core.Identity, uat string) (encryptKeyClient, error)) {
	p.newClientMu.Lock()
	p.newClient = fn
	p.newClientMu.Unlock()
}

func (p *encryptKeyProvider) logf(format string, args ...interface{}) {
	if p.logger != nil {
		p.logger.Printf(format, args...)
	}
}

// Remove drops a subscription's cached key (E6 lifecycle removal). Idempotent.
// Also clears any recorded failure for the id so a fresh subscription reusing
// the same id (unlikely, but harmless) starts clean.
func (p *encryptKeyProvider) Remove(subID string) {
	if subID == "" {
		return
	}
	p.static.Remove(subID)
	p.failMu.Lock()
	delete(p.failedAt, subID)
	p.failMu.Unlock()
}

// EncryptKey implements larkevent.EncryptKeyProvider. Returns (key, true) only
// when a usable key is available; ("", false) otherwise (the SDK then leaves
// the envelope encrypted, which fails the downstream parse fail-closed — no
// plaintext fallback, spec §4.7).
func (p *encryptKeyProvider) EncryptKey(ctx context.Context, subID string) (string, bool) {
	// Fast path: the SDK cache already has it (the common case after the first
	// fetch or an E5 prewarm). No lock contention beyond the static's own RWMutex.
	if key, ok := p.static.EncryptKey(ctx, subID); ok {
		return key, true
	}
	if subID == "" {
		return "", false
	}

	// Singleflight: collapse concurrent misses for the SAME subscription_id
	// into one fetch. The leader (loaded==false) performs it; every waiter
	// blocks on done and shares the outcome.
	f := &encryptKeyFetch{done: make(chan struct{})}
	actual, loaded := p.inflight.LoadOrStore(subID, f)
	if loaded {
		lead := actual.(*encryptKeyFetch)
		select {
		case <-lead.done:
			return lead.key, lead.ok
		case <-ctx.Done():
			// Our own dispatch was cancelled while waiting — transient, drop
			// this event; the leader still completes and backfills the cache.
			return "", false
		}
	}
	defer func() {
		close(f.done)
		p.inflight.Delete(subID)
	}()

	key, ok := p.fetch(ctx, subID)
	f.key, f.ok = key, ok
	return key, ok
}

// fetch is the singleflight leader body: owner resolution -> owner==current
// gate -> UAT -> GetEncryptKey -> backfill. Returns ("", false) on any failure
// (already recorded/marked). Never logs the key.
func (p *encryptKeyProvider) fetch(ctx context.Context, subID string) (string, bool) {
	// Do not hammer a genuine no-key: honor the failure cooldown first.
	if p.recentlyFailed(subID) {
		return "", false
	}

	// subscription_id -> owner, via the Hub registry (spec §4.7 note 3). An
	// unknown subscription / one with no active consumer is never guessed at.
	conns := p.hub.connsByRemoteSubscriptionID(subID)
	if len(conns) == 0 {
		p.logf("[encrypt-key] no consumer registered for subscription_id=%s; not fetching a key", subID)
		return "", false
	}
	owner := conns[0]

	identity, uat, ok := p.resolveOwnerIdentity(ctx, owner, conns)
	if !ok {
		// resolveOwnerIdentity already marked the consumers appropriately.
		return "", false
	}

	p.newClientMu.RLock()
	newClient := p.newClient
	p.newClientMu.RUnlock()
	if newClient == nil {
		p.logf("[encrypt-key] no subscription client configured on this bus; cannot fetch key for subscription_id=%s", subID)
		markDecryptKeyUnavailable(conns)
		return "", false
	}
	cli, err := newClient(identity, uat)
	if err != nil {
		p.logf("[encrypt-key] building subscription client failed for subscription_id=%s: %v", subID, err)
		markDecryptKeyUnavailable(conns)
		p.recordFailure(subID)
		return "", false
	}

	// Bound both slot-acquisition and the call itself by one short timeout.
	fctx, cancel := context.WithTimeout(ctx, p.fetchTimeout)
	defer cancel()
	select {
	case p.sem <- struct{}{}:
		defer func() { <-p.sem }()
	case <-fctx.Done():
		// Overloaded / cancelled before we even got a slot: transient, not a
		// permanent failure — do NOT cache, a later event re-attempts.
		return "", false
	}

	req := larkeventv1.NewGetEncryptKeySubscriptionReqBuilder().SubscriptionId(subID).Build()
	resp, err := cli.GetEncryptKey(fctx, req)
	if err != nil {
		if isTransientCtxErr(fctx, err) {
			// context cancel/deadline: bus restart / eviction / race window,
			// never a confirmed no-key (E2-review lesson). Drop this event
			// only; do not cache, do not mark permanently.
			return "", false
		}
		// A genuine business/transport failure (missing event:encrypt_key:read
		// scope, revoked auth, subscription gone, ...). The SDK error is
		// deliberately not surfaced verbatim (no oracle, spec §4.7): only a
		// classified state + a cooldown.
		p.logf("[encrypt-key] GetEncryptKey failed for subscription_id=%s (key unavailable)", subID)
		markDecryptKeyUnavailable(conns)
		p.recordFailure(subID)
		return "", false
	}

	key := ""
	if resp != nil && resp.Data != nil && resp.Data.EncryptKey != nil {
		key = *resp.Data.EncryptKey
	}
	if key == "" {
		p.logf("[encrypt-key] GetEncryptKey returned no key for subscription_id=%s", subID)
		markDecryptKeyUnavailable(conns)
		p.recordFailure(subID)
		return "", false
	}

	// Backfill the SDK cache and mark the consumers healthy. NEVER log key.
	p.static.Set(subID, key)
	p.clearFailure(subID)
	markDecrypted(conns)
	return key, true
}

// resolveOwnerIdentity turns owner's fixed registration identity into the
// (identity, uat) GetEncryptKey must run as (spec §4.7 "GetEncryptKey 必须使用
// 与 Subscription authority 一致的身份"):
//   - bot/legacy owner (OwnerUserOpenID()=="") -> core.AsBot, no UAT, no gate
//     (bot consumers are NEVER identity-gated — the Task 14 precedent).
//   - user owner -> owner==current gate (reuse ownerMatchesCurrent /
//     resolveCurrent). owner != current marks stale_identity +
//     decrypt_key_unavailable and returns without loading ANY UAT (spec §8:
//     never a historical owner's UAT). Otherwise a FRESH UAT is minted for the
//     current identity.
//
// ok==false means "cannot serve this subscription now"; the conns have already
// been marked. It NEVER returns a UAT for an identity other than the freshly
// resolved current one.
func (p *encryptKeyProvider) resolveOwnerIdentity(ctx context.Context, owner *Conn, conns []*Conn) (core.Identity, string, bool) {
	if owner.OwnerUserOpenID() == "" {
		return core.AsBot, "", true
	}
	if p.gate == nil {
		p.logf("[encrypt-key] no identity gate configured; cannot resolve a user identity for a user subscription")
		markDecryptKeyUnavailable(conns)
		return "", "", false
	}
	cur, err := p.gate.resolveCurrent()
	if err != nil {
		// current identity unresolved: transient-ish (config/keychain may
		// recover) — mark it but do NOT record a permanent failure.
		for _, c := range conns {
			c.SetDegraded(reasonCurrentIdentityUnresolved)
			c.SetDecryptState(decryptStateKeyUnavailable)
		}
		return "", "", false
	}
	if !ownerMatchesCurrent(owner.OwnerAppID(), owner.OwnerUserOpenID(), cur) {
		// owner != current: NO fetch, NO historical UAT (spec §8). Mark
		// stale_identity (as the Publish/lifecycle gates do) AND the decrypt
		// state so status can advise switching profile.
		for _, c := range conns {
			c.SetStaleIdentity()
			c.SetDecryptState(decryptStateKeyUnavailable)
		}
		return "", "", false
	}
	uat, err := p.gate.resolveUAT(ctx, cur.appID, cur.userOpenID)
	if err != nil {
		p.logf("[encrypt-key] UAT resolution failed for a user subscription (key unavailable)")
		markDecryptKeyUnavailable(conns)
		return "", "", false
	}
	return core.AsUser, uat, true
}

func (p *encryptKeyProvider) recentlyFailed(subID string) bool {
	p.failMu.Lock()
	defer p.failMu.Unlock()
	at, ok := p.failedAt[subID]
	if !ok {
		return false
	}
	if time.Since(at) >= p.failCooldown {
		delete(p.failedAt, subID)
		return false
	}
	return true
}

func (p *encryptKeyProvider) recordFailure(subID string) {
	p.failMu.Lock()
	p.failedAt[subID] = time.Now()
	p.failMu.Unlock()
}

func (p *encryptKeyProvider) clearFailure(subID string) {
	p.failMu.Lock()
	delete(p.failedAt, subID)
	p.failMu.Unlock()
}

// isTransientCtxErr reports whether a GetEncryptKey failure was a
// context cancel/deadline (transient) rather than a confirmed no-key. Checks
// fctx.Err() directly (robust regardless of how the client wrapped the error)
// as well as the error chain.
func isTransientCtxErr(fctx context.Context, err error) bool {
	if fctx.Err() != nil {
		return true
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// markDecryptKeyUnavailable sets decrypt_state=decrypt_key_unavailable on every
// matched consumer (spec §4.7 失败语义). A pure local state write — never logs
// a reason that could leak key material.
func markDecryptKeyUnavailable(conns []*Conn) {
	for _, c := range conns {
		c.SetDecryptState(decryptStateKeyUnavailable)
	}
}

// markDecrypted records that a usable key is now available (a strong proxy for
// "these events will decrypt"): decrypt_state=decrypted, clearing a prior
// decrypt_key_unavailable. The SDK decrypts transparently downstream, so this
// is the closest signal the bus has without hooking per-event AES.
func markDecrypted(conns []*Conn) {
	for _, c := range conns {
		c.SetDecryptState(decryptStateDecrypted)
	}
}

// Compile-time assertion: *encryptKeyProvider is a larkevent.EncryptKeyProvider.
var _ larkevent.EncryptKeyProvider = (*encryptKeyProvider)(nil)
