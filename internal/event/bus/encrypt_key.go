// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package bus

import (
	"context"
	"errors"
	"sync"
	"time"

	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/core"
)

// decrypt_state tokens for failure/observability. A small, stable
// vocabulary a status display can key off. decryptStateFailed covers
// the SDK-decrypt-failure path; the Hello-time fetch only ever produces
// decrypted (success) or a rejected Hello (no consumer, so no state).
const (
	decryptStateDecrypted      = "decrypted"
	decryptStateKeyUnavailable = "decrypt_key_unavailable"
	decryptStateFailed         = "decrypt_failed" // key held but SDK decrypt/parse failed
)

// defaultEncryptKeyFetchTimeout bounds the ONE Hello-time GetEncryptKey call.
// A field on the provider (not a const) so tests can shrink it; deliberately
// not env-configurable — this is control-plane housekeeping, run once per
// encrypted consumer registration, never a hot path.
const defaultEncryptKeyFetchTimeout = 5 * time.Second

// encryptKeyClient is the narrow seam the provider needs from
// *event.SubscriptionClient: GetEncryptKey ONLY (the provider never
// lists/creates/patches). *event.SubscriptionClient satisfies it structurally,
// so bus.go's SetSubscriptionClient passes one straight through; tests
// substitute a fake with no *lark.Client or network call — the same test-seam
// idiom as subscriptionActionClient (lifecycle.go).
type encryptKeyClient interface {
	GetEncryptKey(ctx context.Context, req *larkeventv1.GetEncryptKeySubscriptionReq) (*larkeventv1.GetEncryptKeySubscriptionResp, error)
}

// encryptKeyProvider manages per-subscription encrypt_keys for the bus's SDK
// dispatcher. It is NOT itself the dispatcher's provider — the SDK dispatcher
// is wired to the plain static cache (dispatcherProvider() below), whose
// EncryptKey is a pure map lookup: a miss returns ("", false) and the SDK
// fails the decrypt CLOSED, with ZERO runtime remote calls. There is no
// lazy/hot-path fetch-on-miss, no owner/UAT judgment on the event path, and no
// singleflight/cooldown machinery.
//
// A key enters the cache exactly one way: fetchAndSet, called by the bus from
// handleHello when an ENCRYPTED refined consumer registers. It resolves the
// owner identity (owner==current gate for a user subscription; bot needs no
// UAT), calls GetEncryptKey once, and registers the result. It leaves the
// cache via Remove (lifecycle deleted_v1) or bus exit.
//
// SECURITY: the fetched encrypt_key lives ONLY inside the SDK
// StaticEncryptKeyProvider's in-memory map. It is NEVER logged, put on the IPC
// wire, placed in status, or returned anywhere except back to the SDK
// dispatcher that asked for it. The UAT used to fetch it is likewise never
// logged, and owner != current NEVER loads a historical owner's UAT.
type encryptKeyProvider struct {
	// static is the SDK's own concurrency-safe cache and the authoritative
	// key store — also the object handed to the dispatcher as its
	// EncryptKeyProvider. fetchAndSet Set()s a freshly fetched key here;
	// Remove()s on lifecycle deletion; the provider never reads keys back out.
	static *larkevent.StaticEncryptKeyProvider

	// gate supplies resolveCurrent + resolveUAT for a USER subscription's
	// owner==current check and fresh-UAT mint. nil until SetIdentityProviders —
	// a user subscription then cannot be served (fetchAndSet returns an error →
	// Hello rejected), but a bot subscription (no UAT needed) still can.
	gate *identityGate

	// newClient builds a GetEncryptKey-capable client bound to a resolved
	// identity. nil until SetSubscriptionClient — a nil newClient means no
	// fetch can happen (fetchAndSet returns an error). Guarded by its own
	// RWMutex only because SetSubscriptionClient may run slightly before Run()
	// while a test drives fetchAndSet; in production it is
	// set-once-before-Run.
	newClientMu sync.RWMutex
	newClient   func(as core.Identity, uat string) (encryptKeyClient, error)

	fetchTimeout time.Duration
}

// newEncryptKeyProvider builds a provider with an empty SDK cache and the
// default Hello-time fetch timeout. identityGate/newClient are injected
// post-construction by bus.go's SetIdentityProviders/SetSubscriptionClient
// (same "optional, wire before Run()" convention as the lifecycle action).
func newEncryptKeyProvider() *encryptKeyProvider {
	return &encryptKeyProvider{
		static:       larkevent.NewStaticEncryptKeyProvider(nil),
		fetchTimeout: defaultEncryptKeyFetchTimeout,
	}
}

func (p *encryptKeyProvider) setIdentityGate(g *identityGate) { p.gate = g }

func (p *encryptKeyProvider) setNewClient(fn func(as core.Identity, uat string) (encryptKeyClient, error)) {
	p.newClientMu.Lock()
	p.newClient = fn
	p.newClientMu.Unlock()
}

// dispatcherProvider is the EncryptKeyProvider the SDK dispatcher is wired to:
// the plain static cache. Its EncryptKey is a pure lookup — a miss returns
// ("", false) → SDK fail-closed, never a runtime remote fetch.
func (p *encryptKeyProvider) dispatcherProvider() larkevent.EncryptKeyProvider {
	return p.static
}

// Remove drops a subscription's cached key (lifecycle removal). Idempotent.
func (p *encryptKeyProvider) Remove(subID string) {
	if subID == "" {
		return
	}
	p.static.Remove(subID)
}

// fetchAndSet fetches subID's encrypt_key ONCE (at Hello time) under the
// owner==current identity gate and registers it in the SDK static cache, so
// the dispatcher decrypts later events for this subscription with zero
// network. owner is the just-built consumer whose owner identity was fixed
// from its HelloV2 — bot (OwnerUserOpenID()=="") fetches as core.AsBot with no
// UAT and no gate; a user owner must pass owner==current, then a FRESH,
// open_id-verified UAT is minted for the current identity (NEVER a historical
// owner's UAT).
//
// Returns nil only once a usable key is cached. Any error (owner mismatch /
// missing gate|client / GetEncryptKey failure / empty key) means the caller
// (handleHello) must REJECT the Hello so the consumer never registers. The
// returned error NEVER carries the key: on success the owner's decrypt_state is
// marked decrypted; the SDK error is deliberately not surfaced verbatim to the
// consumer (no oracle) — handleHello logs only a classified reason.
func (p *encryptKeyProvider) fetchAndSet(ctx context.Context, subID string, owner *Conn) error {
	if subID == "" {
		return errEncryptKeyNoSubID
	}
	if owner == nil {
		return errEncryptKeyNoOwner
	}

	identity, uat, err := p.resolveOwnerIdentity(ctx, owner)
	if err != nil {
		return err
	}

	p.newClientMu.RLock()
	newClient := p.newClient
	p.newClientMu.RUnlock()
	if newClient == nil {
		return errEncryptKeyNoClient
	}
	cli, err := newClient(identity, uat)
	if err != nil {
		return err
	}

	fctx, cancel := context.WithTimeout(ctx, p.fetchTimeout)
	defer cancel()
	req := larkeventv1.NewGetEncryptKeySubscriptionReqBuilder().SubscriptionId(subID).Build()
	resp, err := cli.GetEncryptKey(fctx, req)
	if err != nil {
		return err
	}

	key := ""
	if resp != nil && resp.Data != nil && resp.Data.EncryptKey != nil {
		key = *resp.Data.EncryptKey
	}
	if key == "" {
		return errEncryptKeyEmpty
	}

	// Backfill the SDK cache; mark the owner healthy. NEVER log key.
	p.static.Set(subID, key)
	owner.SetDecryptState(decryptStateDecrypted)
	return nil
}

// resolveOwnerIdentity turns owner's fixed registration identity into the
// (identity, uat) GetEncryptKey must run as — the identity must match the
// Subscription's own authority:
//   - bot/legacy owner (OwnerUserOpenID()=="") -> core.AsBot, no UAT, no gate
//     (bot consumers are NEVER identity-gated — the same precedent as
//     onConnReady/eligibleConns).
//   - user owner -> owner==current gate (reuse ownerMatchesCurrent /
//     resolveCurrent). owner != current returns errEncryptKeyOwnerMismatch
//     WITHOUT loading ANY UAT (never a historical owner's UAT). Otherwise a
//     FRESH, open_id-verified UAT is minted for the current identity.
func (p *encryptKeyProvider) resolveOwnerIdentity(ctx context.Context, owner *Conn) (core.Identity, string, error) {
	if owner.OwnerUserOpenID() == "" {
		return core.AsBot, "", nil
	}
	if p.gate == nil {
		return "", "", errEncryptKeyNoGate
	}
	cur, err := p.gate.resolveCurrent()
	if err != nil {
		return "", "", err
	}
	if !ownerMatchesCurrent(owner.OwnerAppID(), owner.OwnerUserOpenID(), cur) {
		return "", "", errEncryptKeyOwnerMismatch
	}
	uat, err := p.gate.resolveUAT(ctx, cur.appID, cur.userOpenID)
	if err != nil {
		return "", "", err
	}
	return core.AsUser, uat, nil
}

// encryptKeyProvider fetch sentinels. Each is deliberately key-free and safe to
// log/classify. errEncryptKeyOwnerMismatch uses the same identity boundary as
// the delivery/lifecycle gates: owner != current never fetches and never loads
// a historical UAT.
var (
	errEncryptKeyNoSubID       = errors.New("encrypt-key: empty remote_subscription_id")
	errEncryptKeyNoOwner       = errors.New("encrypt-key: no owner consumer")
	errEncryptKeyNoGate        = errors.New("encrypt-key: no identity gate configured for a user subscription")
	errEncryptKeyOwnerMismatch = errors.New("encrypt-key: subscription owner does not match current identity")
	errEncryptKeyNoClient      = errors.New("encrypt-key: no subscription client configured")
	errEncryptKeyEmpty         = errors.New("encrypt-key: GetEncryptKey returned no key")
)

// encryptKeyFailureClass maps a fetchAndSet error to a short, key-free
// classification safe to write to bus.log. It NEVER echoes a raw SDK error
// (which could carry upstream detail) — a genuine permission failure (the #1
// operational cause: missing event:encrypt_key:read) is called out
// specifically so ops can act, everything else collapses to a generic token.
func encryptKeyFailureClass(err error) string {
	switch {
	case errors.Is(err, errEncryptKeyOwnerMismatch):
		return "owner_mismatch"
	case errors.Is(err, errEncryptKeyNoGate):
		return "no_identity_gate"
	case errors.Is(err, errEncryptKeyNoClient):
		return "no_subscription_client"
	case errors.Is(err, errEncryptKeyEmpty):
		return "no_key_returned"
	case errs.IsPermission(err):
		return "missing_scopes"
	default:
		return "unavailable"
	}
}

// resourceDataStatus maps a consumer's decrypt_state to the status display's
// resource_data rollup: "decrypted" when a usable key is
// available, "unavailable" when the key is missing or decryption is failing,
// and "" for a plaintext subscription (no resource data at all).
func resourceDataStatus(decryptState string) string {
	switch decryptState {
	case decryptStateDecrypted:
		return "decrypted"
	case decryptStateKeyUnavailable, decryptStateFailed:
		return "unavailable"
	default:
		return ""
	}
}

// formatDecryptErrorTime renders a decrypt-error timestamp for status
// (RFC3339, UTC); "" for the zero time. Never carries anything sensitive.
func formatDecryptErrorTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}
