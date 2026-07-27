// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package session

import (
	"fmt"

	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/event/model"
)

// CurrentIdentity is the CLI's active identity resolved AT ACTION TIME — never
// a value cached from bus startup. The ONLY authoritative comparison key
// against a consumer's owner identity is AppID+UserOpenID; Profile is
// diagnostic (it mirrors OwnerRef.Profile) and NEVER participates in the
// owner==current comparison. UAT/tokens are never part of this type and never
// flow through it.
type CurrentIdentity struct {
	AppID      string
	UserOpenID string
	// Profile is the active profile's display name, resolved alongside the
	// identity so a caller that needs to establish an owner (e.g. building a
	// HelloV2) can reuse the same read. Advisory only.
	Profile string
}

// ReasonCurrentIdentityUnresolved is the shared identity-dimension reason used
// by every gate when the current identity itself could not be resolved — distinct
// from stale_identity (which means current WAS resolved but did not match this
// consumer's owner). A single stable token so a status display keys off one
// vocabulary regardless of which gate recorded it.
const ReasonCurrentIdentityUnresolved = "current_identity_unresolved"

// ResolveCurrentIdentity is the ONE implementation of "read the live current
// identity", replacing the copies that used to live in bus/identity.go,
// consume/refined.go, and cmd/event/status.go.
//
// LoadMultiAppConfig reads config.json fresh on every call — no in-process
// caching — so this is never a bus-startup-cached value, unlike
// Factory.Config()/ResolveAccount(). CurrentAppConfig("") + Users[0] mirrors
// how the rest of the CLI picks the active profile/user. A config that has no
// logged-in user is tolerated (UserOpenID stays ""): a bot consumer legitimately
// runs without one, and a user consumer whose current has no user simply fails
// the owner==current comparison (fail-closed stale), which is the correct
// outcome. It errors only when the config is unreadable or there is no current
// app at all.
func ResolveCurrentIdentity() (CurrentIdentity, error) {
	multi, err := core.LoadMultiAppConfig()
	if err != nil {
		return CurrentIdentity{}, err
	}
	app := multi.CurrentAppConfig("")
	if app == nil {
		return CurrentIdentity{}, fmt.Errorf("session: no current app config")
	}
	ci := CurrentIdentity{AppID: app.AppId, Profile: app.ProfileName()}
	if len(app.Users) > 0 {
		ci.UserOpenID = app.Users[0].UserOpenId
	}
	return ci, nil
}

// OwnerMatchesCurrent is the ONE owner comparison, replacing the copies that
// used to live in bus/identity.go and bus/lifecycle/event.go. It compares
// owner_app_id + owner_user_open_id, exactly — never tokens, never Profile.
func OwnerMatchesCurrent(owner model.OwnerRef, cur CurrentIdentity) bool {
	return owner.AppID == cur.AppID && owner.UserOpenID == cur.UserOpenID
}

// Admission is Gate's fail-closed decision for one consumer.
type Admission int

const (
	// AdmitDeliver: proceed — deliver events, bind, or take the remote action.
	// Returned for a bot/legacy owner (never identity-gated) and for a user
	// owner whose identity matches the resolved current identity.
	AdmitDeliver Admission = iota
	// AdmitStale: owner != current. FAIL CLOSED — no delivery, no BindUser, no
	// remote action, and NEVER load a historical owner's UAT. The caller marks
	// the consumer stale_identity. This is the core security invariant.
	AdmitStale
	// AdmitUnresolved: the current identity could not be resolved at all. FAIL
	// CLOSED — never deliver/bind under an unresolved identity. The caller marks
	// the consumer degraded with ReasonCurrentIdentityUnresolved.
	AdmitUnresolved
)

// Gate is the ONE owner/current security gate the four call sites share (hub
// Publish delivery, the bind gate, the lifecycle eligibility gate, and the
// encrypt-key fetch). It decides the fail-closed outcome; each call site
// applies its own side effects (SetStaleIdentity / SetIdentityDegraded / continue vs
// return) to that decision, because those differ by site while the POLICY must
// not.
//
// The decision, in order:
//   - a bot/legacy owner (OwnerUserOpenID == "") is NEVER identity-gated -> AdmitDeliver.
//   - the current identity could not be resolved (curErr != nil) -> AdmitUnresolved.
//   - owner matches current -> AdmitDeliver.
//   - owner does not match current -> AdmitStale.
//
// curErr is the error (if any) the caller got resolving cur; passing it in
// keeps each site's own resolution strategy (lazy-once per Publish, once per
// consumer, once per lifecycle batch) at the call site while the branch order
// stays centralized here.
func Gate(owner model.OwnerRef, cur CurrentIdentity, curErr error) Admission {
	if owner.UserOpenID == "" {
		return AdmitDeliver
	}
	if curErr != nil {
		return AdmitUnresolved
	}
	if OwnerMatchesCurrent(owner, cur) {
		return AdmitDeliver
	}
	return AdmitStale
}
