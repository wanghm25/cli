// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package model

// RemoteAuthority is the projected authority of a remote Lark Subscription (the
// SDK's service/event/v1 Authority), carried as a value object instead of the
// SDK type so no larkeventv1.* spelling leaks into domain-facing contracts.
//
// Type is the authority tier as the platform reports it — "user", "app", or
// any future open-vocabulary value; the CLI never builds a closed enum for it.
// OpenID is set for a user authority; AppID is set for an app authority.
type RemoteAuthority struct {
	Type   string
	OpenID string
	AppID  string
}

// String renders the authority in the compact, normalized vocabulary the event
// subsystem uses everywhere a subscription authority is displayed or matched:
// "user:<open_id>" for a user (bare "user" when no open_id is known), "app" for
// an app, "" for an unset authority, and any other Type passed through verbatim
// (an open vocabulary is never dropped). This is the single canonical spelling
// shared by the `event subscription` row Identity and the lifecycle event
// Authority, so the two can never drift.
func (a RemoteAuthority) String() string {
	switch a.Type {
	case "":
		return ""
	case "user":
		if a.OpenID != "" {
			return "user:" + a.OpenID
		}
		return "user"
	case "app":
		return "app"
	default:
		return a.Type
	}
}

// AsToken renders the authority as the exact `--as` flag token so an agent can
// copy it straight back into `--as <token>`: "bot" for an app authority, "user"
// for a user authority, "" for an unset one, and any other (open-vocabulary)
// Type passed through verbatim. Unlike String — the compact "user:<open_id>" /
// "app" spelling used for MATCHING (AuthorityMatchesOwner) — AsToken is the
// AI-composable OUTPUT form: it never embeds the open_id (surfaced in its own
// field alongside it), and maps the remote "app" tier to the CLI's "bot" identity
// vocabulary so the value round-trips through `--as`.
func (a RemoteAuthority) AsToken() string {
	switch a.Type {
	case "":
		return ""
	case "user":
		return "user"
	case "app":
		return "bot"
	default:
		return a.Type
	}
}

// IsZero reports whether the authority is unset (no type known).
func (a RemoteAuthority) IsZero() bool {
	return a == RemoteAuthority{}
}

// AuthorityMatchesOwner reports whether authority (an already-normalized
// "user:<open_id>" / "app" string — the exact vocabulary RemoteAuthority.String
// produces) matches an owner's own fixed identity, expressed as ownerUserOpenID
// ("" for a bot or legacy owner — the same discriminator OwnerRef.UserOpenID
// uses everywhere else). This is the single canonical authority-vs-owner
// comparison, so the routing cross-check and the lifecycle update-compatibility
// check can share one spelling of "user:"+id / "app" and never drift apart.
// authority=="" is unclear and must never reach here; callers gate on that
// themselves.
func AuthorityMatchesOwner(authority, ownerUserOpenID string) bool {
	if ownerUserOpenID == "" {
		return authority == "app"
	}
	return authority == "user:"+ownerUserOpenID
}
