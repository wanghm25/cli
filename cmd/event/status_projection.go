// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"github.com/larksuite/cli/internal/core"
	"github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/protocol"
)

// This file is `event status`'s read-only projector: it turns the
// already-collected facts — per-app bus state, each consumer's per-dimension
// HealthFacts (protocol.ConsumerInfo.Health, authoritative since PR5), and the
// bounded remote supplement's result — into the AI-composable status view.
//
// It performs NO domain-state writes and issues no I/O of its own (no bus
// send, no remote call, no config write, no health.Set): every method reads the
// facts it was handed and formats them. Keeping the projection here, in one
// auditable unit, is what makes the owner boundary checkable — the projector
// literally has nothing capable of mutating domain state — and lets status
// PROJECT the authoritative facts instead of re-deriving state that could
// contradict them (the bus already classified the recovery next_action and the
// per-dimension health; status maps those, it does not recompute them).

// statusEventKeyUnavailable is the explicit marker status emits in a remote
// subscription's event_key when its event_type + target_resource cannot be
// reversed to a registered, executable EventKey — mirroring
// subscription.EventKeyUnavailable, kept as an independent literal here so the
// status path does not depend on the subscription subcommand package for a
// one-word contract token. A real EventKey is a dotted identifier and never
// collides with it.
const statusEventKeyUnavailable = "unavailable"

// remoteScope is the tri-state verification scope of a refined consumer's remote
// subscription, as determined by the bounded remote supplement. It is emitted as
// the status `scope` field so an AI never has to infer verification from an
// absent field: a genuinely-unverified consumer is explicitly "unknown", never
// silently treated as verified/healthy.
type remoteScope string

const (
	// scopeVerified: the bounded remote read returned this consumer's remote
	// subscription — its remote_state/expire_time/etc. are authoritative.
	scopeVerified remoteScope = "verified"
	// scopeMissing: a reachable, COMPLETE remote enumeration authoritatively
	// found this consumer's remote subscription absent (it is gone remotely).
	scopeMissing remoteScope = "missing"
	// scopeUnknown: the remote read was not attempted, timed out, degraded to
	// local-only, was capped, or only failed a per-id Get (not authoritative
	// absence) — verification is out of reach. NEVER conflated with verified.
	scopeUnknown remoteScope = "unknown"
)

// nextActionCommand is the structured, AI-composable recommended next command
// for a consumer that needs operator/agent attention: an agent can execute
// {Command} with {Args} directly, and {Reason} is the human-readable
// explanation shown alongside (the status text renders the same reason, so the
// structured form and the text never disagree). The zero value (all empty)
// means "no action recommended" and is omitted from JSON. Command is a full
// `lark-cli ...` invocation; Args carry the positional/flag arguments that
// complete it. Reason-only (Command=="") is used where no single CLI command
// safely performs the recovery (e.g. switching the active profile is a human
// decision), so the agent gets the explanation without a misleading command.
type nextActionCommand struct {
	Command string   `json:"command,omitempty"`
	Args    []string `json:"args,omitempty"`
	Reason  string   `json:"reason,omitempty"`
}

// ownerProfile is ONE app's recovery-command context, precomputed by runStatus
// from LoadMultiAppConfig so the stateless projector can render a consumer's
// recovery command against its OWNING profile/identity without reading config or
// session itself.
type ownerProfile struct {
	Profile    string // AppConfig.ProfileName (round-trips as a --profile value)
	UserOpenID string // AppConfig.Users[0].UserOpenId ("" when the profile has no active user)
}

// ownerContext maps a consumer's owner app_id -> its ownerProfile. An app_id
// ABSENT from the map (not found, or ambiguous — the same app_id appeared for
// more than one AppConfig) yields NO executable recovery command (reason-only).
// It is built once by runStatus (buildOwnerContext) and consumed read-only by
// the projector — the projector never loads config/session.
type ownerContext map[string]ownerProfile

// buildOwnerContext precomputes the owner map from ALL apps in the multi-app
// config (not f.Config()'s single resolved app — status spans profiles). An
// app_id that appears for more than one AppConfig is dropped as ambiguous, so a
// consumer owned by it falls to reason-only rather than an ambiguous mapping.
func buildOwnerContext(cfg *core.MultiAppConfig) ownerContext {
	if cfg == nil {
		return ownerContext{}
	}
	seen := make(map[string]int, len(cfg.Apps))
	m := make(ownerContext, len(cfg.Apps))
	for i := range cfg.Apps {
		a := &cfg.Apps[i]
		seen[a.AppId]++
		userOpenID := ""
		if len(a.Users) > 0 {
			userOpenID = a.Users[0].UserOpenId
		}
		m[a.AppId] = ownerProfile{Profile: a.ProfileName(), UserOpenID: userOpenID}
	}
	for id, n := range seen {
		if n > 1 {
			delete(m, id) // ambiguous app_id -> reason-only, never a guessed profile
		}
	}
	return m
}

// recoveryCommandContext resolves the OWNING profile + identity a consumer's
// recovery command must run as, per the owner-first rules. It NEVER falls back
// to the status-invocation profile — an unresolvable owner yields ok=false, so
// the caller emits a reason-only next action:
//   - the owner app_id must map unambiguously to a profile (else ok=false);
//   - a bot owner: profile hit -> {profile, bot};
//   - a user owner: additionally the mapped profile's active user
//     (Users[0].UserOpenId) must be non-empty AND equal the consumer's
//     OwnerUserOpenID -> {profile, user}; otherwise (no active user, or a
//     mismatch) ok=false;
//   - any other/absent owner identity: ok=false.
func recoveryCommandContext(oc ownerContext, c protocol.ConsumerInfo) (event.CommandContext, bool) {
	entry, found := oc[c.OwnerAppID]
	if !found {
		return event.CommandContext{}, false
	}
	switch core.Identity(c.OwnerIdentity) {
	case core.AsBot:
		return event.CommandContext{Profile: entry.Profile, Identity: core.AsBot}, true
	case core.AsUser:
		if entry.UserOpenID == "" || entry.UserOpenID != c.OwnerUserOpenID {
			return event.CommandContext{}, false
		}
		return event.CommandContext{Profile: entry.Profile, Identity: core.AsUser}, true
	default:
		return event.CommandContext{}, false
	}
}

// StatusProjector is the stateless, read-only projector described in the file
// header. It is a type (not loose functions) so the read-only projection is a
// single named owner the command delegates to, and so the "no domain writes"
// boundary is a property of one small unit.
type StatusProjector struct{}

// consumerScope projects a consumer's remote-verification scope from the
// already-collected supplement result. Only a refined consumer (one bound to a
// remote subscription) has a scope at all; a legacy consumer returns "" so the
// field is omitted for it. verified when the supplement filled its remote
// snapshot; missing when the app's authoritative-absent set (RemoteMissing,
// populated only by a complete remote enumeration) names its id; unknown
// otherwise — never a false verified/missing.
func (StatusProjector) consumerScope(s appStatus, c protocol.ConsumerInfo) remoteScope {
	if !c.RefinedSubscription && c.RemoteSubscriptionID == "" {
		return ""
	}
	if c.RemoteSubscription != nil {
		return scopeVerified
	}
	if s.RemoteMissing[c.RemoteSubscriptionID] {
		return scopeMissing
	}
	return scopeUnknown
}

// nextAction projects the single most-actionable structured next command for a
// consumer, in a fixed priority that PROJECTS authoritative facts rather than
// re-deriving them:
//
//  1. The bus's own lifecycle recovery token (ConsumerInfo.NextAction, produced
//     by the lifecycle reducer: reactivate/renew/rebuild/rebind/get) — the
//     authoritative recommendation, mapped to its executable command.
//  2. Else scope=missing (the supplement authoritatively found the remote
//     subscription gone) — recreate it.
//  3. Else a FRESH owner/current identity mismatch (status's own single-authority
//     recompute) — switch back to the owning profile (reason only; no one-shot
//     command safely switches the active profile).
//
// Returns nil when nothing is recommended. decryption guidance is deliberately
// NOT folded in here — it keeps its own decrypt_advisory/decrypt_next_action
// fields — so this stays the one lifecycle/identity recovery recommendation.
func (p StatusProjector) nextAction(s appStatus, c protocol.ConsumerInfo, scope remoteScope, oc ownerContext) *nextActionCommand {
	// Resolve the consumer's OWNING profile+identity once (owner-first; never the
	// status-invocation profile). ownerOK gates whether an executable command is
	// emitted at all — an unresolvable owner yields reason-only.
	ownerCtx, ownerOK := recoveryCommandContext(oc, c)
	if a := busNextActionCommand(c.NextAction, c.RemoteSubscriptionID, c.EventKey, ownerCtx, ownerOK); a != nil {
		return a
	}
	if scope == scopeMissing {
		if cmd := createCommandFor(c.EventKey, ownerCtx, ownerOK); cmd != nil {
			cmd.Reason = "the remote subscription " + orDash(c.RemoteSubscriptionID) +
				" backing this consumer was not found remotely; recreate it, then restart the consumer"
			return cmd
		}
	}
	if match, applicable := consumerProfileMatch(s, c); applicable && !match {
		return &nextActionCommand{Reason: refinedNextAction(match, applicable)}
	}
	return nil
}

// busNextActionCommand maps the bus's frozen lifecycle next_action token to its
// executable command. The token vocabulary (reactivate/renew/rebuild/rebind/get)
// is the stable wire contract from internal/event/bus/lifecycle (NextAction*);
// matched as literals here so the read-only status path stays decoupled from the
// bus package. Returns nil for an empty/unrecognized token. rebuild maps to
// `subscription create <event_key>`; rebind has no single safe command
// (identity re-bind is a profile/auth decision) so it is reason-only.
func busNextActionCommand(token, remoteSubscriptionID, eventKey string, ownerCtx event.CommandContext, ownerOK bool) *nextActionCommand {
	switch token {
	case "reactivate":
		return recoveryCmd(ownerCtx, ownerOK, "lark-cli event subscription reactivate", remoteSubscriptionID,
			"the remote subscription is suspended; reactivate it to resume delivery")
	case "renew":
		return recoveryCmd(ownerCtx, ownerOK, "lark-cli event subscription renew", remoteSubscriptionID,
			"the remote subscription is near expiry; renew it to extend its TTL")
	case "rebuild":
		if cmd := createCommandFor(eventKey, ownerCtx, ownerOK); cmd != nil {
			cmd.Reason = "the remote subscription is gone (expired or deleted); recreate it, then restart the consumer"
			return cmd
		}
		return &nextActionCommand{Reason: "the remote subscription is gone (expired or deleted); recreate it, then restart the consumer"}
	case "rebind":
		return &nextActionCommand{Reason: "the consumer's owner identity needs rebinding; switch back to the owning profile or re-authenticate so it can bind and resume delivery"}
	case "get":
		return recoveryCmd(ownerCtx, ownerOK, "lark-cli event subscription get", remoteSubscriptionID,
			"inspect the current remote subscription state before deciding a recovery action")
	default:
		return nil
	}
}

// recoveryCmd is the OWNER-FIRST gate for a structured recovery command: it
// emits an executable {Command, Args} — the positional plus the OWNING
// --profile/--as (ownerCtx.FlagArgs) — ONLY when the owning profile+identity
// resolved (ownerOK). Otherwise it emits a reason-only action; it never falls
// back to the status-invocation profile, since running the recovery as the wrong
// app/identity would target the wrong subscription.
func recoveryCmd(ownerCtx event.CommandContext, ownerOK bool, command, positional, reason string) *nextActionCommand {
	if !ownerOK {
		return &nextActionCommand{Reason: reason + " — run it as the subscription's OWNING profile/identity (that profile could not be resolved here, so no executable command is offered)"}
	}
	return &nextActionCommand{
		Command: command,
		Args:    append([]string{positional}, ownerCtx.FlagArgs()...),
		Reason:  reason,
	}
}

// createCommandFor builds a `subscription create <event_key>` command carrying
// the OWNING --profile/--as when the owner resolved (ownerOK); nil for an empty/
// unavailable key. When ownerOK is false it returns a bare (Command-less)
// action for the caller to set Reason on — the same owner-first gate as
// recoveryCmd: no executable create against an unresolved owning profile.
func createCommandFor(eventKey string, ownerCtx event.CommandContext, ownerOK bool) *nextActionCommand {
	if eventKey == "" || eventKey == statusEventKeyUnavailable {
		return nil
	}
	if !ownerOK {
		return &nextActionCommand{}
	}
	return &nextActionCommand{
		Command: "lark-cli event subscription create",
		Args:    append([]string{eventKey}, ownerCtx.FlagArgs()...),
	}
}

// projectConsumerView builds one consumer's AI-composable JSON view: it embeds
// the raw ConsumerInfo (so every bus field flows through) and overlays the
// status-DERIVED fields — current_profile_match, scope, structured next_action,
// and the display-only advisories — computed by reading the collected facts
// only. The per-dimension Health facts flow through the embedded ConsumerInfo
// untouched (the projector surfaces them, never rewrites them).
func (p StatusProjector) projectConsumerView(s appStatus, c protocol.ConsumerInfo, oc ownerContext) consumerView {
	cv := consumerView{ConsumerInfo: c}
	if match, applicable := consumerProfileMatch(s, c); applicable {
		m := match
		cv.CurrentProfileMatch = &m
	}
	cv.Scope = p.consumerScope(s, c)
	cv.NextAction = p.nextAction(s, c, cv.Scope, oc)
	cv.RemoteDegradedAdvisory = remoteDegradedAdvisory(c)
	cv.DecryptAdvisory, cv.DecryptNextAction = decryptAdvisory(c)
	return cv
}
