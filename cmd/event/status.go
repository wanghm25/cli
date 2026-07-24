// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package event

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"

	larkeventv1 "github.com/larksuite/oapi-sdk-go/v3/service/event/v1"

	"github.com/larksuite/cli/internal/auth"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/core"
	eventlib "github.com/larksuite/cli/internal/event"
	"github.com/larksuite/cli/internal/event/busctl"
	"github.com/larksuite/cli/internal/event/busdiscover"
	"github.com/larksuite/cli/internal/event/protocol"
	"github.com/larksuite/cli/internal/event/transport"
	"github.com/larksuite/cli/internal/output"
)

func NewCmdStatus(f *cmdutil.Factory) *cobra.Command {
	var (
		asJSON       bool
		current      bool
		failOnOrphan bool
	)
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show event bus daemon status for all discovered apps",
		Long: `Connect to each bus daemon under the config-dir/events/ tree and show PID,
uptime, and active consumers. Use --current for only the current profile's
app. Use --json for machine-readable output. Use --fail-on-orphan to exit 2
when any orphan bus is detected (for health checks).

SCOPE: the local view (bus in-memory state) needs no scope and is always
shown. For a refined consumer, remote_state/expire_time/
include_resource_data are additionally supplemented from a live 'subscription
get' call, but ONLY as a weak, optional dependency: it requires event:subscription:read; 
a missing scope silently fall back to the local-only view — this never fails the command.

OUTPUT: current_profile_match/stale_identity are advisory/informational
only — they never mean "this consumer is dead"; no liveness signal exists
here. next_action is read-only guidance, never an action this command takes
itself.

SAFETY: strictly read-only end to end.`,
		Example: `  lark-cli event status --json
  lark-cli event status --current --json
  lark-cli event status --fail-on-orphan`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runStatus(cmd, f, current, asJSON, failOnOrphan)
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "Emit status as JSON (for AI / scripts)")
	cmd.Flags().BoolVar(&current, "current", false, "Only show status for the current profile's app")
	cmd.Flags().BoolVar(&failOnOrphan, "fail-on-orphan", false, "Exit 2 when any orphan bus is detected (default: always exit 0)")
	cmdutil.SetRisk(cmd, "read")
	return cmd
}

type busState int

const (
	stateNotRunning busState = iota
	stateRunning
	stateOrphan
)

func (s busState) String() string {
	switch s {
	case stateRunning:
		return "running"
	case stateOrphan:
		return "orphan"
	default:
		return "not_running"
	}
}

// appStatus bundles one AppID's derived status; State picks which fields are meaningful.
type appStatus struct {
	AppID     string
	State     busState
	PID       int
	UptimeSec int
	Active    int
	Consumers []protocol.ConsumerInfo

	// Local current_profile_match support.
	//
	// CurrentIdentityKnown/CurrentAppID/CurrentUserOpenID carry the FRESHLY
	// resolved current identity (core.LoadMultiAppConfig, never
	// Factory.Config()'s cache — mirrors internal/event/bus/identity.go's
	// resolveCurrentIdentity so status's notion of "current" tracks exactly
	// what the bus-side delivery gate itself would use right now) used by
	// consumerProfileMatch below. Populated by annotateCurrentIdentity ONLY
	// on the one appStatus row whose AppID matches the current profile's
	// AppID (runStatus's cfg.AppID) — every other (foreign, merely scanned)
	// appStatus is left at its zero value, so a match is never attempted for
	// an app that isn't "current" at all.
	CurrentIdentityKnown bool
	CurrentAppID         string
	CurrentUserOpenID    string
}

type busQuerier interface {
	QueryBusStatus(appID string) (*protocol.StatusResponse, error)
}

// singleAppScanner wraps a Scanner and filters to one AppID for --current queries.
type singleAppScanner struct {
	appID string
	inner busdiscover.Scanner
}

func (s singleAppScanner) ScanBusProcesses() ([]busdiscover.Process, error) {
	if s.inner == nil {
		return nil, nil
	}
	all, err := s.inner.ScanBusProcesses()
	if err != nil {
		return nil, err
	}
	out := all[:0]
	for _, p := range all {
		if p.AppID == s.appID {
			out = append(out, p)
		}
	}
	return out, nil
}

type transportQuerier struct {
	tr transport.IPC
}

func (q *transportQuerier) QueryBusStatus(appID string) (*protocol.StatusResponse, error) {
	return busctl.QueryStatus(q.tr, appID)
}

func runStatus(cmd *cobra.Command, f *cmdutil.Factory, current, asJSON, failOnOrphan bool) error {
	cfg, err := f.Config()
	if err != nil {
		return err
	}

	seeds := map[string]struct{}{}
	if current {
		seeds[cfg.AppID] = struct{}{}
	} else {
		for _, id := range discoverAppIDs() {
			seeds[id] = struct{}{}
		}
		// Always include the current profile so a first-time user sees it as not_running.
		seeds[cfg.AppID] = struct{}{}
	}
	seedList := make([]string, 0, len(seeds))
	for id := range seeds {
		seedList = append(seedList, id)
	}

	tr := transport.New()
	// --current: scope the scanner to this AppID so unrelated orphans don't surface.
	var scanner busdiscover.Scanner
	if current {
		scanner = singleAppScanner{appID: cfg.AppID, inner: busdiscover.Default()}
	} else {
		scanner = busdiscover.Default()
	}
	statuses := deriveStatuses(
		seedList,
		scanner,
		&transportQuerier{tr: tr},
		time.Now(),
	)

	// Strictly read-only.
	//
	// Local current_profile_match: always available, no scope, no network —
	// stamp the freshly-resolved current identity onto the one appStatus row
	// that is actually "current" (cfg.AppID) so writeStatusText/
	// writeStatusJSON can compute each refined consumer's
	// owner-vs-current match. Every other (foreign, merely scanned) app is
	// untouched.
	cur, curOK := loadCurrentIdentityForMatch()
	annotateCurrentIdentity(statuses, cfg.AppID, cur, curOK)

	// Remote supplement: WEAK, optional, current-app-only (see
	// resolveRemoteSupplementGetter's doc for the full precondition chain).
	// Any failed precondition silently keeps every consumer local-only —
	// this must never turn a plain `event status` into a hard failure.
	ctx := cmd.Context()
	capped := applyRefinedSupplement(ctx, statuses, cfg.AppID, func() refinedSubscriptionGetter {
		return resolveRemoteSupplementGetter(ctx, cmd, f, cfg.AppID, cur.userOpenID, time.Now())
	})
	if capped {
		fmt.Fprintln(f.IOStreams.ErrOut, "[event] warning: remote subscription list pagination was capped while supplementing status — some refined consumers may show local-only state even though a matching remote Subscription might still exist beyond the pages read")
	}

	if asJSON {
		if err := writeStatusJSON(f.IOStreams.Out, statuses); err != nil {
			return err
		}
	} else {
		writeStatusText(f.IOStreams.Out, statuses)
	}
	return exitForOrphan(statuses, failOnOrphan)
}

// deriveStatuses classifies each AppID as running/orphan/not_running from socket + process-scan inputs; scanner errors are non-fatal.
func deriveStatuses(seedAppIDs []string, sc busdiscover.Scanner, q busQuerier, now time.Time) []appStatus {
	procByAppID := map[string]busdiscover.Process{}
	if sc != nil {
		if procs, err := sc.ScanBusProcesses(); err == nil {
			for _, p := range procs {
				procByAppID[p.AppID] = p
			}
		}
	}

	ids := map[string]struct{}{}
	for _, id := range seedAppIDs {
		ids[id] = struct{}{}
	}
	for id := range procByAppID {
		ids[id] = struct{}{}
	}
	sorted := make([]string, 0, len(ids))
	for id := range ids {
		sorted = append(sorted, id)
	}
	sort.Strings(sorted)

	// Query in parallel so one wedged peer can't compound the per-op deadline across many apps.
	type probe struct {
		resp *protocol.StatusResponse
		err  error
	}
	probes := make([]probe, len(sorted))
	var wg sync.WaitGroup
	for i, appID := range sorted {
		wg.Add(1)
		go func(i int, appID string) {
			defer wg.Done()
			probes[i].resp, probes[i].err = q.QueryBusStatus(appID)
		}(i, appID)
	}
	wg.Wait()

	result := make([]appStatus, 0, len(sorted))
	for i, appID := range sorted {
		s := appStatus{AppID: appID, State: stateNotRunning}
		if probes[i].err == nil {
			resp := probes[i].resp
			s.State = stateRunning
			s.PID = resp.PID
			s.UptimeSec = resp.UptimeSec
			s.Active = resp.ActiveConns
			s.Consumers = resp.Consumers
		} else if p, ok := procByAppID[appID]; ok {
			s.State = stateOrphan
			s.PID = p.PID
			s.UptimeSec = int(now.Sub(p.StartTime).Seconds())
		}
		result = append(result, s)
	}
	return result
}

// --- current_profile_match (local, read-only) ---

// currentIdentityForMatch is the CLI-side "current" identity used to compute
// each refined consumer's current_profile_match. Mirrors
// internal/event/bus/identity.go's currentIdentity concept, kept as an
// independent (unexported) type here rather than importing the bus package
// just for two strings — cmd/event has no other reason to depend on
// internal/event/bus.
type currentIdentityForMatch struct {
	appID      string
	userOpenID string
}

// loadCurrentIdentityForMatch resolves the current identity FRESH from disk
// (core.LoadMultiAppConfig → CurrentAppConfig("") → Users[0]) — never
// Factory.Config()'s cache — exactly mirroring
// internal/event/bus/identity.go's resolveCurrentIdentity, so what `event
// status` displays as "matching" tracks exactly what the bus's own live
// delivery gate would do right now. ok=false (unreadable
// config, no current app, or no logged-in user under it) means "nothing
// resolvable to compare against" — callers must treat this as
// not-applicable, never as a synthetic mismatch.
func loadCurrentIdentityForMatch() (currentIdentityForMatch, bool) {
	multi, err := core.LoadMultiAppConfig()
	if err != nil {
		return currentIdentityForMatch{}, false
	}
	app := multi.CurrentAppConfig("")
	if app == nil || len(app.Users) == 0 {
		return currentIdentityForMatch{}, false
	}
	return currentIdentityForMatch{appID: app.AppId, userOpenID: app.Users[0].UserOpenId}, true
}

// annotateCurrentIdentity stamps cur/curOK onto the one appStatus row whose
// AppID equals curAppID (runStatus's cfg.AppID) — every other (foreign,
// merely scanned) appStatus is left at its zero value, so
// consumerProfileMatch below never attempts a match for an app that isn't
// "current" at all.
func annotateCurrentIdentity(statuses []appStatus, curAppID string, cur currentIdentityForMatch, curOK bool) {
	for i := range statuses {
		if statuses[i].AppID != curAppID {
			continue
		}
		statuses[i].CurrentIdentityKnown = curOK
		statuses[i].CurrentAppID = cur.appID
		statuses[i].CurrentUserOpenID = cur.userOpenID
	}
}

// consumerProfileMatch reports current_profile_match for one
// consumer against s's current identity (see appStatus's Current* field doc
// and loadCurrentIdentityForMatch). applicable=false — NEVER treated as a
// mismatch — when either side has nothing to compare: the consumer has no
// owner user at all (OwnerUserOpenID=="", i.e. a bot or legacy
// registration — OwnerAppID alone is populated for every consumer and
// is deliberately never used as a standalone comparison
// key), or s isn't the current app / has no resolvable current identity.
func consumerProfileMatch(s appStatus, c protocol.ConsumerInfo) (match, applicable bool) {
	if !s.CurrentIdentityKnown || c.OwnerUserOpenID == "" {
		return false, false
	}
	return c.OwnerAppID == s.CurrentAppID && c.OwnerUserOpenID == s.CurrentUserOpenID, true
}

// refinedNextAction returns the read-only advisory action to display next to
// a real owner/current mismatch (applicable && !match); "" otherwise (either
// not applicable, or matching — nothing to advise). Never describes the
// consumer as dead/inactive (no liveness signal exists, a live
// consumer can still carry a stale flag).
func refinedNextAction(match, applicable bool) string {
	if !applicable || match {
		return ""
	}
	return "read-only: owner identity no longer matches the current profile; switch back to the owning profile to resume delivery, or leave as-is (no action taken, nothing was changed)"
}

// staleIdentityAdvisory renders the stale_identity flag's advisory text
// for display. The FRESHLY-computed match/applicable (from
// consumerProfileMatch above) is authoritative for "does it currently
// match" — the bus-side StaleIdentity flag, by contrast, is only ever set,
// never cleared except on rebind, so a live
// consumer that currently matches can still carry a stale flag left over
// from an earlier evaluation. Presenting that leftover flag next to a fresh
// "current_profile_match=true" as if it were a CURRENT mismatch would be
// two textually-contradictory adjacent lines. So: only a CONFIRMED fresh
// mismatch (applicable && !match) is phrased as a current mismatch; every
// other case — a fresh match, or no fresh comparison available at all for
// this app/consumer — is phrased as an earlier-snapshot advisory instead,
// never asserting a current mismatch it cannot verify. Never describes the
// consumer as dead/inactive (advisory-only — no liveness signal
// exists).
func staleIdentityAdvisory(match, applicable bool) string {
	if applicable && !match {
		return "stale_identity (owner identity does not match the current profile; informational only — the consumer may still be actively receiving events)"
	}
	if applicable && match {
		return "stale_identity (a stale_identity flag was set by an earlier evaluation; the consumer currently matches the active profile) — informational only"
	}
	return "stale_identity (a stale_identity flag was set by an earlier evaluation; current match status could not be freshly confirmed for this app) — informational only"
}

// --- weak remote supplement (read-only) ---

// refinedSubscriptionGetter narrows *eventlib.SubscriptionClient to the two
// calls the remote supplement needs. Tests substitute a fake with no
// *lark.Client or network call involved. List is included so the supplement can
// switch from one Get per id to a paginated List scan when many distinct
// remote_subscription_ids are present.
type refinedSubscriptionGetter interface {
	Get(ctx context.Context, req *larkeventv1.GetSubscriptionReq) (*larkeventv1.GetSubscriptionResp, error)
	List(ctx context.Context, req *larkeventv1.ListSubscriptionReq) (*larkeventv1.ListSubscriptionResp, error)
}

// remoteSupplementListThreshold is supplementRefinedConsumers's dedup/List
// switchover point: at or below this many distinct remote_subscription_ids,
// one Get per id stays targeted; above it, a paginated List scan (see
// listRemoteSupplementDetails) is cheaper than that many individual Gets.
// This is still a weak, best-effort supplement — List takes no id filter, so
// it pages (bounded by eventlib.MaxSubscriptionListPages) collecting only the
// wanted ids it encounters; any wanted id not found within the pages actually
// read simply stays local-only, same as an unreachable/errored Get would.
const remoteSupplementListThreshold = 5

// requiredRemoteSupplementScopes is the single scope status's weak remote
// read depends on. Kept as an independent literal rather than
// importing cmd/event/subscription's unexported subscriptionReadScopes
// (mirrors that package's own get.go scopeRemediationHint comment about the
// identical asymmetry).
var requiredRemoteSupplementScopes = []string{"event:subscription:read"}

// getStoredToken indirects auth.GetStoredToken (mirrors
// internal/credential/credential_provider.go's identical seam) purely so
// tests can substitute a token without touching the real OS keychain;
// production never reassigns it.
var getStoredToken = auth.GetStoredToken

// remoteSupplementUAT resolves the user access token to bind into
// NewSubscriptionClient for the remote supplement WITHOUT ever refreshing it
// (never triggering a token refresh). stored must come from getStoredToken (a
// pure disk/keychain read, never network) — this function itself makes no
// calls at all, it only inspects the fields it was handed, which is also the
// no-refresh proof: there is no code path here that could refresh anything.
// ok=false for any failed precondition (nil/expired stored token, or a
// missing scope) — callers must fall back to local-only silently, never
// surface an error (this is an observe gate, not a typed failure).
func remoteSupplementUAT(stored *auth.StoredUAToken, nowMillis int64) (uat string, ok bool) {
	if stored == nil || stored.ExpiresAt <= nowMillis {
		return "", false
	}
	if missing := auth.MissingScopes(stored.Scope, requiredRemoteSupplementScopes); len(missing) > 0 {
		return "", false
	}
	return stored.AccessToken, true
}

// resolveRemoteSupplementGetter performs the Factory-touching half of the
// remote-supplement gate: it is the ONE call site where `event
// status` resolves its own identity (f.ResolveAs/CheckStrictMode/
// CheckIdentity — the pattern cmd/event/subscription/subscription.go's
// resolveEffectiveIdentity uses, minus a --as flag: status defines none, so
// flagAs is always core.AsAuto and f.ResolveAs auto-detects exactly as if
// --as had never been passed at all).
//
// core.AsUser is the only branch with a local stored-token/scope precheck:
// auth.GetStoredToken is a USER token store keyed by appID+userOpenID —
// bot/tenant tokens have no equivalent local record in this CLI
// (cmd/event/subscription/subscription.go's own resolveUATAndCheckScopes
// comment notes the identical asymmetry). core.AsBot skips straight to
// constructing the client with no token option (NewSubscriptionClient
// ignores uat for bot identity) — any real bot permission failure is caught
// later by Get's own error classification, exactly like `event subscription
// get --as bot` already behaves.
//
// Returns nil — NEVER an error — for any failed precondition: unresolvable/
// disallowed identity, no valid unrefreshed token, missing scope, or no SDK
// client available.
func resolveRemoteSupplementGetter(ctx context.Context, cmd *cobra.Command, f *cmdutil.Factory, appID, curUserOpenID string, now time.Time) refinedSubscriptionGetter {
	as := f.ResolveAs(ctx, cmd, core.AsAuto)
	if err := f.CheckStrictMode(ctx, as); err != nil {
		return nil
	}
	if err := f.CheckIdentity(as, []string{"user", "bot"}); err != nil {
		return nil
	}

	var uat string
	if as == core.AsUser {
		stored := getStoredToken(appID, curUserOpenID)
		resolvedUAT, ok := remoteSupplementUAT(stored, now.UnixMilli())
		if !ok {
			return nil
		}
		uat = resolvedUAT
	}

	sdk, err := f.LarkClient()
	if err != nil || sdk == nil {
		return nil
	}
	client, err := eventlib.NewSubscriptionClient(sdk, as, uat)
	if err != nil {
		return nil
	}
	return client
}

// hasRefinedConsumer reports whether any consumer in the slice is refined
// (RemoteSubscriptionID != "") — used to skip the remote-supplement gate
// entirely (no Factory/identity/keychain work at all) when there is nothing
// to supplement.
func hasRefinedConsumer(consumers []protocol.ConsumerInfo) bool {
	for _, c := range consumers {
		if c.RemoteSubscriptionID != "" {
			return true
		}
	}
	return false
}

// applyRefinedSupplement is the pure(ish) orchestration for status's weak
// remote read: for the ONE appStatus matching curAppID that has
// at least one refined consumer, it calls resolveGetter (assumed ALREADY
// gated by every Factory-dependent precondition) AT MOST ONCE and, if
// non-nil, fills that appStatus's refined consumers via
// supplementRefinedConsumers. Injecting resolveGetter (rather than a
// Factory/cmd) keeps this orchestration itself unit-testable with a canned
// getter-or-nil — no Factory/keychain/network involved.
//
// capped mirrors supplementRefinedConsumers's own return: true only when the
// one appStatus actually supplemented hit the List page cap before every
// wanted remote_subscription_id was accounted for. The caller (runStatus)
// logs this as an advisory — it is never proof those ids don't exist, only
// that they weren't found within the pages actually read.
func applyRefinedSupplement(ctx context.Context, statuses []appStatus, curAppID string, resolveGetter func() refinedSubscriptionGetter) (capped bool) {
	for i := range statuses {
		s := &statuses[i]
		if s.AppID != curAppID || !hasRefinedConsumer(s.Consumers) {
			continue
		}
		if getter := resolveGetter(); getter != nil {
			capped = supplementRefinedConsumers(ctx, getter, s.Consumers)
		}
		return // only one appStatus can ever match curAppID
	}
	return false
}

// supplementRefinedConsumers fills RemoteSubscription/RemoteState on every
// refined consumer (RemoteSubscriptionID != "") in consumers. getter is
// assumed ALREADY gated by every precondition — this function performs no
// gating, only the read + mapping, so a fake getter exercises it with no
// Factory/cmd/keychain/network involved. A nil getter, a remote error, or an
// empty/malformed response for one id degrades ONLY the consumer(s) sharing
// that id to local-only (unreachable -> local-only, no fail) — it never
// aborts the rest and never returns an error itself.
//
// Reads are deduped by remote_subscription_id first. Several consumers can
// share one id (e.g. two local processes bound to the same remote
// Subscription), so each distinct id is read exactly once and its result is
// fanned out to every consumer sharing it: at or below
// remoteSupplementListThreshold distinct ids via one Get per id; above it via
// a paginated List scan instead of that many individual Gets. capped is true
// only when that List scan hit the page cap before every wanted id was found
// — never set on the Get path, which has no pagination concept at all.
func supplementRefinedConsumers(ctx context.Context, getter refinedSubscriptionGetter, consumers []protocol.ConsumerInfo) (capped bool) {
	if getter == nil {
		return false
	}

	// Group consumer INDICES by remote_subscription_id: the dedup key.
	byID := map[string][]int{}
	for i := range consumers {
		id := consumers[i].RemoteSubscriptionID
		if id == "" {
			continue
		}
		byID[id] = append(byID[id], i)
	}
	if len(byID) == 0 {
		return false
	}

	var details map[string]*larkeventv1.SubscriptionDetail
	if len(byID) > remoteSupplementListThreshold {
		details, capped = listRemoteSupplementDetails(ctx, getter, byID)
	} else {
		details = getRemoteSupplementDetails(ctx, getter, byID)
	}

	for id, idxs := range byID {
		d, ok := details[id]
		if !ok {
			continue // unreachable/error/not-returned -> local-only for every consumer sharing this id
		}
		info := mapRemoteSubscriptionInfo(d)
		for _, i := range idxs {
			consumers[i].RemoteSubscription = info
			consumers[i].RemoteState = info.State
		}
	}
	return capped
}

// getRemoteSupplementDetails fetches each of wantIDs' remote Subscription
// snapshots via ONE Get per distinct id: at most len(wantIDs) calls, never one
// per consumer. An error or empty/malformed response for one id simply omits it
// from the returned map — the caller treats a missing entry as "stays
// local-only", never a failure.
func getRemoteSupplementDetails(ctx context.Context, getter refinedSubscriptionGetter, wantIDs map[string][]int) map[string]*larkeventv1.SubscriptionDetail {
	out := make(map[string]*larkeventv1.SubscriptionDetail, len(wantIDs))
	for id := range wantIDs {
		req := larkeventv1.NewGetSubscriptionReqBuilder().SubscriptionId(id).Build()
		resp, err := getter.Get(ctx, req)
		if err != nil || resp == nil || resp.Data == nil || resp.Data.Subscription == nil {
			continue
		}
		out[id] = resp.Data.Subscription
	}
	return out
}

// listRemoteSupplementDetails fetches wantIDs' remote Subscription snapshots
// by paginating List (via eventlib.WalkSubscriptionPages, bounded by
// eventlib.MaxSubscriptionListPages) — cheaper than individual Gets once
// there are more than remoteSupplementListThreshold distinct ids. List takes
// no id filter (only state/target_resource/event_type), so this pages
// through an unfiltered scan, stopping as soon as every wanted id has been
// found, a page reports has_more=false, or the cap is reached.
//
// This is still a weak, best-effort supplement, not a completeness
// guarantee: any wanted id not found within the pages actually read simply
// stays local-only in the returned map, same as an unreachable/errored Get.
// capped=true means the cap was reached before every wanted id was
// accounted for — that is NOT proof the missing ids don't exist, only that
// they weren't found within the pages read, and the caller must log it
// rather than silently degrade as if it were a confirmed negative.
func listRemoteSupplementDetails(ctx context.Context, getter refinedSubscriptionGetter, wantIDs map[string][]int) (out map[string]*larkeventv1.SubscriptionDetail, capped bool) {
	out = make(map[string]*larkeventv1.SubscriptionDetail, len(wantIDs))
	remaining := len(wantIDs)

	buildReq := func(pageToken string) *larkeventv1.ListSubscriptionReq {
		b := larkeventv1.NewListSubscriptionReqBuilder()
		if pageToken != "" {
			b = b.PageToken(pageToken)
		}
		return b.Build()
	}

	capped, err := eventlib.WalkSubscriptionPages(ctx, getter, buildReq, func(item *larkeventv1.SubscriptionDetail) bool {
		if item.SubscriptionId != nil {
			if id := *item.SubscriptionId; wantIDs[id] != nil {
				if _, already := out[id]; !already {
					out[id] = item
					remaining--
				}
			}
		}
		return remaining > 0 // stop early once every wanted id has been found
	})
	if err != nil {
		return out, false // unreachable/error -> whatever was found so far stands, rest stay local-only, same degrade as today
	}
	return out, capped
}

// mapRemoteSubscriptionInfo maps one SDK SubscriptionDetail into the
// CLI-facing wire shape — shared by both the Get and the List path so they
// build this identically (extracted from the old inline per-consumer
// mapping).
func mapRemoteSubscriptionInfo(d *larkeventv1.SubscriptionDetail) *protocol.RemoteSubscriptionInfo {
	info := &protocol.RemoteSubscriptionInfo{}
	if d.State != nil {
		info.State = *d.State
	}
	if d.ExpireTime != nil {
		info.ExpireTime = int64(*d.ExpireTime)
	}
	if d.PayloadOptions != nil && d.PayloadOptions.IncludeResourceData != nil {
		info.IncludeResourceData = *d.PayloadOptions.IncludeResourceData
	}
	if d.Suspension != nil && d.Suspension.Code != nil {
		// Captured verbatim from the response already fetched above — NOT a
		// new remote call. Feeds the degraded advisory (remoteDegradedAdvisory
		// below); this function itself does no interpretation, only mapping.
		info.SuspensionCode = *d.Suspension.Code
	}
	return info
}

// --- remote degraded advisory (read-only) ---
//
// remoteDegradedAdvisory derives a read-only degraded advisory from c's
// ALREADY-fetched remote-supplement result (c.RemoteState/
// c.RemoteSubscription, filled by supplementRefinedConsumers above).
// A fuller "refresh local degraded/error state from remote" is deliberately
// NOT implemented because the remote state string is an open vocabulary with
// no closed enum in this SDK — guessing at "which states count as unhealthy"
// would risk fabricating incorrect logic. This function is grounded to
// EXACTLY the two values that ARE documented outside that open vocabulary:
//   - "suspended": SDK service/event/v1/model.go's Suspension field doc
//     (returned only when state=suspended) and already used as a string-equality
//     check in shipped code (cmd/event/subscription/update.go's
//     applyUpdate/errUpdateSuspended).
//   - "expired": the well-known subscription expiry lifecycle state.
//
// Any OTHER remote_state — including a healthy value like "active"/
// "enabled", or any other string this open vocabulary might produce in the
// future — returns "" (no derived advisory; the raw remote_state is still
// displayed verbatim by the caller, unaffected). DISPLAY-ONLY: reads fields
// already populated by an earlier, already-gated fetch (or, per
// RemoteState's own doc, a future lifecycle push); this function
// itself makes no call, no bus/Conn write, nothing — its signature (a single
// value parameter, no ctx/getter/bus reference) makes that structurally
// impossible.
func remoteDegradedAdvisory(c protocol.ConsumerInfo) string {
	switch c.RemoteState {
	case "suspended":
		if c.RemoteSubscription != nil && c.RemoteSubscription.SuspensionCode != "" {
			return fmt.Sprintf("remote subscription suspended (suspension_code=%s)", c.RemoteSubscription.SuspensionCode)
		}
		return "remote subscription suspended"
	case "expired":
		return "remote subscription expired"
	default:
		return ""
	}
}

// humanizeDuration formats d as a coarse "N unit ago" string.
func humanizeDuration(d time.Duration) string {
	s := int(d.Seconds())
	if s < 60 {
		return fmt.Sprintf("%ds ago", s)
	}
	m := s / 60
	if m < 60 {
		return fmt.Sprintf("%dm ago", m)
	}
	h := m / 60
	if h < 24 {
		return fmt.Sprintf("%dh ago", h)
	}
	return fmt.Sprintf("%dd ago", h/24)
}

// writeRefinedSubLine prints the additive, indented sub-line(s) under one
// refined consumer's table row. Legacy (non-refined) consumers
// get no output at all here — their row stays exactly as it was before this
// change. stale_identity/degraded_reason are shown as advisory/informational
// only: this NEVER labels the consumer
// as dead/inactive, and next_action is read-only (no action is ever taken by
// this command).
func writeRefinedSubLine(out io.Writer, s appStatus, c protocol.ConsumerInfo) {
	if !c.RefinedSubscription {
		return
	}
	parts := []string{fmt.Sprintf("remote_subscription_id=%s", c.RemoteSubscriptionID)}
	if c.OwnerIdentity != "" || c.OwnerAppID != "" || c.OwnerUserOpenID != "" {
		parts = append(parts, fmt.Sprintf("owner=%s app=%s user=%s",
			orDash(c.OwnerIdentity), orDash(c.OwnerAppID), orDash(c.OwnerUserOpenID)))
	}
	match, applicable := consumerProfileMatch(s, c)
	if applicable {
		parts = append(parts, fmt.Sprintf("current_profile_match=%t", match))
	}
	if c.RemoteState != "" {
		parts = append(parts, fmt.Sprintf("remote_state=%s", c.RemoteState))
	}
	// resource_data / decrypt_state rollup, when this
	// consumer's subscription is encrypted (decrypt_state != "").
	if c.ResourceData != "" {
		parts = append(parts, fmt.Sprintf("resource_data=%s", c.ResourceData))
	}
	if c.DecryptState != "" {
		parts = append(parts, fmt.Sprintf("decrypt_state=%s", c.DecryptState))
	}
	fmt.Fprintf(out, "      %s\n", strings.Join(parts, "  "))

	if c.StaleIdentity {
		fmt.Fprintf(out, "      advisory: %s\n", staleIdentityAdvisory(match, applicable))
		if action := refinedNextAction(match, applicable); action != "" {
			fmt.Fprintf(out, "      next_action: %s\n", action)
		}
	}
	if c.DegradedReason != "" {
		fmt.Fprintf(out, "      advisory: degraded (%s) — informational only\n", c.DegradedReason)
	}
	// APPEND (never clobber) a degraded advisory
	// derived from the remote-supplement result, alongside whichever of the
	// two advisories above may already be printed.
	if advisory := remoteDegradedAdvisory(c); advisory != "" {
		fmt.Fprintf(out, "      advisory: %s — informational only (remote-supplement, read-only)\n", advisory)
	}
	// decryption advisory + read-only next_action for an
	// encrypted consumer whose resource data cannot currently be decrypted.
	if advisory, action := decryptAdvisory(c); advisory != "" {
		fmt.Fprintf(out, "      advisory: %s — informational only\n", advisory)
		if action != "" {
			fmt.Fprintf(out, "      next_action: %s\n", action)
		}
	}
}

// decryptAdvisory derives a read-only decryption advisory (+ next_action) from
// a consumer's decrypt_state/last_decrypt_error. Returns
// ("", "") for a healthy or plaintext consumer (decrypt_state "" or
// "decrypted"). DISPLAY-ONLY: reads fields already populated by the bus; never
// a key/ciphertext/plaintext, never an action this command takes itself.
func decryptAdvisory(c protocol.ConsumerInfo) (advisory, nextAction string) {
	switch c.DecryptState {
	case "decrypt_key_unavailable":
		return "resource data is unavailable to the current identity (decrypt_key_unavailable)",
			"verify this identity is --as user, owns the subscription, and holds scope event:encrypt_key:read; otherwise switch --as/profile or delete + recreate the subscription after human confirmation"
	case "decrypt_failed":
		detail := ""
		if c.LastDecryptError != nil {
			detail = fmt.Sprintf(" (last_decrypt_error: class=%s count=%d time=%s)",
				c.LastDecryptError.Class, c.LastDecryptError.Count, orDash(c.LastDecryptError.Time))
		}
		return fmt.Sprintf("resource data is unavailable%s", detail),
			"if this persists, delete + recreate the subscription after human confirmation"
	default:
		return "", ""
	}
}

// orDash renders "" as "-" for compact sub-line display.
func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func writeStatusText(out io.Writer, statuses []appStatus) {
	for i, s := range statuses {
		if i > 0 {
			fmt.Fprintln(out)
		}
		fmt.Fprintf(out, "── %s ──\n", s.AppID)
		switch s.State {
		case stateNotRunning:
			fmt.Fprintln(out, "  Bus: not running")
		case stateRunning:
			fmt.Fprintf(out, "  Bus:              running (PID %d, uptime %s)\n",
				s.PID, (time.Duration(s.UptimeSec) * time.Second).String())
			fmt.Fprintf(out, "  Active consumers: %d\n", s.Active)
			if len(s.Consumers) > 0 {
				headers := []string{"CONSUMER", "EVENT KEY", "SUB", "RECEIVED", "DROPPED"}
				rows := make([][]string, 0, len(s.Consumers))
				for _, c := range s.Consumers {
					subDisplay := "-"
					if c.SubscriptionID != "" && c.SubscriptionID != c.EventKey {
						subDisplay = strings.TrimPrefix(c.SubscriptionID, c.EventKey+":")
					}
					rows = append(rows, []string{
						fmt.Sprintf("pid=%d", c.PID),
						c.EventKey,
						subDisplay,
						fmt.Sprintf("%d", c.Received),
						fmt.Sprintf("%d", c.Dropped),
					})
				}
				widths := tableWidths(headers, rows)
				const colGap = "  "
				fmt.Fprintln(out)
				fmt.Fprint(out, "  ")
				printTableRow(out, widths, headers, colGap)
				for ci, row := range rows {
					fmt.Fprint(out, "  ")
					printTableRow(out, widths, row, colGap)
					writeRefinedSubLine(out, s, s.Consumers[ci])
				}
			}
		case stateOrphan:
			if s.PID == 0 {
				fmt.Fprintln(out, "  Bus:     orphan (PID unknown — bus.pid file unreadable)")
				fmt.Fprintln(out, "  Issue:   live bus detected but pid file is missing or corrupt")
				fmt.Fprintln(out, "  Action:  inspect ~/.lark-cli/events/<app>/bus.pid and kill manually")
				break
			}
			fmt.Fprintf(out, "  Bus:     orphan (PID %d, started %s)\n",
				s.PID, humanizeDuration(time.Duration(s.UptimeSec)*time.Second))
			fmt.Fprintln(out, "  Issue:   socket file missing — consumers cannot connect")
			fmt.Fprintf(out, "  Action:  kill %d\n", s.PID)
		}
	}
}

// consumerView is writeStatusJSON's per-consumer wire shape: it embeds
// protocol.ConsumerInfo directly (Go struct embedding flattens its JSON keys
// to the top level, so every current and future ConsumerInfo field flows
// through with ZERO per-field mapping code here) and adds three values that
// are DERIVED/computed rather than raw bus data, so they don't belong on
// ConsumerInfo itself: current_profile_match/next_action —
// computed locally by consumerProfileMatch/refinedNextAction because the bus
// does not know the querying profile — and remote_degraded_advisory,
// computed locally by remoteDegradedAdvisory from the
// consumer's own already-fetched remote_state/remote_subscription
// (refreshing local degraded/error state, scoped to the two known
// remote states this SDK documents outside an open vocabulary). Pointer
// CurrentProfileMatch (not bool) so "not applicable" (nil -> omitted) is
// distinguishable from a real "false" mismatch. remote_degraded_advisory is
// deliberately a SEPARATE key from the bus-side degraded_reason (embedded
// verbatim from ConsumerInfo) — additive/appended, never a replacement or a
// clobber of that pre-existing advisory.
type consumerView struct {
	protocol.ConsumerInfo
	CurrentProfileMatch    *bool  `json:"current_profile_match,omitempty"`
	NextAction             string `json:"next_action,omitempty"`
	RemoteDegradedAdvisory string `json:"remote_degraded_advisory,omitempty"`
	// decrypt_state/last_decrypt_error/resource_data flow
	// through automatically from the embedded ConsumerInfo; these two are the
	// locally-computed advisory + read-only next_action for a decrypt issue,
	// kept as separate keys (mirroring remote_degraded_advisory) so they never
	// clobber the identity-mismatch next_action above.
	DecryptAdvisory   string `json:"decrypt_advisory,omitempty"`
	DecryptNextAction string `json:"decrypt_next_action,omitempty"`
}

func writeStatusJSON(w io.Writer, statuses []appStatus) error {
	type jsonStatus struct {
		AppID           string         `json:"app_id"`
		Status          string         `json:"status"`
		Running         bool           `json:"running"` // backward compat
		PID             int            `json:"pid,omitempty"`
		UptimeSec       int            `json:"uptime_sec,omitempty"`
		Active          int            `json:"active_consumers,omitempty"`
		Consumers       []consumerView `json:"consumers,omitempty"`
		Issue           string         `json:"issue,omitempty"`
		SuggestedAction string         `json:"suggested_action,omitempty"`
	}
	payload := make([]jsonStatus, 0, len(statuses))
	for _, s := range statuses {
		var consumers []consumerView
		if len(s.Consumers) > 0 {
			consumers = make([]consumerView, 0, len(s.Consumers))
			for _, c := range s.Consumers {
				cv := consumerView{ConsumerInfo: c}
				if match, applicable := consumerProfileMatch(s, c); applicable {
					m := match
					cv.CurrentProfileMatch = &m
					cv.NextAction = refinedNextAction(match, applicable)
				}
				cv.RemoteDegradedAdvisory = remoteDegradedAdvisory(c)
				cv.DecryptAdvisory, cv.DecryptNextAction = decryptAdvisory(c)
				consumers = append(consumers, cv)
			}
		}
		js := jsonStatus{
			AppID:     s.AppID,
			Status:    s.State.String(),
			Running:   s.State == stateRunning,
			PID:       s.PID,
			UptimeSec: s.UptimeSec,
			Active:    s.Active,
			Consumers: consumers,
		}
		if s.State == stateOrphan {
			if s.PID == 0 {
				js.Issue = "live bus detected but pid file is missing or corrupt"
				js.SuggestedAction = "inspect events dir and kill manually"
			} else {
				js.Issue = "socket file missing"
				js.SuggestedAction = fmt.Sprintf("kill %d", s.PID)
			}
		}
		payload = append(payload, js)
	}
	output.PrintJson(w, map[string]interface{}{"apps": payload})
	return nil
}

// exitForOrphan returns ExitValidation iff failOnOrphan and any status is orphan; default exit 0 preserves observe-only semantics.
func exitForOrphan(statuses []appStatus, failOnOrphan bool) error {
	if !failOnOrphan {
		return nil
	}
	for _, s := range statuses {
		if s.State == stateOrphan {
			return output.ErrBare(output.ExitValidation)
		}
	}
	return nil
}
