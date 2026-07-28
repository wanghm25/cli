// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package subscription

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/larksuite/cli/internal/event/buslocal"
)

// localConsumerInfo is one running local consumer bound to a subscription's
// remote_subscription_id, as discovered from the local bus (buslocal). It is
// the additive, AI-composable detail surfaced by `list`/`get` (inside a row's
// `local` object) and by `update`/`delete` --dry-run (inside
// local_impact.consumers).
type localConsumerInfo struct {
	PID      int    `json:"pid"`
	EventKey string `json:"event_key,omitempty"`
	AppID    string `json:"app_id,omitempty"`
}

// localConsumerView is the `local` object attached to a list/get subscription
// row: whether a local consumer is currently running for this subscription and,
// if so, which one(s). Emitted ONLY when at least one running consumer matches;
// a down/unreachable bus, or no match, leaves the row's `local` omitted — "no
// local consumer known", never an error (best-effort).
type localConsumerView struct {
	Running   bool                `json:"running"`
	Consumers []localConsumerInfo `json:"consumers,omitempty"`
}

// queryLocalConsumers is the process-wide seam for discovering the running
// local consumers. Production points it at buslocal.Query (busdiscover +
// busctl.QueryStatus — the same mechanism `event status` uses, best-effort and
// never erroring); tests replace it with a canned set so the management
// commands' local-surfacing is exercised with no real bus.
var queryLocalConsumers = buslocal.Query

// signalSubscriptionUpdated is the process-wide seam for telling a running bus
// that a subscription was just updated, so it proactively degrades the matching
// local consumers rather than waiting for the platform's own updated_v1 push.
// Production points it at buslocal.SignalSubscriptionUpdated (busctl over the
// real IPC transport, best-effort); tests replace it with a capture so update's
// post-Patch signalling is exercised with no real bus.
var signalSubscriptionUpdated = buslocal.SignalSubscriptionUpdated

// notifyAffectedBuses fires the best-effort SubscriptionUpdated signal to the
// bus of each DISTINCT app that has a running consumer bound to
// remoteSubscriptionID — so those consumers are proactively degraded after a
// real update Patch. One signal per app (a bus degrades all its matching
// consumers from a single id); a consumer with no known app_id is skipped (it
// cannot be addressed), and every send error is ignored (best-effort — the
// platform's own updated_v1 remains the backstop, so no bus reachable still
// leaves update succeeding). Call ONLY after a Patch actually applied — never on
// a no-op or --dry-run.
func notifyAffectedBuses(remoteSubscriptionID string, affected []localConsumerInfo) {
	seen := make(map[string]bool, len(affected))
	for _, c := range affected {
		if c.AppID == "" || seen[c.AppID] {
			continue
		}
		seen[c.AppID] = true
		_ = signalSubscriptionUpdated(c.AppID, remoteSubscriptionID)
	}
}

// matchLocalConsumers returns the running local consumers bound to
// remoteSubscriptionID (the OpenAPI Subscription primary key). Matching is by
// remote_subscription_id alone — that id is globally unique, so a consumer
// under any app's bus that carries it is genuinely bound to this subscription.
// An empty id (nothing to match) yields none.
func matchLocalConsumers(consumers []buslocal.Consumer, remoteSubscriptionID string) []localConsumerInfo {
	if remoteSubscriptionID == "" {
		return nil
	}
	var out []localConsumerInfo
	for _, c := range consumers {
		if c.RemoteSubscriptionID == remoteSubscriptionID {
			out = append(out, localConsumerInfo{PID: c.PID, EventKey: c.EventKey, AppID: c.AppID})
		}
	}
	return out
}

// localViewFor builds the additive `local` annotation for a subscription row:
// the running consumer(s) bound to remoteSubscriptionID, or nil when none match
// (so the row's `local` field stays omitted — additive, best-effort).
func localViewFor(consumers []buslocal.Consumer, remoteSubscriptionID string) *localConsumerView {
	matched := matchLocalConsumers(consumers, remoteSubscriptionID)
	if len(matched) == 0 {
		return nil
	}
	return &localConsumerView{Running: true, Consumers: matched}
}

// formatLocalConsumers renders matched consumers for a text line / hint / note,
// e.g. "pid=4242 (im.message.created_v1/chat-id/oc_aaa)". Empty input yields "".
func formatLocalConsumers(consumers []localConsumerInfo) string {
	parts := make([]string, 0, len(consumers))
	for _, c := range consumers {
		if c.EventKey != "" {
			parts = append(parts, fmt.Sprintf("pid=%d (%s)", c.PID, c.EventKey))
			continue
		}
		parts = append(parts, fmt.Sprintf("pid=%d", c.PID))
	}
	return strings.Join(parts, ", ")
}

// formatLocalConsumersColumn renders the compact LOCAL column for `list` text
// output: "pid=4242" or "pid=4242,5353". Empty input yields "-".
func formatLocalConsumersColumn(view *localConsumerView) string {
	if view == nil || !view.Running || len(view.Consumers) == 0 {
		return "-"
	}
	pids := make([]string, 0, len(view.Consumers))
	for _, c := range view.Consumers {
		pids = append(pids, strconv.Itoa(c.PID))
	}
	return "pid=" + strings.Join(pids, ",")
}
