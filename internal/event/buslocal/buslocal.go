// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

// Package buslocal discovers the running local event-bus consumers across all
// bus daemons. It reuses the SAME two primitives `event status` uses — no
// discovery is reinvented here:
//
//   - busdiscover: the per-AppID PID-file scan that enumerates live buses, and
//   - busctl.QueryStatus: the wire-level status query to one bus.
//
// It exists so the management-plane commands (event subscription
// list/get/update/delete) can learn REAL local-consumer facts — which local
// `event consume` process, if any, is currently bound to a given
// remote_subscription_id — without duplicating that discovery.
//
// Everything here is BEST-EFFORT: a missing, unreachable, orphaned, or wedged
// bus contributes no consumers rather than an error, so a management command is
// NEVER failed or blocked because the local bus is down. Per-bus queries fan
// out in parallel and each carries busctl's own read deadline, so one wedged
// peer cannot compound the wait across many apps — mirroring status's
// deriveStatuses.
package buslocal

import (
	"sync"

	"github.com/larksuite/cli/internal/event/busctl"
	"github.com/larksuite/cli/internal/event/busdiscover"
	"github.com/larksuite/cli/internal/event/protocol"
	"github.com/larksuite/cli/internal/event/transport"
)

// Consumer is the identifying subset of one running local consumer the
// management plane needs: which app's bus it runs under, its OS PID, the
// EventKey it consumes, and the remote_subscription_id it is bound to (empty
// for a legacy, non-refined consumer). It is a projection of
// protocol.ConsumerInfo, decoupling callers from the fuller bus wire shape.
type Consumer struct {
	AppID                string
	PID                  int
	EventKey             string
	RemoteSubscriptionID string
}

// Querier queries one bus daemon's status by AppID. The production
// implementation dials the bus over the IPC transport (busctl.QueryStatus);
// tests substitute a fake with no socket involved, mirroring status's
// fakeBusQuerier.
type Querier interface {
	QueryBusStatus(appID string) (*protocol.StatusResponse, error)
}

// transportQuerier is the production Querier: busctl.QueryStatus over the real
// IPC transport. Mirrors cmd/event/status.go's identically-named seam.
type transportQuerier struct{ tr transport.IPC }

func (q transportQuerier) QueryBusStatus(appID string) (*protocol.StatusResponse, error) {
	return busctl.QueryStatus(q.tr, appID)
}

// Default returns the production scanner + querier: busdiscover's PID-file
// scanner and a Querier that dials each bus over a fresh IPC transport — the
// same wiring runStatus builds inline.
func Default() (busdiscover.Scanner, Querier) {
	return busdiscover.Default(), transportQuerier{tr: transport.New()}
}

// Query is the production entry point: discover + fan-out over the Default
// wiring. Best-effort; see QueryConsumers. This is the single seam the
// management commands point their local-consumer lookup at.
func Query() []Consumer {
	sc, q := Default()
	return QueryConsumers(sc, q)
}

// SignalSubscriptionUpdated tells appID's bus that remoteSubscriptionID was just
// updated (an operator Patch), so the bus can proactively degrade its matching
// local consumers instead of waiting for the platform's own updated_v1 push.
// Best-effort over a fresh IPC transport (same wiring Query uses); a down or
// unreachable bus simply returns an error the management caller ignores. This is
// the single seam the management plane points its post-Patch signal at, mirroring
// Query's role for discovery.
func SignalSubscriptionUpdated(appID, remoteSubscriptionID string) error {
	return busctl.SendSubscriptionUpdated(transport.New(), appID, remoteSubscriptionID)
}

// QueryConsumers discovers every running local consumer across all live bus
// daemons: it scans for live buses (sc) and, for each, queries its status (q),
// flattening the returned consumers into []Consumer.
//
// BEST-EFFORT end to end — it never returns an error:
//   - a nil scanner/querier, or a scan error, yields no consumers;
//   - a per-bus query failure (an orphan bus with no socket, a wedged peer, a
//     decode error) simply skips that bus, never aborting the rest.
//
// Queries fan out in parallel so one wedged bus cannot compound busctl's
// per-query read deadline across many apps (as status's deriveStatuses does).
func QueryConsumers(sc busdiscover.Scanner, q Querier) []Consumer {
	if sc == nil || q == nil {
		return nil
	}
	procs, err := sc.ScanBusProcesses()
	if err != nil || len(procs) == 0 {
		return nil
	}

	// Query each discovered bus in parallel; a failed query leaves its slot nil.
	responses := make([]*protocol.StatusResponse, len(procs))
	var wg sync.WaitGroup
	for i, p := range procs {
		wg.Add(1)
		go func(i int, appID string) {
			defer wg.Done()
			if resp, err := q.QueryBusStatus(appID); err == nil {
				responses[i] = resp
			}
		}(i, p.AppID)
	}
	wg.Wait()

	var out []Consumer
	for i, p := range procs {
		resp := responses[i]
		if resp == nil {
			continue
		}
		for _, c := range resp.Consumers {
			out = append(out, Consumer{
				AppID:                p.AppID,
				PID:                  c.PID,
				EventKey:             c.EventKey,
				RemoteSubscriptionID: c.RemoteSubscriptionID,
			})
		}
	}
	return out
}
