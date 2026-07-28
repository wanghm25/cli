// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package buslocal

import (
	"errors"
	"testing"

	"github.com/larksuite/cli/internal/event/busdiscover"
	"github.com/larksuite/cli/internal/event/protocol"
)

// fakeScanner / fakeQuerier mirror cmd/event/status_orphan_test.go's
// fakeScanner / fakeBusQuerier — the same fake bus-query seam, so
// QueryConsumers is exercised with no real PID files or sockets.
type fakeScanner struct {
	procs []busdiscover.Process
	err   error
}

func (f fakeScanner) ScanBusProcesses() ([]busdiscover.Process, error) { return f.procs, f.err }

type fakeQuerier struct {
	respByAppID map[string]*protocol.StatusResponse
	errByAppID  map[string]error
}

func (f fakeQuerier) QueryBusStatus(appID string) (*protocol.StatusResponse, error) {
	if err := f.errByAppID[appID]; err != nil {
		return nil, err
	}
	if r, ok := f.respByAppID[appID]; ok {
		return r, nil
	}
	return nil, errors.New("dial failed")
}

func consumer(pid int, eventKey, remoteSubID string) protocol.ConsumerInfo {
	return protocol.ConsumerInfo{PID: pid, EventKey: eventKey, RemoteSubscriptionID: remoteSubID}
}

// TestQueryConsumers_FlattensRunningConsumers proves the happy path: every
// discovered bus is queried and its consumers are flattened, carrying each
// consumer's app, pid, event_key, and remote_subscription_id.
func TestQueryConsumers_FlattensRunningConsumers(t *testing.T) {
	sc := fakeScanner{procs: []busdiscover.Process{{AppID: "cli_a"}, {AppID: "cli_b"}}}
	q := fakeQuerier{respByAppID: map[string]*protocol.StatusResponse{
		"cli_a": protocol.NewStatusResponse(1, 0, 1, []protocol.ConsumerInfo{
			consumer(4242, "im.message.created_v1/chat-id/oc_aaa", "sub_1"),
		}),
		"cli_b": protocol.NewStatusResponse(2, 0, 1, []protocol.ConsumerInfo{
			consumer(5353, "im.message.created_v1/chat-id/oc_bbb", "sub_2"),
		}),
	}}

	got, unreachable, scanFailed := QueryConsumers(sc, q)
	if scanFailed {
		t.Error("scanFailed = true, want false when the scan succeeded")
	}
	if len(unreachable) != 0 {
		t.Errorf("unreachable = %v, want none when every discovered bus answered", unreachable)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2: %+v", len(got), got)
	}
	byID := map[string]Consumer{}
	for _, c := range got {
		byID[c.RemoteSubscriptionID] = c
	}
	if c := byID["sub_1"]; c.AppID != "cli_a" || c.PID != 4242 || c.EventKey != "im.message.created_v1/chat-id/oc_aaa" {
		t.Errorf("sub_1 consumer = %+v, want app=cli_a pid=4242 event_key=...oc_aaa", c)
	}
	if c := byID["sub_2"]; c.AppID != "cli_b" || c.PID != 5353 {
		t.Errorf("sub_2 consumer = %+v, want app=cli_b pid=5353", c)
	}
}

// TestQueryConsumers_PerBusQueryFailure_ReportsUnreachable locks the
// fail-closed contract: a DISCOVERED bus whose query fails is reported in
// `unreachable` (never silently dropped as "no consumer"), while the reachable
// bus's consumers still surface. A write-gate caller relies on this to fail
// closed on the unqueryable bus instead of Patching a shared filter as if no
// consumer existed there.
func TestQueryConsumers_PerBusQueryFailure_ReportsUnreachable(t *testing.T) {
	sc := fakeScanner{procs: []busdiscover.Process{{AppID: "cli_up"}, {AppID: "cli_down"}}}
	q := fakeQuerier{
		respByAppID: map[string]*protocol.StatusResponse{
			"cli_up": protocol.NewStatusResponse(1, 0, 1, []protocol.ConsumerInfo{
				consumer(7, "k", "sub_up"),
			}),
		},
		errByAppID: map[string]error{"cli_down": errors.New("no socket")},
	}

	got, unreachable, _ := QueryConsumers(sc, q)
	if len(got) != 1 || got[0].RemoteSubscriptionID != "sub_up" {
		t.Fatalf("got = %+v, want only the reachable bus's consumer (sub_up)", got)
	}
	if len(unreachable) != 1 || unreachable[0] != "cli_down" {
		t.Fatalf("unreachable = %v, want [cli_down] (discovered but unqueryable)", unreachable)
	}
}

// TestQueryConsumers_ScanError_ReportsScanFailed locks the fail-closed signal: a
// scanner failure means NO bus could be enumerated, so local impact is UNKNOWN
// — scanFailed is true (distinct from "no bus", which is certain). A write-gate
// caller fails closed on it; a read caller ignores it.
func TestQueryConsumers_ScanError_ReportsScanFailed(t *testing.T) {
	sc := fakeScanner{err: errors.New("ps failed")}
	got, unreachable, scanFailed := QueryConsumers(sc, fakeQuerier{})
	if got != nil || unreachable != nil {
		t.Errorf("got = %+v, unreachable = %v, want both nil on scan error", got, unreachable)
	}
	if !scanFailed {
		t.Error("scanFailed = false, want true on a scan error (impact is unknown, must fail closed)")
	}
}

// TestQueryConsumers_NoBuses_ReturnsNil: no discovered bus means no consumers,
// no uncertainty (we are CERTAIN there is nothing), and no query attempted.
func TestQueryConsumers_NoBuses_ReturnsNil(t *testing.T) {
	got, unreachable, scanFailed := QueryConsumers(fakeScanner{procs: nil}, fakeQuerier{})
	if got != nil || unreachable != nil || scanFailed {
		t.Errorf("got = %+v, unreachable = %v, scanFailed = %v; want empty + scanFailed=false when no bus is discovered", got, unreachable, scanFailed)
	}
}

// TestQueryConsumers_NilInputs_ReturnsNil guards the degenerate wiring.
func TestQueryConsumers_NilInputs_ReturnsNil(t *testing.T) {
	got, unreachable, scanFailed := QueryConsumers(nil, nil)
	if got != nil || unreachable != nil || scanFailed {
		t.Errorf("got = %+v, unreachable = %v, scanFailed = %v; want all zero for nil scanner/querier", got, unreachable, scanFailed)
	}
}
