// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package consume

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/event/busctl"
	"github.com/larksuite/cli/internal/event/protocol"
	"github.com/larksuite/cli/internal/event/transport"
)

// ProbeBusEligibility is the refined consume startup chain's first stage
// (design spec §4.2/§4.9): STRICTLY read-only — it never forks a bus and
// never writes anything remote. It runs two independent checks:
//
//   - (a) remote: reuses CheckRemoteConnections (remote_preflight.go)
//     UNCHANGED — this is the exact same single-long-connection guard
//     EnsureBus itself enforces later (spec §4.3: "one bus/WebSocket per
//     app", never widened to two). A confirmed online_instance_cnt>0 fails
//     closed: typed failed_precondition, no write. Mirroring EnsureBus's own
//     handling of this exact call, an INCONCLUSIVE check (a transport/decode
//     error, count not actually observed) fails OPEN — logged, not fatal —
//     since this is a best-effort probe, not the sole enforcement point
//     (EnsureBus re-checks before it would ever fork a new local bus).
//   - (b) local: busctl.QueryStatus(tr, appID) reads the local bus's
//     status_response, if any. An old (pre-refined) bus never sets
//     ProtocolVersion/Capabilities at all — their ABSENCE, not a mismatched
//     value, is the incompatibility signal (see
//     protocol.StatusResponse's own doc comment). Attaching a refined
//     consumer to such a bus would silently misroute or drop dual-indexed
//     events (it does not understand HelloV2 fields or
//     remote_subscription_id routing — Task 13/14), so this fails closed:
//     typed failed_precondition, Hint prompts `event stop` (retiring it so a
//     freshly forked bus carries the required capabilities). No local bus
//     reachable at all is FINE — a fresh one gets forked later
//     (StartOrConnectBus) already carrying full v2 capabilities.
//
// Deliberately NOT checked here: whether the target event_type is already
// present in RegisteredEventTypes. A brand-new bus (even a fully
// capability-advertising one) legitimately has zero registered event types
// until ITS first consumer's Hello succeeds — that is the normal,
// overwhelmingly common first-attach case, not a sign of incompatibility.
// Gating on that would reject every first-time refined consumer for a new
// event type. The only trustworthy incompatibility signal is the
// bus-generation marker (ProtocolVersion/Capabilities), not the
// currently-registered-consumer snapshot.
func ProbeBusEligibility(ctx context.Context, tr transport.IPC, appID string, apiClient APIClient, errOut io.Writer) error {
	if errOut == nil {
		errOut = os.Stderr //nolint:forbidigo // library-caller fallback, mirrors EnsureBus's own default
	}

	if apiClient != nil {
		count, err := CheckRemoteConnections(ctx, apiClient)
		if err != nil {
			fmt.Fprintf(errOut, "[event] probe_bus_eligibility: remote connection check failed: %v (proceeding)\n", err)
		} else {
			fmt.Fprintf(errOut, "[event] probe_bus_eligibility: remote connection check: online_instance_cnt=%d\n", count)
			if count > 0 {
				return errs.NewValidationError(errs.SubtypeFailedPrecondition,
					"another event bus is already connected to this app (%d remote event connection(s) detected via API); only one bus should run globally to avoid duplicate event delivery", count).
					WithHint("probe_bus_eligibility: stage=remote_connection reason=online_instance_cnt=%d; stop the owning host/process (`lark-cli event stop` only inspects LOCAL buses) or use a separate app/profile before starting a refined consumer", count)
			}
		}
	} else {
		fmt.Fprintf(errOut, "[event] probe_bus_eligibility: no API client supplied; skipping remote connection check\n")
	}

	resp, err := busctl.QueryStatus(tr, appID)
	if err != nil {
		// No local bus reachable (or one mid-shutdown) -- fine, a fresh one
		// gets forked later with full v2 capabilities.
		return nil
	}
	if !busAdvertisesRefinedCapabilities(resp) {
		return errs.NewValidationError(errs.SubtypeFailedPrecondition,
			"the running local event bus for this app predates refined-subscription support (missing protocol_version/capabilities in status_response)").
			WithHint("probe_bus_eligibility: stage=local_bus_capability reason=old_bus_missing_v2_capabilities; run `lark-cli event stop` to retire it, then retry — a freshly started bus will carry the required capabilities")
	}
	return nil
}

// busAdvertisesRefinedCapabilities reports whether resp came from a bus new
// enough to understand dual-index routing and HelloV2 — both the version
// marker AND both specific capability strings must be present; a bus that
// only partially advertises (e.g. a mid-rollout partial build) is treated
// the same as an old bus: fail closed rather than assume compatibility.
func busAdvertisesRefinedCapabilities(resp *protocol.StatusResponse) bool {
	if resp == nil || resp.ProtocolVersion == "" {
		return false
	}
	return hasCapability(resp.Capabilities, protocol.CapabilityRefinedRouting) &&
		hasCapability(resp.Capabilities, protocol.CapabilityHelloV2)
}

func hasCapability(capabilities []string, want string) bool {
	for _, c := range capabilities {
		if c == want {
			return true
		}
	}
	return false
}
