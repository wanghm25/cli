// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

// Package subscription is the single owner of the remote-Subscription
// Observe -> Plan -> Apply flow behind a refined EventKey.
//
// It splits the responsibility that used to live in internal/event/reconcile.go
// (the classify) and the create/consume Apply helpers into three collaborators
// plus a Policy:
//
//   - Observer reads remote state through the Gateway port (List/Walk) and
//     reports an Observation: the authority-narrowed match (if any) plus whether
//     the scan was Complete or Indeterminate (a paginated scan that hit the page
//     cap without a definitive answer). It never writes.
//   - Planner classifies an Observation against a request and a Policy into a
//     SubscriptionPlan (Create/Reuse/Reactivate/Update/Block/Indeterminate),
//     preserving every conflict dimension the old reconcile enforced:
//     include_resource_data, the server-side filter (fail-closed, never leaking
//     filter contents), authority match, and the encrypted-active encrypt_key
//     probe. It never writes except the read-only GetEncryptKey probe.
//   - Controller is the single remote-write path: it turns a writable plan into
//     an ApplyReceipt through the Gateway port (Create/Reactivate), generating a
//     fresh per-subscription encrypt_key for an encrypted create and
//     guaranteeing a non-empty model.RemoteSubscriptionID (an empty id is an
//     InvalidResponse).
//
// Policy encodes the differences between callers explicitly, as data on the
// Policy rather than as branches in each caller: whether an active encrypted
// match is classified now by probing its key (ManagementCreate) or reused with
// key confirmation deferred to the bus (ConsumeBootstrap), and whether a
// compatible suspended match is Blocked (ManagementCreate) or Reactivated
// (ConsumeBootstrap).
//
// The package owns the outbound Gateway port (gateway.go) and its domain specs
// (CreateSpec/PatchSpec/ListParams/SubscriptionPage), all expressed in SDK-free
// domain types (internal/event/model, the CLI Filter model). It depends on
// neither the Lark SDK's service/event/v1 nor the platform/lark adapter that
// implements the port — the dependency runs inward, adapter -> domain.
package subscription
