// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

// Package lark is the Lark platform gateway for the event subsystem's remote
// Subscription management plane. It is the single owner of the SDK's
// service/event/v1 Subscription surface: it builds the larkeventv1.*Req values,
// calls the SDK through an identity-bound client, unwraps and validates the
// responses, classifies failures into typed errs.* errors, and projects the SDK
// SubscriptionDetail into the domain RemoteSubscription (and small domain specs
// back into requests).
//
// Callers hold the SubscriptionGateway interface and see domain types only —
// never a larkeventv1.* value crosses the boundary — so the `event
// subscription` commands, `event status`'s remote supplement, and the bus
// lifecycle control plane are all insulated from the SDK's exact type spelling.
//
// The package imports internal/event for the CLI Filter model and its SDK
// projection (event.FilterFromSDK / event.WalkSubscriptionPages); that keeps
// the Filter projection in its existing single home rather than duplicating it
// here.
package lark
