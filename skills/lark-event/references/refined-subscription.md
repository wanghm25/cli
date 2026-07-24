# Refined (per-resource) Event Subscriptions

> **Prerequisite:** Read [`../SKILL.md`](../SKILL.md) first for the `event consume` essentials (commands, subprocess contract, jq usage, `--as` identity switching).
>
> **Heads-up for AI agents**: a refined EventKey (`event schema <key> --json` shows `refined_subscription:true`) is **not** consumable as-is. It must first be **materialized** with a resource selector, e.g. `im.message.created_v1/chat-id/oc_9f3b1c2d8a`, using one of the key's `key_templates[].example` values. Passing the bare base key to `event consume` or `event subscription create` is always rejected (typed `invalid_argument`, hint points at `event schema <base> --json`). Also unlike a legacy key, `consume`'s risk is `effective:"write"` for a refined key — it may create/reuse/reactivate a remote resource before it starts streaming — so run `--dry-run` first.

## 1. Discover: `list` / `schema` are the source of truth

| Command | Refined-specific additive output |
|---|---|
| `event list --json` | `refined_subscription:true`, `key_templates[]`, `auth_types`, `dry_run_supported:true`, `next_action` |
| `event schema <base> --json` | all of the above, plus full `key_templates[]`, `conditional_scopes`, `risk{effective:"write", dry_run_recommended:true}`, `subscription{payload_options, dry_run.example}`, `next_action` |

`event schema` only accepts the bare **base** key (e.g. `im.message.created_v1`) — that is where `key_templates` lives. It does not accept an already-materialized key.

`key_templates[]` field reference:

| Field | Meaning |
|---|---|
| `template` | shape: `<base>/<path-segment>/{selector}` |
| `example` | a concrete, copy-pasteable materialized key |
| `selector_key` | the OAPI `target_resource` query key this template resolves to (e.g. `chat_id`) |
| `path_segment` | the kebab-case CLI path segment (e.g. `chat-id`) |
| `fixed_value` | only present on a fixed-value template (e.g. `owner/me` — the value segment must be literally `me`) |
| `auth_types` | identities **this specific template** accepts — can be a narrower subset than the base key's own `auth_types` |

Today the only shipped refined base key is `im.message.created_v1` (mock catalog; more base keys/templates arrive later from a metadata source with no change to any command below). It has two templates:

- `im.message.created_v1/chat-id/{chat_id}` → example `im.message.created_v1/chat-id/oc_9f3b1c2d8a` — user + bot
- `im.message.created_v1/owner/me` → fixed value `me` — user only

## 2. Materialize → dry-run → consume/create

1. `event schema im.message.created_v1 --json` → read `key_templates`.
2. Pick one, e.g. `im.message.created_v1/chat-id/oc_9f3b1c2d8a`.
3. Preview with zero writes:
   - `event consume <materialized-key> --dry-run --as user|bot|auto` — parses, resolves identity, runs `ProbeBusEligibility` (read-only local/remote-connection check) and a `List`/`Get`-only plan, then prints the plan to **stderr** and exits; **stdout stays empty**, nothing is written remotely.
   - `event subscription create <materialized-key> --dry-run --json` — the management-plane's own richer JSON preview (`remote_before`, `planned_change`, `next_action`).
4. Drop `--dry-run` to apply: `consume` creates-or-reuses-or-reactivates the remote Subscription (the *only* write step in its startup chain), then starts the bus and streams NDJSON — same ready-marker/exit-code contract as a legacy key.

A conflicting active subscription (different `payload_options`) is never silently overwritten — both `consume` and `subscription create` return a typed `failed_precondition` guiding you to `subscription get` the existing one instead.

## 3. TTL and renew

Every remote Subscription carries a server TTL (`expire_time`, unix seconds — visible via `subscription get`/`list`). It is extended two ways:

- **Explicit**: `event subscription renew <remote_subscription_id> [--as ...] [--dry-run] [--json]`. TTL-only change; always reads current state first for the dry-run/impact preview; never requires `--yes`.
- **Automatic (best-effort, in the background bus)**: on `event.subscription.expiration_reminder_v1`, the bus issues a single Renew **only** if it has a matching **active local consumer** (same app/profile/authority/event_key). Success is silent; failure marks that consumer `degraded` with `next_action:"renew"`. No retry, no polling loop.
- No matching local consumer → nothing renews automatically; renew explicitly, or let it expire.
- Once actually `expired`, nothing recovers it automatically (neither Renew nor Reactivate applies to an expired Subscription) — the guidance is always to rebuild (materialize + `consume`/`create` again).

## 4. Identity gate (`--as user|bot|auto`)

- `--as auto` resolves to one identity via the normal CLI default-as chain (see `lark-shared`). For `consume`/`subscription create` on a refined key, the resolved identity must **also** fall inside the **matched template's** `auth_types` — not just the base key's — e.g. `owner/me` only accepts `user` even though `im.message.created_v1` itself allows `user`+`bot`. A mismatch is a typed `failed_precondition` naming the allowed identities; it is never a silent fallback to another identity.
- **Owner vs. current** (applies to `consume`'s background lifecycle handling only, not the management plane): the first successful `consume` for a materialized key + authority fixes an "owner" identity (`app_id` + `user_open_id`) at Hello time. Every later background action (BindUser, lifecycle recovery) re-resolves "current" identity fresh from disk and compares `owner_app_id`+`owner_user_open_id` — the access token itself is never part of the comparison.
  - Match → delivery / BindUser / lifecycle recovery proceed normally.
  - Mismatch → the consumer is flagged `stale_identity` (informational only, surfaced by `event status`): no events are delivered to it, no historical UAT is loaded, nothing remote changes. Switch back to the owning profile to resume delivery.
  - Bot consumers are never identity-gated this way (no per-user BindUser applies to them).
- The management plane (`subscription list/get/update/renew/reactivate/delete`) carries no owner/current concept at all — `--as` just resolves one effective identity per call; user and bot tokens are never mixed within a single call. Only `create` enforces the template-level check above (it is the one management command that takes an EventKey).

## 5. Lifecycle control plane (automatic — nothing to call)

The bus registers 6 typed handlers for `event.subscription.*_v1` server pushes and only *acts* when the event matches a **local active consumer**; an unmatched event just updates the in-memory summary (never a remote call):

| Event | Matched-consumer behavior | Unmatched |
|---|---|---|
| `activated_v1` | clears suspension; a user consumer additionally needs a successful BindUser before it counts as running again | summary only |
| `updated_v1` | compares the new Authority against local intent — compatible: clears any conflict flag; incompatible: `degraded (remote_subscription_conflict)`; unclear: one `Get` to reconcile | summary only |
| `suspended_v1` | `suspension_code=authority_revoked` → single automatic Reactivate (+ BindUser for a user consumer); any other/unknown code → one `Get` reconcile instead of guessing | summary only, never Reactivate |
| `expiration_reminder_v1` | single automatic Renew; failure → `degraded`, `next_action:"renew"` | summary only |
| `expired_v1` | `degraded`, `next_action:"rebuild"` — never auto-Renew/Reactivate | summary only |
| `deleted_v1` | `degraded`, `next_action:"rebuild"`; sets an in-memory TTL tombstone (~10 min) so a late/out-of-order `activated_v1`/`updated_v1` for the same id cannot resurrect it | tombstone still set |

`event status` (text or `--json`) is the read-only window into all of this: `remote_state`, `last_lifecycle_event`, `suspension_reason`, `last_action`/`last_action_error`, `degraded_reason`, `next_action`. The recovery command name is always literally `reactivate` (never "resume"/"reactive"). Every field here is advisory/informational — none of it means "this consumer is dead"; no liveness signal exists in this system.

## 6. Management plane: `event subscription ...`

All 7 subcommands operate on the platform's persistent remote Subscription, addressed by `remote_subscription_id` (from `list`/`get`/`create` output) — entirely independent of any local `event consume` process.

| Subcommand | Scope | Identity check | `--yes` required? |
|---|---|---|---|
| `list` | `event:subscription:read` | effective identity only | no |
| `get <id>` | `event:subscription:read` | effective identity only | no |
| `create <refined key>` | read **+** write (both, always) | base key's `auth_types` **and** the matched template's `auth_types` | no (additive + pre-checked for conflicts) |
| `update <id>` | read **+** write | effective identity only | **yes** (exit code 10 without it) |
| `renew <id>` | read **+** write | effective identity only | no |
| `reactivate <id>` | read **+** write | effective identity only | no |
| `delete <id>` | read **+** write | effective identity only | **yes** (exit code 10 without it) |

- Every mutating subcommand hard-requires **both** `event:subscription:read` and `event:subscription:write` even where the underlying platform call would only need `write` — the CLI always reads remote state first (idempotency / conflict / impact analysis) as a design invariant, and never offers a "skip the read" path.
- `--dry-run` on any of the 5 mutating subcommands previews `remote_before` / `planned_change` / `next_action` with zero writes (`list`/`get` are already read-only and have no `--dry-run`).
- `update` and `delete` are the only two gated by a confirmation-required error (category `confirmation`, **exit code 10**) — retry the identical command with `--yes` only after a human has confirmed. `create`/`renew`/`reactivate` never prompt for confirmation.
- `create` reconciles against existing remote state before writing anything: no match → create; active + compatible `payload_options` → idempotent reuse (same id, no duplicate); active + conflicting → typed `failed_precondition` (guides you to `get`); suspended → guides you to `reactivate` instead of creating a duplicate; expired/deleted → treated as gone, safe to create fresh.
- `delete` removing the remote Subscription is explicitly **not** a substitute for stopping a local `event consume` process still bound to it — see the stop chain below.

## 7. The stop chain (local vs. remote — do not conflate)

Two independent things can each be "stopped"; stopping one never stops the other:

| To stop... | Command | Effect |
|---|---|---|
| the local streaming process | `event stop`, or SIGTERM / closing stdin on the `consume` process | Stops NDJSON delivery to *this* process only. Never touches the remote Subscription — a refined consumer has no cleanup hook (unlike a legacy key's PreConsume/cleanup pair). |
| the remote Subscription | `event subscription delete <remote_subscription_id> --yes` | Removes the platform-side resource. Does **not** stop any local `event consume` process still bound to it — stop that separately. |

To fully tear down a refined subscription: stop the local consumer **and** delete the remote subscription (either order) — neither one implies the other.

## 8. Resource data & encryption (`--include-resource-data`)

`--include-resource-data` defaults to `false`; `false` delivers plaintext business events with no resource snapshot (unchanged behavior). Setting `true` creates an **encrypted** Subscription — there is no "plaintext resource_data" mode.

**Mechanism.** A Subscription-level `encrypt_key` encrypts the *whole* `{schema, header, event}` push envelope, not just `resource_data`. The push body is `{encrypt_info (plaintext routing, carries subscription_id), encrypt (ciphertext)}`. Decryption happens transparently inside the **bus** process at the SDK EventDispatcher, *before* routing/handlers — consumers, `jq`, and NDJSON output only ever see plaintext. `--include-resource-data=true` is supported on `event subscription create` and (on a refined EventKey) `event consume`, for **user identity only**: resource data is a user-only platform capability (a bot/app subscription with `include_resource_data=true` is rejected by the platform), so `--as bot` (or `auto` that resolves to bot) is a typed `invalid_argument` before any remote call. It is refused on `event subscription update` (see rotation below), and stays a typed `invalid_argument` on an ordinary (non-refined) `event consume` key (there is no remote Subscription for the flag to apply to).

**Key creation & source.** The CLI never accepts `--encrypt-key`. On an encrypted create it generates a fresh, high-entropy per-subscription key with the OS CSPRNG in memory and submits it **atomically** in the same Create request as `include_resource_data=true` (fail-closed: a failed create never downgrades to a plaintext subscription). After success only the `remote_subscription_id` is kept; the key is never printed, logged, persisted, or returned. `--dry-run` generates no key at all.

**Scope.** Fetching a key needs the dedicated scope `event:encrypt_key:read` — `event:subscription:read`/`write` do NOT imply it. `event schema <base> --json` discloses it under `conditional_scopes` (`event:encrypt_key:read` when `--include-resource-data` is set with `--as user`); `event subscription create --dry-run --json` lists it in `required_scopes` for an encrypted request.

**Key lifecycle in the bus.** The front-end never hands the key to the bus (IPC never carries a key). The bus fetches its own copy via `GetEncryptKey(subscription_id)` using the current gated user's UAT (resource data is user-only, so the bus never fetches a key as bot; owner≠current is never fetched, and a historical owner's UAT is never loaded), caches it in memory for the bus lifetime only (never on disk), and releases it on `deleted_v1`. A refined encrypted `consume` asks the bus to preload the key during the Hello handshake, before the bus registers the consumer or sends the successful ack that lets the front-end report `ready`: if the key is not retrievable (missing scope, foreign/mismatched identity, or the subscription is gone), the consumer does **not** report `ready` — it returns a typed `decrypt_key_unavailable` `failed_precondition` (next_action: fix scope/identity or delete+recreate), and no half-registered consumer remains. There is no user-facing `get-encrypt-key` command — key retrieval is entirely bus-internal.

**Conflict matrix (encryption dimension).** `create` resolves every row at reconcile time (it probes `GetEncryptKey`, since it has no later key gate to defer to). `consume` resolves the `include_resource_data` mismatch rows at reconcile but **defers the key-retrievability decision (the last two rows) to the bus Hello** — its front-end never calls `GetEncryptKey`; the bus's Hello-time fetch is the single authoritative, fail-closed key gate.

| Local intent | Remote state | Action |
|---|---|---|
| no resource data | `include_resource_data=false` | reuse |
| no resource data | plaintext OR encrypted resource data | conflict → human decision |
| encrypted | `include_resource_data=false` | conflict → human decision |
| encrypted | plaintext resource data | unsupported → human decision (create); consume defers to the bus Hello |
| encrypted | encrypted + key retrievable | reuse |
| encrypted | encrypted + key NOT retrievable | create: conflict → fix scope/identity; consume: reuse, then the bus Hello rejects (`decrypt_key_unavailable`) if the key is unretrievable |

**Rotation / enable / disable = delete + recreate.** `encrypt` is Create-only: it can never be added, changed, or removed afterward. `event subscription update --include-resource-data=true` is therefore refused with a typed `failed_precondition` guiding you to delete + recreate (or create a separate new subscription) after human confirmation. There is no in-place rotate/update-key.

**Observability.** `event status` shows, per encrypted consumer, `resource_data` (`decrypted`/`unavailable`), `decrypt_state` (`decrypted`/`decrypt_key_unavailable`/`decrypt_failed`), and `last_decrypt_error {class,count,time}` — advisory only. A single undecryptable event is dropped fail-closed and counted (never delivered as ciphertext, never on stdout); persistent failures mark the consumer `degraded`.

**Redaction (hard red lines).** The `encrypt_key`, App Secret, UAT, refresh token, ciphertext envelope, and any decrypted temporary plaintext never appear in command args, stdin, stdout, stderr, `bus.log`, `status`, telemetry, or error envelopes/Hints. Errors carry at most `remote_subscription_id`, a failure stage, a classification, and a request/log ID. AES-CBC has no MAC, so "decrypt success" is not an integrity proof on its own — the CLI relies on the SDK envelope parse + `subscription_id` routing + local authority/resource cross-check, and never self-relaxes those.

## Gotchas

- **Bare base key rejected (R1)**: `event consume im.message.created_v1` or `event subscription create im.message.created_v1` (no template segment) always fails `invalid_argument`, pointing at `event schema im.message.created_v1 --json`. Only a materialized key (e.g. a `key_templates[].example` value) works.
- **A legacy key never accepts a path suffix**: `im.message.receive_v1/foo/bar` is not a refined-style path — legacy keys only ever match exactly.
- **Selector value URL discipline**: a selector value is percent-decoded exactly once; an unescaped `/` inside it is rejected outright (it would silently change how the key is segmented) — percent-encode a literal `/` as `%2F`.
- **`create`'s conflict is about `payload_options`, not caller intent**: two `create` calls for the same `event_type` + `target_resource` + identity but a different `--include-resource-data` (including plaintext-vs-encrypted, or encrypted-but-key-not-retrievable) collide as `failed_precondition`, never a silent overwrite — see the encryption conflict matrix under "Resource data & encryption" above.
- **`event consume` has mixed per-key risk**: the command is conservatively registered as `write` because a refined key may create/reuse/reactivate a remote Subscription before streaming. Ordinary legacy keys remain effectively `read`; `event schema <base> --json` exposes the refined `risk` field, and `--dry-run` is recommended before consuming a refined key.
