# Refined (per-resource) Event Subscriptions

> **Prerequisite:** Read [`../SKILL.md`](../SKILL.md) first for the `event consume` essentials (commands, subprocess contract, jq usage, `--as` identity switching).
>
> **Heads-up for AI agents**: a refined EventKey (`event schema <key> --json` shows `refined_subscription:true`) is **not** consumable as-is. It must first be **materialized** with a resource selector, e.g. `im.message.example_v1/chat-id/oc_9f3b1c2d8a`, using one of the key's `key_templates[].example` values. Passing the bare base key to `event consume` or `event subscription create` is always rejected. Also unlike a legacy key, `consume`'s risk is `effective:"write"` for a refined key — it may create/reuse/reactivate a remote resource before it starts streaming — so run `--dry-run` first.

## 1. Discover: `list` / `schema` are the source of truth

| Command | Refined-specific additive output |
|---|---|
| `event list --json` | `refined_subscription:true`, `key_templates[]`, `auth_types`, `dry_run_supported:true`, `next_action` |
| `event schema <base> --json` | all of the above, plus full `key_templates[]`, `conditional_scopes`, `risk`, `subscription{payload_options, filter, dry_run}`, `next_action` |

`event schema` only accepts the bare **base** key (e.g. `im.message.example_v1`) — that is where `key_templates` lives. It does not accept an already-materialized key.

`key_templates[]` field reference:

| Field | Meaning |
|---|---|
| `template` | shape: `<base>/<path-segment>/{selector}` |
| `example` | a concrete, copy-pasteable materialized key |
| `selector_key` | the OAPI `target_resource` query key this template resolves to (e.g. `chat_id`) |
| `path_segment` | the kebab-case CLI path segment (e.g. `chat-id`) |
| `fixed_value` | only present on a fixed-value template (e.g. `owner/me` — the value segment must be literally `me`) |
| `auth_types` | identities **this specific template** accepts — can be a narrower subset than the base key's own `auth_types` |

Refined base keys are discovered with `event list` (the `REFINED` column); more base keys/templates arrive later from a metadata source with no change to any command below. The examples below use an illustrative key `im.message.example_v1` (substitute a real base key from `event list`), which has two templates:

- `im.message.example_v1/chat-id/{chat_id}` → example `im.message.example_v1/chat-id/oc_xxxx` — user + bot
- `im.message.example_v1/owner/me` → fixed value `me` — user only

## 2. Materialize → dry-run → consume/create

1. `event schema im.message.example_v1 --json` → read `key_templates`.
2. Pick one, e.g. `im.message.example_v1/chat-id/oc_9f3b1c2d8a`.
3. Preview with zero writes:
   - `event consume <materialized-key> --dry-run --as user|bot` — validates the request and prints the plan to **stderr**; **stdout stays empty**.
   - `event subscription create <materialized-key> --dry-run --json` — a richer JSON preview (`remote_before`, `planned_change`, `next_action`).
4. Drop `--dry-run` to apply the plan and stream NDJSON — the same ready-marker/exit-code contract as a legacy key.

A conflicting active subscription (different `payload_options` or server-side filter) is never silently overwritten — both `consume` and `subscription create` return a typed `failed_precondition` guiding you to inspect the existing subscription.

### Server-side filter (`--filter`) vs local projection (`--jq`)

- `--filter '<json>'` filters remotely before delivery and is available only when `schema.subscription.filter.supported` is true. It accepts inline JSON only — not stdin (`-`) or `@file`.
- Read the exact operands, operators, limits, and example from `schema.subscription.filter`; they are tied to the event type. Do not guess them.
- For a condition, use `value` with `eq` / `contains`, and `list_value` with `in`. The current schema does not support `not`.
- `--filter` is optional. `--jq` is separate: it filters or transforms events locally after delivery.
- A filter mismatch blocks automatic reuse. `consume` never changes the existing remote filter; inspect it with `subscription get`, then use `subscription update --filter ...` or `--clear-filter` when the user intends to change it.

## 3. Identity (`--as user|bot`)

- Agents should explicitly pass `--as user` or `--as bot` for predictable behavior. If `--as` is omitted, the CLI auto-resolves an identity from the current configuration.
- For `consume` and `subscription create`, the resolved identity must be allowed by the selected `key_templates[].auth_types`. A mismatch returns a typed error; the CLI never silently switches identity.
- Remote Subscriptions are identity-scoped. Use the same Identity context for later `list`, `get`, `update`, `renew`, `reactivate`, or `delete` calls. If a command cannot see the expected Subscription, verify the selected profile and identity before creating another one.

## 4. Subscription management commands

All 7 subcommands manage the persistent remote Subscription addressed by `remote_subscription_id` (from `list`/`get`/`create` output). They do not start or stop a local `event consume` process; update/delete still report any known local impact.

| Subcommand | Scope | Identity check | `--yes` required? |
|---|---|---|---|
| `list` | `event:subscription:read` | effective identity only | no |
| `get <id>` | `event:subscription:read` | effective identity only | no |
| `create <refined key>` | read **+** write (both, always) | base key's `auth_types` **and** the matched template's `auth_types` | no (additive + pre-checked for conflicts) |
| `update <id>` | read **+** write | effective identity only | only when a running local consumer is affected, or local impact cannot be verified |
| `renew <id>` | read **+** write | effective identity only | no |
| `reactivate <id>` | read **+** write | effective identity only | no |
| `delete <id>` | read **+** write | effective identity only | **yes** (exit code 10 without it) |

- Every mutating subcommand requires **both** `event:subscription:read` and `event:subscription:write` because it reads the current remote state before changing it.
- `--dry-run` on `create`/`update`/`renew`/`reactivate`/`delete` (all 5 mutating subcommands) previews `remote_before` / `planned_change` / `next_action` with zero writes (`list`/`get` are already read-only and have no `--dry-run`).
- A real `delete` (without `--dry-run`) always returns a confirmation-required error (category `confirmation`, **exit code 10**) without `--yes`. `update` returns the same confirmation signal only when it would affect a running local consumer, or when the local impact cannot be verified. Show the affected consumers/reason to the user and retry with `--yes` only after explicit confirmation.
- `list` and `get` add a `local` object when a running consumer is known to use that `remote_subscription_id`; absence means “none known”, not proof that none exists.
- `create` checks existing Subscriptions first: no match → create; active + compatible settings → reuse; active + conflicting settings → typed `failed_precondition`; suspended → inspect/reactivate; expired/deleted → create fresh.
- `update` changes only the server-side **filter**: `--filter <json>` sets or replaces it, `--clear-filter` removes it (exactly one is required). `--dry-run --json` reports `local_impact` and affected consumers. After a real update, check `event status --json` and follow `next_action` for any affected consumer. It never touches `include_resource_data` (see §6).
- `renew` extends the TTL of an active Subscription. Check `expire_time` with `list` / `get`; use `--dry-run` before renewing.
- `reactivate` is for a suspended Subscription. An expired or deleted Subscription must be created again instead.
- `delete` removing the remote Subscription is explicitly **not** a substitute for stopping a local `event consume` process still bound to it — see the stop chain below.

When a local consumer needs attention, run `event status --json` and follow its structured `next_action`.

## 5. The stop chain (local vs. remote — do not conflate)

Two independent things can each be "stopped"; stopping one never stops the other:

| To stop... | Command | Effect |
|---|---|---|
| one local streaming process | SIGTERM; for an unbounded consume, closing stdin also works | Stops only that process. Bounded runs ignore stdin EOF. Never touches the remote Subscription. |
| the shared local bus | `event stop [--app-id ...]` | Stops the app's bus. It refuses while consumers are active unless `--force`; forcing it may disconnect every consumer using that bus. |
| the remote Subscription | `event subscription delete <remote_subscription_id> --yes` | Removes the platform-side resource. Does **not** stop any local `event consume` process still bound to it — stop that separately. |

To fully tear down one refined subscription without disrupting unrelated consumers: stop that consume process with SIGTERM (or stdin close when unbounded), then delete its remote subscription after confirmation. Do not use `event stop --force` as a per-consumer stop.

## 6. Resource data (`--include-resource-data`)

- The flag is supported only by refined `consume` and `subscription create`, and only with user identity. A legacy key or bot identity is rejected before the Subscription is changed.
- It additionally requires `event:encrypt_key:read`; read the exact conditional scope from `event schema <base> --json`.
- Encryption and decryption are transparent. You don't need to care or perceive it.
- Use `--dry-run` first. If the existing Subscription has a different resource-data setting, the CLI reports a conflict instead of silently changing it.
- `subscription update` cannot enable, disable, or rotate resource data. After explicit confirmation, delete and recreate the Subscription with the desired flag.

## Gotchas

- **Bare base key rejected (R1)**: `event consume im.message.example_v1` or `event subscription create im.message.example_v1` (no template segment) always fails `invalid_argument`, pointing at `event schema im.message.example_v1 --json`. Only a materialized key (e.g. a `key_templates[].example` value) works.
- **A legacy key never accepts a path suffix**: `im.message.receive_v1/foo/bar` is not a refined-style path — legacy keys only ever match exactly.
- **Selector value URL discipline**: a selector value is percent-decoded exactly once; an unescaped `/` inside it is rejected outright (it would silently change how the key is segmented) — percent-encode a literal `/` as `%2F`.
