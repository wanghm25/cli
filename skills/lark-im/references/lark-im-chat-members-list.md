# im +chat-members-list

> **Prerequisite:** Read [`../lark-shared/SKILL.md`](../../lark-shared/SKILL.md) first to understand authentication, global parameters, and safety rules.

List the members of a chat. Users and bots are returned in **separate buckets** — `users[]` and `bots[]` — with per-bucket totals (`user_total` / `bot_total`). Use `--member-types` to return only one kind.

This skill maps to the shortcut: `lark-cli im +chat-members-list` (internally calls `GET /open-apis/im/v1/chats/{chat_id}/members/list`).

## Commands

```bash
# Single page (default)
lark-cli im +chat-members-list --chat-id oc_xxx

# Only users, or only bots
lark-cli im +chat-members-list --chat-id oc_xxx --member-types user
lark-cli im +chat-members-list --chat-id oc_xxx --member-types user,bot

# JSON output / preview the request
lark-cli im +chat-members-list --chat-id oc_xxx --format json
lark-cli im +chat-members-list --chat-id oc_xxx --dry-run
```

## Parameters

| Parameter | Required | Limits | Description |
|------|------|------|------|
| `--chat-id <id>` | Yes | `oc_xxx` | Target chat |
| `--member-types <strings>` | No | `user`, `bot` (comma-separated or repeated) | Member types to return. Omitted = all |
| `--member-id-type <type>` | No | `open_id` (default), `union_id`, `user_id` | ID type for `member_id` in the response |
| `--format json` | No | - | Output as JSON |
| `--dry-run` | No | - | Preview the request without executing it |

> Supports both `--as user` (default) and `--as bot`. The caller must be in the target chat, and must belong to the same tenant for internal chats.

## Output Fields

| Field | Description |
|------|------|
| `chat_id` | The queried chat ID |
| `users` | Array of user members (`member_id`, `name`, `tenant_key`, …) |
| `bots` | Array of bot members (`member_id`, `app_id`, `name`, …) |
| `user_total` / `bot_total` | Server-reported totals for each bucket |
| `truncations` | Non-empty when the server **capped a bucket** due to security config — see below |
| `has_more` / `page_token` | Paging signals from the final page fetched |

## Truncation: the result may be incomplete

The server applies a security cap to large member lists. When a bucket is capped, the response carries a `truncations[]` entry (e.g. `[{"limit": 100, "member_type": "user"}]`) **on the final page only**. The shortcut surfaces this two ways so it is never missed:

- **stderr**: `⚠️  member list truncated by server security config: user bucket capped at 100 — the list is INCOMPLETE.`
- **stdout JSON**: the `truncations` array is preserved verbatim in the output.

A truncated result is *not* fixable by paging further — it is a server-side cap. Treat `users`/`bots` as a partial list whenever `truncations` is non-empty.

## Result scope

For pagination controls, inspect this concrete command's `--help`. Exhausting pages does not bypass the server-side security cap described above; a non-empty `truncations` array still means the member list is incomplete.

## Common Errors and Troubleshooting

| Symptom | Root Cause |   | Solution |
|---------|---------|---|---------|
| `--chat-id is required` | `--chat-id` omitted |   | Provide the `oc_xxx` chat ID |
| `--member-types contains invalid value` | value other than `user`/`bot` |   | Use `user`, `bot`, or both |
| Permission denied | missing `im:chat.members:read` |   | Bot: enable the scope in the console. User: `lark-cli auth login --scope "im:chat.members:read"` |
