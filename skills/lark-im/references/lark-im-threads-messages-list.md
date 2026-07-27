# im +threads-messages-list

> **Prerequisite:** Read [`../lark-shared/SKILL.md`](../../lark-shared/SKILL.md) first to understand authentication, global parameters, and safety rules.

Fetch the reply message list inside a thread. When `im +chat-messages-list` returns messages that include a `thread_id` field, use this command to inspect all replies in that thread.

By default each reply also carries a `reactions` block (counts + details from `im.reactions.batch_query`) when the server has reactions for it, and `update_time` for messages that were actually edited. Pass `--no-reactions` to skip the extra round-trip. Pass `--download-resources` to additionally download message resources (image/file/audio/video/media + post-embedded, excluding stickers) into `./lark-im-resources/` and attach a `resources` block — off by default, no extra requests when omitted. See [message enrichment](lark-im-message-enrichment.md) for the full contract.

This skill maps to the shortcut: `lark-cli im +threads-messages-list` (internally calls `GET /open-apis/im/v1/messages` with `container_id_type=thread` to fetch thread messages).

## Commands

```bash
# Get thread replies (ascending by time by default, table output)
lark-cli im +threads-messages-list --thread omt_xxx

# Reverse chronological order (latest first)
lark-cli im +threads-messages-list --thread omt_xxx --order desc

# Output format options
lark-cli im +threads-messages-list --thread omt_xxx --format pretty
lark-cli im +threads-messages-list --thread omt_xxx --format table
lark-cli im +threads-messages-list --thread omt_xxx --format csv

# View as a bot
lark-cli im +threads-messages-list --thread omt_xxx --as bot

# Preview the request without executing it
lark-cli im +threads-messages-list --thread omt_xxx --dry-run
```

## Parameters

| Parameter | Required | Description |
|------|------|------|
| `--thread <id>` | Yes | Thread ID (`om_xxx` or `omt_xxx` format) |
| `--no-reactions` | No | Skip auto-fetching the `reactions` block |
| `--download-resources` | No | Download message resources (image/file/audio/video/media + post-embedded, excluding stickers) into `./lark-im-resources/` and attach a `resources` block. Off by default |
| `--order <order>` | No | Sort order: `asc` (default) / `desc` |
| `--format <fmt>` | No | Output format: `json` (default) / `pretty` / `table` / `ndjson` / `csv` |
| `--as <identity>` | No | Identity type: `user` (default) / `bot` |
| `--dry-run` | No | Print the request only, do not execute it |

## Core Constraints

### 1. Source of `thread_id`

`thread_id` (`omt_xxx` or `om_xxx`) comes from the `thread_id` field in results returned by `im +chat-messages-list` or `im +messages-search`. Do not guess a thread ID. Fetch messages first and use the returned value.

### 2. No time filtering support

Thread messages do not support `start_time` / `end_time` filtering because of Feishu API limitations. Use sort order to control ordering, and inspect this concrete command's `--help` when the task requires the complete thread.

## Usage Scenarios

### Scenario 1: Expand a thread discovered in group messages

```bash
# Step 1: Fetch group messages and find one that contains thread_id
lark-cli im +chat-messages-list --chat-id oc_xxx

# Step 2: Extract thread_id from the JSON output and fetch thread replies
lark-cli im +threads-messages-list --thread omt_xxx
```

## Resource Rendering

Thread replies are rendered into human-readable text. Image messages appear as placeholders such as `![Image](img_xxx)`; by default resource binaries are **not** downloaded.

Pass `--download-resources` to download every eligible resource (image/file/audio/video/media + post-embedded, excluding stickers) into `./lark-im-resources/` in one pass and attach a `resources` block to each reply (see [message enrichment](lark-im-message-enrichment.md#resource-auto-download---download-resources-opt-in)). Otherwise download individual resources manually through `im +messages-resources-download` (see [lark-im-messages-resources-download](lark-im-messages-resources-download.md)).

## Common Errors and Troubleshooting

| Symptom | Root Cause | Solution |
|---------|---------|---------|
| "Invalid thread ID format" | `thread_id` does not start with `om_` or `omt_` | Use a valid `om_xxx` or `omt_xxx` value |
| Empty thread result | Wrong thread_id or no replies in the thread | Confirm the thread_id came from `im +chat-messages-list` output |
| Permission denied | The user is not authorized or is not a conversation member | Make sure OAuth authorization is complete and the identity is a chat member |

## References

- [lark-im](../SKILL.md) - all message-related commands
- [lark-im-chat-messages-list](lark-im-chat-messages-list.md) - fetch conversation messages (source of `thread_id`)
- [lark-shared](../../lark-shared/SKILL.md) - authentication and global parameters
