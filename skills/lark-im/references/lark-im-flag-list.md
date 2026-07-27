# im +flag-list

> **Prerequisite:** Read [`../lark-shared/SKILL.md`](../../lark-shared/SKILL.md) for authentication, global parameters, and security rules.

This skill maps to shortcut: `lark-cli im +flag-list`. Underlying API: `GET /open-apis/im/v1/flags`.

## Sorting Rules (Important)

The API returns data sorted by `update_time` in **ascending order**, meaning **oldest first, newest last**. When the result is incomplete, you cannot simply take the first page's items as the latest flags. Inspect this concrete command's `--help` for full-read controls, then take the last item only after the result reports complete.

## Commands

```bash
# Fetch first page (default page-size=50)
lark-cli im +flag-list --as user

# Disable auto-enrichment of message content (enabled by default)
lark-cli im +flag-list --as user --enrich-feed-thread=false
```

## Parameters

| Parameter | Default | Description |
|------|------|------|
| `--enrich-feed-thread` | true | Auto-enrich feed-layer thread entries with message content (calls `im.messages.mget`) |
| `--as user` | Required | Currently only supports user identity |

## Response Structure

The response has `data` as the main body, with fields described below:

| Field | Type | Description |
|------|------|------|
| `flag_items` | array | List of currently existing (not canceled) flags, sorted by `update_time` ascending |
| `delete_flag_items` | array | List of previously canceled flags, sorted by `update_time` ascending |
| `messages` | array | Message content inlined by the server for `(default, message)` type flags |
| `has_more` | boolean | Whether there's a next page |
| `page_token` | string | Pagination token for the next page |

Note: `(thread, feed)` / `(msg_thread, feed)` entries are automatically enriched via `mget` by the shortcut, and written to the corresponding entry's `message` field.

## Limitations

- **Auto-pagination is bounded**: `--page-all` fetches at most 20 pages by default. If the response still has `has_more=true`, the result is incomplete; increase `--page-limit` up to 1000 or resume with `page_token`. Never interpret `flag_items: []` as an authoritative zero while more pages remain. Historical `delete_flag_items` may occupy early pages and push active flags to later pages.
- **delete_flag_items are not enriched**: Message content is only fetched for active flags (`flag_items`), not canceled flags (`delete_flag_items`). If you need message content for a canceled flag, query the message separately using `+messages-mget --message-ids <item_id>`.

## Response Example (Sanitized)

```json
{
  "data": {
    "delete_flag_items": [
      {
        "create_time": "xxx",
        "flag_type": "xxx",
        "item_id": "xxx",
        "item_type": "xxx",
        "update_time": "xxx"
      }
    ],
    "flag_items": [
      {
        "create_time": "xxx",
        "flag_type": "xxx",
        "item_id": "xxx",
        "item_type": "xxx",
        "update_time": "xxx"
      }
    ],
    "has_more": false,
    "messages": [],
    "page_token": "xxx"
  }
}
```

## Permissions

- Base scope: `im:feed.flag:read`
- Additional scopes only when `--enrich-feed-thread=true` needs to fetch missing message content: `im:message.group_msg:get_as_user`, `im:message.p2p_msg:get_as_user`
