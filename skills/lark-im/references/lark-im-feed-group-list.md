# +feed-group-list

> Shortcut for `lark-cli im +feed-group-list`. List the caller's feed groups (tags) while preserving both the live and soft-deleted lists.

`+feed-group-list` is the only CLI surface for listing feed groups — there is no raw `feed.groups list` command. The list response carries two parallel arrays — `groups` (live) and `deleted_groups` (soft-deleted). When traversing multiple pages, the shortcut merges **both** arrays (a naive single-array pager would silently drop one list's later pages). It adds no enrichment.

## Identity

User-only. Run with `--as user`.

## Scopes

- `im:feed_group_v1:read`

## Usage

```bash
# First page
lark-cli im +feed-group-list --as user

# Within an update-time window
lark-cli im +feed-group-list --as user \
  --start-time 1767196800000 --end-time 1767200000000
```

## Flags

| Flag | Required | Description |
|---|---|---|
| `--start-time` | No | Update-time window start (Unix milliseconds as a decimal string) |
| `--end-time` | No | Update-time window end (Unix milliseconds as a decimal string) |

For pagination controls, inspect this concrete command's `--help`. The dual-list merge guarantee applies when multiple pages are fetched.

## Output

JSON keeps the raw envelope. When multiple pages are fetched, both lists are returned fully merged:

```json
{
  "groups": [
    { "group_id": "ofg_xxx", "type": "normal", "name": "Releases", "rules": { "rules": [] } }
  ],
  "deleted_groups": [
    { "group_id": "ofg_yyy", "type": "rule", "name": "Old", "rules": { "rules": [] } }
  ],
  "page_token": "",
  "has_more": false
}
```

> Page size counts live and deleted groups together, and the per-page count can be smaller still when entries are filtered — so never infer completeness from counts.

## See also

- [lark-im-feed-groups.md](lark-im-feed-groups.md) — raw `feed.groups.*` APIs, enums, and rule guidance
- [lark-im-feed-group-list-item.md](lark-im-feed-group-list-item.md) — list the feed cards inside one group
- [lark-im-feed-group-query-item.md](lark-im-feed-group-query-item.md) — look up specific feed cards by ID
