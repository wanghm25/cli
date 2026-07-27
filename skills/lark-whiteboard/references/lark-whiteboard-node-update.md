# whiteboard +node-update

> **前置条件:** 先阅读 [`../../lark-shared/SKILL.md`](../../lark-shared/SKILL.md) 了解认证、全局参数和安全规则。画板节点操作默认使用 `--as user`。

按 node id 更新已有节点字段。当前 CLI 输入是批量形态, 但执行层会逐节点调用单节点 update OpenAPI；等 `batch_update` 上线后, 输入格式保持不变, 只替换底层调用。

## 适用场景

- 已经知道 `whiteboard-token`, 且拥有画板编辑权限。
- 已经知道目标 node id。
- 需要局部修改文字、颜色、样式、位置等节点字段。

## 不适用场景

- 不知道目标 node id。
- 需要重绘复杂图表或重新布局。
- 想替换整个画板内容。

## 定位节点

先导出 raw 节点结构:

```bash
lark-cli whiteboard +export \
  --whiteboard-token <whiteboard_token> \
  --output-type raw \
  --as user
```

从返回的 `data.nodes[].id` 读取目标 node id, 再构造更新输入。

## 参数

| 参数 | 必填 | 说明 |
|---|---|---|
| `--whiteboard-token` | 是 | 画板 token。 |
| `--source` | 是 | JSON, 必须包含非空 `nodes` 数组, 每个 node 必须包含 `id`。支持 `@path` 文件读取或 `-` stdin。 |
| `--idempotent-token` | 否 | 为未来 `batch_update` 兼容保留, 最少 10 个字符；当前逐节点 update 调用不会发送该 token。 |

## 输入

CLI 输入保持批量形态:

```json
{
  "nodes": [
    {
      "id": "o2:5",
      "type": "composite_shape",
      "text": {
        "text": "updated",
        "font_weight": "regular",
        "font_size": 14,
        "horizontal_align": "center",
        "vertical_align": "mid"
      }
    }
  ]
}
```

执行时每个节点会拆成:

- `PUT /open-apis/board/v1/whiteboards/:whiteboard_id/nodes/:node_id`
- body 为 `{"node": <去掉 id 后的节点字段>}`

## 示例

```bash
lark-cli whiteboard +node-update \
  --whiteboard-token <whiteboard_token> \
  --source @./node-updates.json \
  --as user \
  --dry-run

lark-cli whiteboard +node-update \
  --whiteboard-token <whiteboard_token> \
  --source @./node-updates.json \
  --as user
```

## 输出

```json
{
  "data": {
    "ids": "o2:5",
    "count": 1
  }
}
```

## Safety

- 当前执行非原子: 多节点更新时, 如果后面的节点失败, 前面的节点可能已经更新成功。
- 多节点更新前先使用 `--dry-run` 检查将要发起的每个单节点请求。
- 失败信息会带 `nodes[i]` 和 node id；不要在未确认失败位置前改用整图覆盖。
- 不要在节点更新失败时自动回退到 `+update --overwrite`, 除非用户明确要求替换整个画板。
