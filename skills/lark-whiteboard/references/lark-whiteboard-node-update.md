# whiteboard +node-update

> **前置条件:** 先阅读 [`../../lark-shared/SKILL.md`](../../lark-shared/SKILL.md) 了解认证、全局参数和安全规则。画板节点操作默认使用 `--as user`。

按 node id 更新已有节点字段。CLI 输入是批量形态, 执行层会发起一次 `batch_update` OpenAPI 请求。

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
| `--idempotent-token` | 否 | 幂等 token, 最少 10 个字符；非空时作为 `client_token` 随 batch_update 请求发送。 |

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

执行时所有节点会保持在同一个请求中:

- `PUT /open-apis/board/v1/whiteboards/:whiteboard_id/nodes/batch_update`
- body 为 `{"nodes": [...]}`, 节点内的 `id` 会保留。
- `--idempotent-token` 非空时, query 参数带 `client_token=<token>`。

## 示例

```bash
lark-cli whiteboard +node-update \
  --whiteboard-token <whiteboard_token> \
  --source @./node-updates.json \
  --idempotent-token <10+字符唯一串> \
  --as user \
  --dry-run

lark-cli whiteboard +node-update \
  --whiteboard-token <whiteboard_token> \
  --source @./node-updates.json \
  --idempotent-token <10+字符唯一串> \
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

- 多节点更新前先使用 `--dry-run` 检查 batch_update method、URL、params 和 body。
- batch_update 后端不承诺跨阶段事务回滚；如服务端提示请求未完整完成, 需用 `+export --output-type raw` 读回目标节点确认状态。
- 不要在节点更新失败时自动回退到 `+update --overwrite`, 除非用户明确要求替换整个画板。
