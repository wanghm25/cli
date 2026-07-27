# whiteboard +node-create

> **前置条件:** 先阅读 [`../../lark-shared/SKILL.md`](../../lark-shared/SKILL.md) 了解认证、全局参数和安全规则。画板节点操作默认使用 `--as user`。

向已有画板追加 OpenAPI 节点。它适合局部新增已编译好的节点, 不适合从零创作复杂图表。

## 适用场景

- 已经知道 `whiteboard-token`, 且拥有画板编辑权限。
- 已经有可追加的 OpenAPI `nodes[]`。
- 需要向已有画板追加节点, 而不是覆盖整图。

## 不适用场景

- 从零创作复杂图表, 或需要自动布局、批量排版、复杂连线计算。
- 只有 DSL / Mermaid / SVG, 还没有转换成 OpenAPI 节点。
- 只想在文档正文里插入或移动画板块, 这属于 `lark-doc`。

## 参数

| 参数 | 必填 | 说明 |
|---|---|---|
| `--whiteboard-token` | 是 | 画板 token。 |
| `--source` | 是 | JSON, 必须包含非空 `nodes` 数组。支持 `@path` 文件读取或 `-` stdin。 |
| `--idempotent-token` | 否 | 幂等 token, 最少 10 个字符。重试同一次逻辑新增时复用同一个值。 |

## 输入

`nodes[]` 必须是飞书 OpenAPI 画板节点, 不是 whiteboard-cli DSL。不要把 `{"type":"shape","shape":...}` 这类 DSL 节点直接传给本命令。

推荐先用 `npx -y @larksuite/whiteboard-cli@^0.2.13 --to openapi --format json` 生成 OpenAPI 结果, 再整理成 `{ "nodes": [...] }`。

```json
{
  "nodes": [
    {
      "id": "tmpNode",
      "type": "composite_shape",
      "x": 0,
      "y": 0,
      "width": 260,
      "height": 45,
      "text": {
        "text": "hello",
        "font_weight": "regular",
        "font_size": 14,
        "horizontal_align": "center",
        "vertical_align": "mid"
      },
      "style": {
        "border_color": "#3370ff",
        "border_width": "narrow",
        "border_style": "solid",
        "fill_color": "#e8f3ff"
      },
      "composite_shape": {
        "type": "round_rect"
      }
    }
  ]
}
```

## 示例

```bash
lark-cli whiteboard +node-create \
  --whiteboard-token <whiteboard_token> \
  --source @./nodes.json \
  --idempotent-token <10+字符唯一串> \
  --as user \
  --dry-run

lark-cli whiteboard +node-create \
  --whiteboard-token <whiteboard_token> \
  --source @./nodes.json \
  --idempotent-token <10+字符唯一串> \
  --as user
```

## 输出

JSON 输出使用 `data.ids`, 多个 id 用逗号拼接:

```json
{
  "data": {
    "ids": "o2:5"
  }
}
```

## Safety

- 写入前先用 `--dry-run` 检查 method、URL、params 和 body。
- 对手写节点尤其要先 dry-run；dry-run 只能验证请求结构, 不能证明节点语义一定可插入。
- 复杂图表继续走 `whiteboard-cli -> +update` 或 workflow 路径, 不要把 `+node-create` 当作默认创作入口。
