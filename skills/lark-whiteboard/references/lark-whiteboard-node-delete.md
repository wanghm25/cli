# whiteboard +node-delete

> **前置条件:** 先阅读 [`../../lark-shared/SKILL.md`](../../lark-shared/SKILL.md) 了解认证、全局参数和安全规则。画板节点操作默认使用 `--as user`。

按 node id 删除已有节点。这是高风险写操作, 只能删除已经确认的目标节点。

## 适用场景

- 已经知道 `whiteboard-token`, 且拥有画板编辑权限。
- 已经确认要删除的 node id。
- 需要删除已有画板中的局部节点。

## 不适用场景

- 不知道目标 node id。
- 只是想隐藏、移动或更新节点。
- 需要清空或整体替换画板。

## 定位节点

先导出 raw 节点结构:

```bash
lark-cli whiteboard +export \
  --whiteboard-token <whiteboard_token> \
  --output-type raw \
  --as user
```

从返回的 `data.nodes[].id` 读取目标 node id。不要删除从上下文猜测出来的 ambient 节点。

## 参数

| 参数 | 必填 | 说明 |
|---|---|---|
| `--whiteboard-token` | 是 | 画板 token。 |
| `--node-ids` | 是 | 要删除的 node id, 多个 id 用英文逗号分隔。 |
| `--idempotent-token` | 否 | 幂等 token, 最少 10 个字符。重试同一次逻辑删除时复用同一个值。 |
| `--yes` | 真实执行需要 | 高风险写操作确认。先 dry-run, 确认目标后再传。 |

## 示例

```bash
lark-cli whiteboard +node-delete \
  --whiteboard-token <whiteboard_token> \
  --node-ids <node_id_1>,<node_id_2> \
  --idempotent-token <10+字符唯一串> \
  --as user \
  --dry-run

lark-cli whiteboard +node-delete \
  --whiteboard-token <whiteboard_token> \
  --node-ids <node_id_1>,<node_id_2> \
  --idempotent-token <10+字符唯一串> \
  --as user \
  --yes
```

## 输出

```json
{
  "data": {
    "ids": "o2:5,o2:6",
    "count": 2
  }
}
```

## Safety

- 删除前必须用 `+export --output-type raw` 确认 node id。
- 先运行 `--dry-run`, 检查 method 是 `DELETE`, URL 是 `/nodes/batch_delete`, body 是 `{"ids":[...]}`。
- 只有确认目标节点后才传 `--yes`。
- 不要因为用户说“删掉这个”就删除最近消息里的节点；缺少 node id 时先导出 raw 或要求定位依据。
