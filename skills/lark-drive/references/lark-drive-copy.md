
# drive +copy

> **前置条件：** 先阅读 [`../lark-shared/SKILL.md`](../../lark-shared/SKILL.md) 了解认证、全局参数和安全规则。

复制一个 Drive 文件（在线文档、表格、多维表格、幻灯片、思维笔记或普通文件）到目标文件夹，生成一个内容相同的新副本。推荐直接传文档 URL。

## 命令

```bash
# 推荐：直接传文档 URL（自动识别类型和 token）
lark-cli drive +copy \
  --url 'https://example.larksuite.com/docx/<DOCX_TOKEN>' \
  --name '副本名称' \
  --folder-token <TARGET_FOLDER_TOKEN>

# 目标文件夹也可以直接传文件夹 URL
lark-cli drive +copy \
  --url 'https://example.larksuite.com/sheets/<SHEET_TOKEN>' \
  --name '副本名称' \
  --folder-token 'https://example.larksuite.com/drive/folder/<FOLDER_TOKEN>'

# 裸 token 需要显式指定 --type
lark-cli drive +copy \
  --token <BITABLE_TOKEN> \
  --type bitable \
  --name '副本名称' \
  --folder-token <TARGET_FOLDER_TOKEN>

# 复制旧版 doc 的同时转换为新版 docx（--extra 透传特殊复制语义）
lark-cli drive +copy \
  --url 'https://example.larksuite.com/doc/<DOC_TOKEN>' \
  --name '副本名称' \
  --folder-token <TARGET_FOLDER_TOKEN> \
  --extra target_type=docx

# 仅预览即将发起的请求，不真正执行
lark-cli drive +copy \
  --url 'https://example.larksuite.com/docx/<DOCX_TOKEN>' \
  --name '副本名称' \
  --folder-token <TARGET_FOLDER_TOKEN> \
  --dry-run
```

## 参数

| 参数 | 必填 | 说明 |
|------|------|------|
| `--url` | 与 `--token` 二选一 | 源文档 URL，支持 `doc` / `docx` / `sheet` / `file` / `mindnote` / `slides` / `base` / `bitable` 路径 |
| `--token` | 与 `--url` 二选一 | 源文档 token 或 URL；裸 token 必须配合 `--type` |
| `--type` | 裸 token 时必填 | 源文件类型：`doc`、`docx`、`sheet`、`file`、`mindnote`、`slides`、`bitable`（`base` 为兼容别名）；传 URL 时可省略，显式传入时必须与 URL 类型一致 |
| `--name` | 是 | 副本名称，最长 256 字节 |
| `--folder-token` | 是 | 目标文件夹 token 或文件夹 URL；传租户根文件夹 token 表示复制到"我的空间"根目录 |
| `--extra` | 否 | 可重复的 `key=value` 对，原样透传给 API 的 `extra` 自定义复制参数；典型用法 `--extra target_type=docx`（复制旧版 doc 时转换为 docx 副本） |

## 输入规则

- `--url` 与 `--token` 互斥，只传一个
- `--type` 必须与源文件真实类型一致，类型不匹配时服务端会返回失败
- `base` 与 `bitable` 是同一概念，CLI 会把 `base` 归一化为 `bitable` 后发给服务端
- 目标文件夹必须是云空间（云盘/云存储）文件夹 token，不能传 wiki 节点 token

## Wiki 场景

`drive +copy` 只复制云盘（Drive）文件，不接受 wiki URL / token；传入时返回校验错误，错误 hint 会给出替代命令。知识库内复制节点用 [`lark-wiki`](../../lark-wiki/SKILL.md) 的 `wiki +node-copy`；要把 wiki 文档复制成 Drive 空间里的独立副本（脱离知识库），先用 `drive +inspect` 解包拿到底层 `token` 和 `type`，再对底层 token 执行 `drive +copy`。

## 行为说明

- 该 shortcut 继承通用能力，可配合 `--as user|bot|auto`、`--format`、`--jq`、`--dry-run` 使用
- `--dry-run` 只输出请求方法、路径、身份和请求体预览，不会真正创建副本
- 这是写入操作；执行前应确认源文档和目标文件夹准确无误

## 权限要求

- 当前调用身份需要对源文件有读写权限
- 当前调用身份需要对目标文件夹有编辑权限
- shortcut 声明的 scope 为 `docs:document:copy`

## 输出

```json
{
  "copied": true,
  "file_token": "QWr9dtp4xoFPmfxcQCnco0AYnN1",
  "file_type": "docx",
  "name": "测试文档-副本",
  "url": "https://example.larksuite.com/docx/QWr9dtp4xoFPmfxcQCnco0AYnN1",
  "source_file_token": "D1FGdkfvionNYRxg9gac9TignOh",
  "source_type": "docx",
  "folder_token": "KTsOfh8ETlum3rd9p82cj1kinId"
}
```

## 参考

- [lark-drive](../SKILL.md) -- 云空间（云盘/云存储）全部命令
- [lark-wiki](../../lark-wiki/SKILL.md) -- 知识库节点复制（`wiki +node-copy`）
- [lark-shared](../../lark-shared/SKILL.md) -- 认证和全局参数
