# docs +create（创建飞书云文档）

从 XML（默认）或 Markdown 内容创建一个新的飞书云文档；语义创作默认使用 XML，只有 Authoring 明确判定为 Markdown 例外时才使用 Markdown。

## 命令

```bash
lark-cli docs +script --command create-temp-xml --file-name "draft" --format json
lark-cli docs +create --doc-format xml --content "@<create-temp-xml 返回的 data.path>"
```

单次内容优先使用 `--content -` 从 stdin 读取。XML 使用 `@file` 时，必须先给 `create-temp-xml` 传入不带扩展名的文件名；命令会原子创建 `<名称>_<随机值>_folder/<名称>.xml`，再把稿件写入返回的 `data.path`。不得去掉随机值、复用已存在目录或使用其他任务返回的路径。`@file` 只接受当前工作目录下的相对路径，且参数必须整体加引号，例如 `"@<data.path>"`。明确命中 Markdown 例外时才创建任务独占的临时 Markdown 文件。完成 Deliver 的传输验证后，只清理本任务创建的文件及其随机父目录，不得使用通配符清理，也不得依赖平台专用的 `rm` / `del` 命令。

## 返回值

```json
{
  "ok": true,
  "identity": "user",
  "data": {
    "document": {
      "document_id": "docx_token",
      "revision_id": 1,
      "url": "https://xxx.feishu.cn/docx/docx_token",
      "new_blocks": [
        { "block_id": "blkcnXXXX", "block_type": "whiteboard", "block_token": "boardXXXX" }
      ]
    }
  }
}
```

- **`document.new_blocks`**：本次操作新增的 block 列表（如画板）。`block_id` 可用于 `docs +update` 的 `--block-id` 做精确编辑；`block_token` 是资源块（如画板）的 token，可交给 `lark-whiteboard` 等 skill 继续操作

## 结果处理与退出条件

1. 检查命令是否成功、业务结果是否成功，并逐项处理 `warnings`；不得只看到文档 URL 就宣布完成。
2. 传输不一致或存在未处理降级时，基于 fetch 结果修复并重新验证；实际结果与已批准版本一致后才结束。

> \[!IMPORTANT]
> 如果文档是**以应用身份（bot）创建**的，如 `lark-cli docs +create --as bot` 在文档创建成功后，CLI 会**尝试为当前 CLI 用户自动授予该文档的 `full_access`（可管理权限）**。
>
> 以应用身份创建时，结果里会额外返回 `permission_grant` 字段，明确说明授权结果：
> - `status = granted`：当前 CLI 用户已获得该文档的可管理权限
> - `status = skipped`：本地没有可用的当前用户 `open_id`，因此不会自动授权；可提示用户先完成 `lark-cli auth login`，再让 AI / agent 继续使用应用身份（bot）授予当前用户权限
> - `status = failed`：文档已创建成功，但自动授权用户失败；会带上失败原因，并提示稍后重试或继续使用 bot 身份处理该文档
>
> `permission_grant.perm = full_access` 表示该资源已授予”可管理权限”。
>
> **不要擅自执行 owner 转移。** 如果用户需要把 owner 转给自己，必须单独确认。

## 参数

|参数|必填|说明|
|-|-|-|
|`--title`|否|文档标题，Markdown 导入时使用；XML 创建推荐在 `--content` 开头写 `<title>...</title>`；多个标题仅保留第一个并在 `warnings` / `degrade_details` 提示|
|`--content`|视情况|文档内容（XML 或 Markdown 格式）；不传 `--content` 时必须传 `--title`|
|`--reference-map`|否|结构化 `reference_map` JSON object；必须与 `--content` 一起使用。普通写入优先把结构写在正文里；该参数主要用于保留或回放已有 `document.reference_map`。支持直接 JSON、任务独占目录内的相对 `@file`，或 `-` 从 stdin 读取。|
|`--doc-format`|否|CLI 与语义创作均默认 `xml`，并建议显式传入；仅用户明确要求 Markdown 或保真导入 Markdown 时使用 `markdown`。单次内容禁止混用两种语法。|
|`--parent-token`|否|父文件夹或知识库节点 token（与 `--parent-position` 互斥）|
|`--parent-position`|否|父节点位置，如 `my_library`（与 `--parent-token` 互斥）|
