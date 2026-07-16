# `docs +script`

`docs +script` 可创建名称唯一的临时 XML、解析并统计本地内容或在线文档，也可在本地转换格式。`--command parse` 必须且只能提供一种输入：用 `--doc` 传文档 URL / token，或用 `--content` 传字面内容、`@当前目录下的相对路径`、`-`（stdin）。`--format json` 只控制 CLI 输出格式，不表示输入格式。

## 创建唯一的临时 XML

在准备 XML 草稿前先原子创建空文件，并读取返回的 `data.path`：

```bash
lark-cli docs +script --command create-temp-xml --file-name "川西" --format json
```

成功时返回：

```json
{
  "data": {
    "path": "川西_123456789_folder/川西.xml"
  }
}
```

- `--file-name` 输入不带 `.xml` 扩展名的可移植名称。命令创建 `<名称>_<随机值>_folder/<名称>.xml`；例如输入 `川西`，返回 `川西_123456789_folder/川西.xml`。
- `path` 是当前工作目录下的相对路径，也是成功结果中的唯一业务字段，可直接组成 `--content "@<data.path>"`。
- 随机目录通过原子操作创建；并发任务不得自行去掉随机值、改成固定目录，也不得复用另一个任务返回的路径。
- 该能力直接使用 CLI 的跨平台文件接口，不依赖 Unix `mktemp` 或 Windows 专用命令。
- 名称不得包含路径分隔符、Windows 保留字符或设备名。`create-temp-xml` 不接受 `--content`、`--doc`、`--output` 或 `--overwrite`。文件初始为空；写入、解析和文档创建结束后，精确删除 `data.path` 及其随机父目录。

## 解析 XML 或 Markdown

使用同一条 `parse` 指令。shortcut 根据内容自动识别 XML 或 Markdown，调用方不需要判断或声明输入格式：

```bash
lark-cli docs +script --command parse --doc "<文档 URL 或 token>" --format json
lark-cli docs +script --command parse --content "@document.xml" --format json
lark-cli docs +script --command parse --content "@document.md" --format json
```

`--doc` 支持裸 Docx token，以及包含 `/docx/` 或 `/wiki/` 的文档 URL。该模式会联网读取 XML 后直接解析，不需要先执行 `docs +fetch` 或创建临时文件，并需要 `docx:document:readonly` 权限。本地 `--content` 模式不发起 OpenAPI 请求。`--doc` 不适用于 `markdown-to-xml`。

XML 输入执行严格解析，但为与服务端 SDK 对齐，带引号的属性值允许裸 `&` 并按字面值解析（例如 URL 查询参数 `...?seed=lark-cli&raw=1`）；完整的未知实体（如 `&unknown;`）、不完整标签、错误嵌套、非法属性或不支持的 LarkOpenCLI 标签仍会返回非零退出码。Markdown 输入按 LarkOpenCLI Markdown 语义解析。资源块内部未出现在输入文本中的内容不计入字数或字符数。

成功时 `data` 只包含 `profile`：

```json
{
  "data": {
    "profile": {
      "word_count": 10,
      "char_count": 15,
      "block_count": 2,
      "blocks": [
        {"type": "p", "count": 1, "ratio": 0.5},
        {"type": "title", "count": 1, "ratio": 0.5}
      ]
    }
  }
}
```

- `data.profile.word_count`：语义字数。统计汉字、英文单词 / URL / code path、数字、中文标点和独立可见符号；英文单词内部按一个语义单位计算。
- `data.profile.char_count`：可见字符数，不含空格；统计汉字、英文字母、数字、中英文标点和可见符号，非 BMP 符号按 UTF-16 code unit 计算。
- `data.profile.block_count`：block 总数。
- `data.profile.blocks[]`：每种 block 的 `type`、`count` 和 `ratio`；`ratio = count / block_count`。

## Markdown 转 XML

`markdown-to-xml` 只负责把 Markdown 转成 LarkOpenCLI XML：

```bash
lark-cli docs +script --command markdown-to-xml --content "@document.md" --format json
```

成功时 `data` 只包含转换结果：

```json
{
  "data": {
    "xml": "<h1>标题</h1><p>正文</p>"
  }
}
```

该指令不返回 `profile`。需要统计原 Markdown 时，独立执行 `--command parse`。
