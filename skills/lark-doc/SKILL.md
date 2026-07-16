---
name: lark-doc
description: "飞书云文档（Docx / Wiki）内容操作：读取、创建、编辑文档，插入或下载图片附件，以及操作思维笔记。用户提供文档 URL/token（包括 doubao.com 的 /docx/、/wiki/）时使用；按 URL 路径/token 而非域名路由。文档内嵌资源按读取参考中的统一规则分流。文档评论走 lark-drive；表格或 Base 内部数据操作不在本 skill。"
metadata:
  requires:
    bins: ["lark-cli"]
  cliHelp: "lark-cli docs --help;lark-cli mindnotes --help"
---

# docs

## 场景与 Shortcut 路由

**CRITICAL：先判断场景，再读取该场景的参考文件；不要在任务开始时一次性读取全部参考文件。每个文件只在首次进入对应阶段时读取一次。**

**身份：文档操作默认使用 `--as user`**

优先使用 `lark-cli docs +<verb>` Shortcut；思维笔记使用独立的 `mindnotes` 命令。

### 文档正文

- **读取 / 摘要 — [`+fetch`](references/lark-doc-fetch.md)**：先读参考再获取文档。只读或摘要默认用 `simple`；更新前定位用局部 `with-ids`；保真改写才读 `full`；带 `#share-...` 的选区链接按原 URL 传入。
- **从零创作 — [`创建工作流`](references/lark-doc-create-workflow.md) → [`+create`](references/lark-doc-create.md)**：先完整执行创建工作流，**简单任务不是跳过的理由**；通过 Publish Gate 后再创建文档。
- **导入 / 空文档 — [`+create`](references/lark-doc-create.md)**：仅创建空文档或原样导入用户提供的完整内容时，跳过创建工作流。
- **编辑 / block 直达链接 — [`+update`](references/lark-doc-update.md)**：语义改写、润色、重组、补写或排版时先完整读取参考，按推荐流程 fetch 后局部更新；明确旧文本 → 新文本可直接 `str_replace`，但写后必须 fetch 验证；每次更新后重新获取最新 block ID；block 直达链接也按该参考生成。

### 辅助能力

- **临时文件、解析与统计 — [`+script`](references/lark-doc-script.md)**：创建名称唯一的临时 XML，直接解析文档 URL / token 或本地 XML / Markdown、将 Markdown 转为 XML，或统计文档总字数 / 总字符数。
- **历史版本 — [`+history-list` / `+history-revert` / `+history-revert-status`](references/lark-doc-history.md)**：查询、回滚文档历史版本或检查回滚任务状态。

### 资源、画板与思维笔记

- **插入本地素材 — [`+media-insert`](references/lark-doc-media-insert.md)**：在文末插入本地图片或文件。
- **预览素材 — [`+media-preview`](references/lark-doc-media-preview.md)**：预览文档中的图片、附件或素材。
- **下载素材 — [`+media-download`](references/lark-doc-media-download.md)**：下载文档中的图片、附件、素材或画板缩略图。
- **Docx 封面 — [`+resource-download` / `+resource-update` / `+resource-delete`](references/lark-doc-resource-cover.md)**：下载、更新或删除 Docx 封面。
- **画板 — [`画板工作流`](references/lark-doc-whiteboard.md)**：创建或更新画板时先读取工作流；更新已有画板必须复用现有 token，禁止新建空白画板；底层写入优先使用 [`whiteboard +update`](../lark-whiteboard/references/lark-whiteboard-update.md)，`docs +whiteboard-update` 仅为别名。
- **思维笔记 — `mindnotes`**：已有思维笔记走 [`思维笔记链路`](references/lark-doc-mindnote.md)；新建思维笔记走 [`lark-doc-whiteboard`](references/lark-doc-whiteboard.md)。

## 不在本 Skill 范围

- **Drive 文件级操作**：找文档、导入导出、云空间文件上传 / 下载 / 权限管理 → [`lark-drive`](../lark-drive/SKILL.md)。复制文档、创建副本或另存为副本时，按其指引使用 `lark-cli drive files copy`；不要用 `docs +fetch` + `docs +create` 重建正文，也不要走 `drive +export` / `drive +import`。
- **文档评论**：添加、查看、回复评论或增删 reaction → [`lark-drive`](../lark-drive/SKILL.md)。
- **文档内嵌资源下钻**：处理不在本 Skill 范围的内嵌资源（如电子表格或 Base 内部数据）时，统一读取 [`lark-doc-fetch.md`](references/lark-doc-fetch.md#处理文档内嵌资源) 的「处理文档内嵌资源」，提取 token / ID 后按该节切到对应 Skill 或命令；根 Skill 不重复维护标签分发表。
