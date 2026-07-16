# 飞书 XML 语法

**语法遵循 HTML，渲染遵循 Markdown-Enhanced**。

以下为自解释的标签签名：必填属性写在开始标签内，可选属性写在说明中；`bool`=`true|false`，`A|B`=任选一，`T[]`=英文逗号分隔的多值。签名不是可直接复制的 XML；实际输出须为属性值加引号并填写真实值。

## Markdown 常用映射标签

- `p, h1-h6, blockquote, hr, img, b, em, u, del, br, span` 语义不变。
- `<a type=url-preview href>链接标题；渲染为预览卡片。</a>`
- `<latex>行内公式，如 E = mc^2。</latex>`
- `<ol><li seq="1">order1：seq=1 表示序号从1开始，为空时表示继承前序<ul><li>item1：子列表放在 li 内；新增列表项必须包在 ul 或 ol；</li><li>item2</li></ul></li><li>order1</li></ol>`
- `<table><colgroup><col/><col/></colgroup><thead><tr><th><p></p></th><th><p></p></th></tr></thead><tbody><tr><td><p></p></td><td><p></p></td></tr></tbody></table>`：表格。
- `<pre lang="类型"><code>代码内容</code></pre>`：代码块；可选 `caption`；代码必须放在 `<code>` 内，禁止直接放在 `<pre>` 下。
- `<img/>`：href="上传网络图片，支持 HTTP(S)"；src="token，复制原始图片"；href 和 src 必须存在一个；可选 `width, height, caption, name`。
- `<source name/>`：文件附件，可独立成块或内联。
- `<checkbox done=bool>待办项</checkbox>`
- `p, h1-h9, li, checkbox, title` 可选属性 `align=left|center|right`。

## 必备标签

- `<title>必有文档标题，每篇唯一</title>`

## 飞书特有拓展标签

- `<cite type=user user-id="open-id"></cite>`：@人，会渲染为用户头像，不得写纯文本名字，必须显式传入 `user-id`
- `<cite type=doc doc-id="doc-token"></cite>`：@文档，会渲染为文档标题
- `<whiteboard type=blank|mermaid|plantuml|svg path="@相对路径文件">支持通过 path 直接导入，也支持直接写入。</whiteboard>`
- `<grid><column width-ratio=0.5><p>分栏；各列 width-ratio 之和必须为 1。</p></column><column width-ratio=0.5><p>内容</p></column></grid>`
- `<callout><p>高亮块内容，无特殊渲染要求、正式场景慎用；子块仅支持文本、标题、列表、待办、引用；可选 emoji（默认 bulb）、background-color、border-color、text-color。</p></callout>`
- 其他拓展标签时，figure、bookmark、button、time、sheet、task、chat_card、sub-page-list、okr, 可查看 [`lark-doc-xml-extended-blocks.md`](lark-doc-xml-extended-blocks.md#okr-block)。

## 颜色与美化
- 基础色：`red, orange, yellow, green, blue, purple, gray`；常用 emoji：💡（默认）、✅、❌、📝、❓、❗、👍、❤️、📌、🏁、⭐。
- `<span text-color>`、`<callout text-color>`、`<callout border-color>`：基础色。
- `<span background-color>`、`<th/td background-color>`、`<button background-color>`：基础色 + `light-{色}` + `medium-gray`。
- `<callout background-color>`：`gray` + `light-{色}` + `medium-{色}`。

## 转义规则

禁止转义标签本身；只转义标签内部的文本内容。

- 文本转义：`<` → `&lt;`，`>` → `&gt;`，`&` → `&amp;`，换行符 `\n` → `<br/>`。
- 错误：`&lt;p&gt;内容&lt;/p&gt;`
- 正确：`<p>A &amp; B 的对比：1 &lt; 2</p>`