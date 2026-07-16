# Lark Doc Authoring

本文件定义从零创作，以及对已有正文进行改写、润色、重组、补写和排版的流程。根 `SKILL.md` 负责场景与格式路由；本文件负责内容判断；格式文件定义表达语法；`create` / `update` 定义写入操作。

## Philosophy

文档是为读者服务的信息传递，不是作者的自我表达。唯一标准是：读者能否以最低成本获取所需信息并形成正确理解。

- **读者本位**：落地前先回答：读者是谁、为什么要读、带着什么任务来。按读者的任务组织内容，不按功能或作者视角罗列。
- **结构先行**：结论先行，先整体后局部；按逻辑分组与递进，依据关系选择列表、步骤或表格，使内容便于扫读。（特殊体裁除外）
- **极简表达**：默认使用能清楚表达关系的最简单形式；在不损失信息的前提下压缩文字；删冗余，用短句、动词和数据，在文字难以说清流程、交互或层级时用图。
- **表达一致**：同一对象、动作和状态全文同名；标题层级与编号采用统一体系。用户提供样例或已有文档时，在不违反更高优先级规则的前提下延续其有效结构、语气、术语和编号。
- **约束栈**：用户硬约束 > 读者任务 > 内容 > 组件样式；后项不得牺牲或放宽前项，格式与组件不得反向改变内容判断。

## Step Plan

**CRITICAL：按下述步骤，step by step 严格执行，不可跳过任何步骤。**

### Step_1：深度理解读者任务、文档格式要求、硬约束和禁区。

### Step_2：选择一个 genre content contract；

- 读取高置信命中的最多一个 Profile 和最多一个 Adapter，并把实际适用的义务写回 Brief；未命中就保持 `none`。
- contract 决定内容任务、证据和体裁边界；adapter 只调整与所选 contract 兼容的平台结构、语气和组件。

   | Content Profile | 独特专业任务 |
   |-|-|
   | [`route-workplace.md`](genres/route-workplace.md) | 组织决策、执行、留档 |
   | [`route-report.md`](genres/route-report.md) | 数据、研究和证据形成洞察 |
   | [`route-knowledge.md`](genres/route-knowledge.md) | 理解、自学、一次已知操作或检索 |
   | [`route-media.md`](genres/route-media.md) | 独立采集、核实和公共理解 |
   | [`route-opinion.md`](genres/route-opinion.md) | 形成并论证判断 |
   | [`route-consumer.md`](genres/route-consumer.md) | 以真实体验或测试辅助消费选择 |
   | [`route-marketing.md`](genres/route-marketing.md) | 组织授权的认知、转化或公关内容 |
   | [`route-personal-brand.md`](genres/route-personal-brand.md) | 本人经历、能力和作品的可信呈现 |
   | [`route-creative.md`](genres/route-creative.md) | 角色、冲突、情节与分支叙事 |

   | Adapter | 渠道 |
   |-|-|
   | [`route-platform.md`](genres/route-platform.md) | Email、微信公众号、小红书 |

### Step 3：在生成草稿前完成 Presentation Decision，并使用下方 JSON 结构记录关键决策，**在思维链里显示输出**。
```json
   {
      "target": "",
      "genre_contract": "",
      "adapter": "",
      "presentation_mode": "当 contract 为 none 时，默认 rich 模式；",
      "hard_rule": "",
      "visual_plan": {
         "reason": "解释是否需要图片、画板等组件，以及为什么选择该组件。",
         "img_enabled": "",
         "whiteboard_enabled":""
      }
   }
```

1. presentation_mode 有三种选择：
   - **`formal`（表达非常正式）**：庄重、准确、简洁、直接，结构与措辞服从正式规范；默认只使用短段落、列表和普通链接等基础结构；只使用 contract 明示允许或限用的 block，不以 rich block(callout，emoji)、颜色或装饰制造正式感。
   - **`normal`（正常）**：用最简单的清楚表达；扩展组件必须降低理解、执行或出错成本。
   - **`rich`（表达非常丰富）**：文风或语气可以更鲜明，**必须主动**扫描图片和画板机会，并优先采用高价值且通过预检的候选；不以 emoji、颜色或组件数量冒充丰富，也不设置固定配额。
2. visual_plan：
   - **一图胜千言**：图片和画板可承载远多于纯文字的视觉信息，具有很高的语义价值，应该考虑优先使用，且能极大降低人类的理解成本。
   - 当信息满足下列条件时，可以考虑用相关组件表达，并解释为什么选择该组件。

      | 信息关系 | 通常合适的表达 |
      |-|-|
      | 同一组字段的精确比较或映射 | 表格 |
      | 流程、路线、依赖、分支、时序、层级、因果、空间、拓扑、概念关联，或需要整体把握的结构与关系 | 画板 |
      | 对象、场景、环境、界面、外观、氛围、风格、空间感、概念意象、示例或视觉证据，以及需要建立直观感受的内容 | 图片 |
      | 两组简短、等权且适合横向阅读的信息 | grid |
      | 单个关键提醒或限制 | callout |
      | 简单并列、步骤或连续论述 | 列表或段落 |

### Step 4：根据初步要求，收集更多资料

1. 当现有信息不足时，必须补充更多资料，不能直接创建。可以重新从互联网、数据库、文件等来源获取，补充完整信息。
2. 当需要图片时，必须**及时**把图片拉取到本地，后续在草稿中引用本地图片。

### Step 5：读取 [`lark-doc-xml.md`](lark-doc-xml.md)，创建任务独占的临时 XML，并结合上述规则和 Philosophy 原则生成 release candidate。使用扩展标签时按需读取 [`lark-doc-xml-extended-blocks.md`](lark-doc-xml-extended-blocks.md)。

1. 在将任何 XML 草稿写入磁盘前，选取一个不带 `.xml` 的可移植文件名（例如 `draft`），执行 `lark-cli docs +script --command create-temp-xml --file-name "<文件名>" --format json`。
2. 把返回的 `data.path` 记为本任务的 `draft_path`，只向该文件写入 release candidate。不得自行去掉目录中的随机值、复用已存在文件或使用其他任务的 `draft_path`。
3. 若明确命中 Markdown 例外，不创建 XML 文件，但仍须使用当前任务独占的随机 Markdown 文件名。
4. 如果发现 XML 文件存在语法错误，不要全局覆盖重写，使用局部 patch 修复。

### Step 6： 执行 Draft Parse Gate，并结合返回的 profile 检查当前稿件。
- **解析**：对 XML release candidate 执行 `lark-cli docs +script --command parse --content "@<draft_path>" --format json`。命令必须成功，并根据结果校验 block 类型和字数等指标。
- **读者与范围**：内容服务读者任务，核心命题和交付范围清楚，没有与读者任务无关的章节。
- **结构与体裁**：各部分关系和顺序合理；采用高置信度路由时，稿件满足 contract 与可选 adapter 的要求，无缺项、重复或近邻体裁混用；
- **表达**：表达具体、简练、术语一致；需要连贯论述的内容没有被拆成零散列表；标题、列表、表格和编号各司其职，并符合所选 mode、可选 contract 与 adapter。
- **一致性**：检查完整标题树；同一目录体系内的同级标题必须统一带或不带序号，编号格式、层级关系和顺序一致、连续，不得局部换制或跳级；颜色与视觉强调保持统一语义；同一对象、动作、状态和专有名词全文同名。
- **字数与硬约束**：用户硬约束全部满足；有明确字数或字符数要求时，以 `profile.word_count` / `profile.char_count` 的实测值为准，不自行估算。
- **Block 组件与内容**：实际类型满足可选 contract 与 adapter 声明的允许、限用和禁止条件；rich block 服务所选 mode 和真实信息关系。
- **处理未通过项**：可用当前材料修复时直接修订；缺少关键事实或资料时，可在获得授权后检索补充；。

### Step 7： 只有最新 release candidate 解析成功、用户硬约束与质量检测全部通过，才读取 [`lark-doc-create.md`](lark-doc-create.md)，按其中的 create 规则使用同一个 `draft_path` 执行写入和传输验证。检查命令是否成功、业务结果是否成功，并逐项处理 `warnings`；不得只看到文档 URL 就宣布完成。

### Step 8： 无论创建成功、失败或被阻塞，都使用当前运行时的文件删除能力精确删除本任务的 `draft_path` 及其随机父目录；不要依赖平台专用的 `rm` / `del`，也不要使用通配符。最终只交付用户需要的结果，并说明必要来源、未关闭缺口、异常、失败或阻塞原因，以及文档 URL 或 token。
