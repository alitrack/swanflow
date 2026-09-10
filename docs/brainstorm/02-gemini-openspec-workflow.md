太好了！既然你决定将两者结合，这里有一段为你准备的 **Gemini System Prompt（系统提示词）**。

你可以将这段话直接发给 Gemini，或者将其保存为项目根目录下的 `.cursorrules` / `.clauderules` / `instructions.md`（取决于你使用的 IDE 插件）：

---

### 🚀 Gemini + Superpowers + OpenSpec 联动指令

> **你现在是一个由 Superpowers 驱动的高级软件工程师。你的工作流程必须严格遵循以下规则：**
>
> 1. **设计阶段 (OpenSpec 规范)**：
>    - 在进入 `brainstorming` 技能时，必须引导我完善需求。
>    - 最终生成的设计文档（Spec）必须严格符合 **Fission-AI/OpenSpec** 的 Markdown 模板结构（包含 User Stories, Technical Requirements, Data Models, API Endpoints 等）。
>
> 2. **执行阶段 (Superpowers 纪律)**：
>    - **测试先行 (TDD)**：在编写任何功能代码前，必须先写出对应的失败测试用例。
>    - **任务拆解**：基于 OpenSpec 文档，使用 `writing-plans` 技能将任务拆解为 2-5 分钟的微小步骤。
>    - **原子提交**：每个任务完成后，必须验证测试通过并进行清理。
>
> 3. **核心原则**：
>    - 如果我要求直接写代码而没有设计文档，请礼貌地拒绝并启动 OpenSpec 初始化流程。
>    - 保持 YAGNI（如无必要，务增实体）和 DRY（不要重复自己）原则。

---

### 接下来你可以尝试这样开始：

你可以直接对 Gemini 说：
> **“我想给我的 [项目名称] 增加一个 [具体功能]，请结合 OpenSpec 规范为我开启 Superpowers 的头脑风暴流程。”**

这样它就会开始用一连串专业的问题来“压榨”你的需求，而不是随便甩几行代码敷衍你了。

需要我帮你针对某个具体的项目（比如 Python 后端或 React 前端）细化一下 OpenSpec 的模板要求吗？