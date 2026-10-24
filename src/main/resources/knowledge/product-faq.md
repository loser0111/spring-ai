# Spring AI Agent Demo 功能说明（示例文档）

> 本文档介绍本 Demo 自身的能力，方便通过对话测试 RAG 是否生效。

## 一、这个 Demo 是什么

这是一个基于 Spring Boot 3.5 和 Spring AI 1.1 的网页版 Agent 示例。它把大模型、工具调用、会话记忆和知识库检索串成一个可以直接在浏览器里体验的对话应用。

前端是一个单页聊天界面，后端使用 WebFlux 提供 SSE 流式接口。模型返回的文本会逐字显示，工具调用过程会以卡片形式实时插入到回答上方。

## 二、支持的能力

1. **流式输出**：通过 `/api/chat/stream` 返回 SSE 事件，前端边接收边渲染 Markdown。ReAct 决策调用是非流式的，最终回答生成后分块推送。
2. **会话记忆**：同一个 conversationId 内保留最近 30 条消息，支持多轮追问。
3. **图级 ReAct**：StateGraph 由 prepare、agent、tools 三个节点组成，agent 与 tools 之间用条件边循环，最多执行 REACT_MAX_ITERATIONS 轮（默认 8 轮）。
4. **工具调用**：模型可以按需调用时间、天气、计算器和待办工具，每次工具调用后都会保存检查点。
5. **知识库检索（RAG）**：启动时把 `src/main/resources/knowledge/*.md` 切块写入内存向量库，每次提问先检索最相关的 4 个片段，再连同问题一起发给模型。
6. **引用来源**：回答中会标注 [1]、[2]，页面底部展示对应的文档名和片段摘要。
7. **工具过程可视化**：每个工具的开始、结束和错误都会推送到前端，方便观察 Agent 的决策过程。
8. **StateGraph 检查点**：prepare、每次 agent、每次 tools 执行后都会通过 MemorySaver 保存检查点，可以从历史检查点回放。

## 三、内置工具

| 工具名 | 用途 | 说明 |
| --- | --- | --- |
| getCurrentDateTime | 查询当前日期、时间和星期 | 使用 Asia/Shanghai 时区 |
| getTimeInZone | 查询指定 IANA 时区的时间 | 例如 Asia/Tokyo |
| getWeather | 查询城市天气 | 演示用模拟数据，不是实时天气 |
| calculate | 计算数学表达式 | 支持四则运算、括号和乘方 |
| addTodo | 添加待办事项 | 数据按会话隔离，保存在内存中 |
| listTodos | 查看待办清单 | 只返回当前会话的数据 |
| completeTodo | 完成并移除待办 | 需要提供待办原文 |

## 四、接口说明

- `POST /api/chat/stream`：流式对话，返回 `text/event-stream`。请求体为 `{"conversationId":"c-1","message":"你好"}`。
- `POST /api/chat`：同步对话，返回完整回答、引用来源和工具调用记录。
- `GET /api/meta`：返回当前模型名、知识库状态、文档列表和工具列表。
- `GET /api/checkpoints?conversationId=c-1`：列出该会话的检查点，最新的在前。
- `POST /api/checkpoints/{checkpointId}/resume?conversationId=c-1`：从指定检查点继续执行，返回回放后的回答。

SSE 事件类型包括：`meta`（会话信息）、`sources`（引用来源）、`tool`（工具调用）、`delta`（文本增量）、`checkpoint`（检查点已保存）、`error`（错误）、`done`（结束）。

## 五、配置项

- `OPENAI_API_KEY`：模型服务的 API Key。
- `OPENAI_BASE_URL`：OpenAI 兼容服务的地址，不要带 `/v1` 后缀。
- `OPENAI_CHAT_MODEL`：对话模型名，默认 `gpt-4o-mini`。
- `OPENAI_EMBEDDING_MODEL`：向量模型名，默认 `text-embedding-3-small`。
- `RAG_ENABLED`：是否启用知识库检索，默认 `true`。
- `REACT_MAX_ITERATIONS`：agent 与 tools 的最大循环轮数，默认 `8`。

## 六、常见问题

**问：工具调用没有触发怎么办？**

答：确认使用的模型支持 Function Calling。部分轻量模型或本地模型不支持工具调用，此时只能进行普通对话。当前实现由 Graph 的 tools 节点执行工具，模型只负责返回 tool_calls。

**问：为什么回答不是逐字出现？**

答：ReAct 的决策调用需要拿到完整的 tool_calls 才能执行工具，所以用的是非流式模型调用；最终回答生成完成后会按块推送，前端仍然能看到分块渲染和工具卡片。

**问：知识库状态显示未启用怎么办？**

答：通常是 Embedding 模型不可用。请检查 base-url、api-key 和 embedding model 是否匹配；如果当前服务只提供对话模型，可以设置 `RAG_ENABLED=false` 关闭检索。

**问：会话重启后会丢失吗？**

答：会。当前使用内存版 ChatMemory、SimpleVectorStore 和 MemorySaver，应用重启后会话、向量数据和检查点都会清空。生产环境应换成 JDBC ChatMemory、持久化向量库以及 Redis/MySQL 版本的 checkpoint saver。

**问：检查点回放会重新调用模型吗？**

答：会。回放不是读取缓存的回答，而是让 StateGraph 从指定检查点继续执行，检查点之后的 agent / tools 节点会重新调用模型或执行工具，因此可能产生新的回答，也可能在会话记忆里追加一轮消息。

**问：可以接入哪些模型？**

答：任何提供 OpenAI 兼容接口的服务都可以，例如 OpenAI、DeepSeek、通义千问兼容模式、vLLM、Ollama 的 OpenAI 兼容端点等。只需要修改 base-url、api-key 和模型名。
