# Spring AI 网页版 Agent Demo

一个可以直接在浏览器里玩的 Agent 示例：Spring Boot 3.5 + Spring AI 1.1 + Spring AI Alibaba Graph + WebFlux，支持 **图级 ReAct、流式输出、多轮记忆、工具调用、RAG 知识库检索、StateGraph 检查点**。

页面会把 Agent 的思考过程摊开给你看：什么时候查了时间、算了数、翻了知识库，分别传了什么参数、拿到什么结果，都会以卡片形式实时出现在回答上方。Graph 用 `agent ⇄ tools` 的条件循环实现 ReAct，每一步都会用 `MemorySaver` 保存检查点，可以从任意历史检查点回放。

![tech](https://img.shields.io/badge/Spring%20Boot-3.5.15-6DB33F) ![tech](https://img.shields.io/badge/Spring%20AI-1.1.8-6DB33F) ![tech](https://img.shields.io/badge/Spring%20AI%20Alibaba%20Graph-1.1.2.4-c71d23) ![tech](https://img.shields.io/badge/Java-17-orange)

## 功能

- **图级 ReAct**：`agent` 节点让模型决策，`tools` 节点执行工具，条件边根据 `tool_calls` 决定回环还是结束，最多 `app.react.max-iterations` 轮。
- **流式对话**：`/api/chat/stream` 通过 SSE 返回；模型决策使用非流式调用以保证工具参数完整，最终回答按块实时推送。
- **会话记忆**：同一个 `conversationId` 保留最近 30 条消息，由 `AgentService` 手动读写 `ChatMemory`。
- **工具调用**：时间、时区、天气、计算器、待办共 7 个工具，由 Graph 的 `tools` 节点逐个执行。
- **RAG 知识库**：启动时把 `knowledge/*.md` 切块写入内存向量库，每轮对话检索 Top 4 片段注入上下文。
- **引用可追溯**：回答标注 `[1]`、`[2]`，页面展示对应文档与片段摘要。
- **工具过程可视化**：工具的开始 / 结果 / 错误通过 SSE 实时推送，前端渲染成卡片。
- **节点级检查点**：`prepare`、每次 `agent`、每次 `tools` 执行后都由 `MemorySaver` 保存状态；侧边栏可以查看检查点并从历史检查点回放。
- **零前端构建**：一个 `index.html`，无 npm、无打包步骤。

## 快速开始

### 1. 环境要求

- JDK 17+
- Maven 3.9+（没有的话：`brew install maven`，或直接用 IntelliJ IDEA 打开 `pom.xml`）

### 2. 配置模型

```bash
export OPENAI_API_KEY=sk-你的key
export OPENAI_BASE_URL=https://api.openai.com
export OPENAI_CHAT_MODEL=gpt-4o-mini
export OPENAI_EMBEDDING_MODEL=text-embedding-3-small
```

> `OPENAI_BASE_URL` 不要带 `/v1` 后缀，Spring AI 会自动拼接 `/v1/chat/completions`。

### 3. 启动

```bash
mvn spring-boot:run
```

浏览器打开 <http://localhost:8080>。

如果只想先看看页面、不配置 Key，也可以直接启动：应用能正常起来，只是发消息时会返回一条鉴权失败的提示。

## 接入不同模型

只要服务提供 OpenAI 兼容接口，改环境变量即可。

| 服务 | `OPENAI_BASE_URL` | `OPENAI_CHAT_MODEL` | Embedding |
| --- | --- | --- | --- |
| OpenAI | `https://api.openai.com` | `gpt-4o-mini` | `text-embedding-3-small` |
| DeepSeek | `https://api.deepseek.com` | `deepseek-chat` | 不支持，需 `RAG_ENABLED=false` |
| 阿里云百炼（兼容模式） | `https://dashscope.aliyuncs.com/compatible-mode` | `qwen-plus` | `text-embedding-v4` |
| vLLM | `http://localhost:8000` | 部署的模型名 | 视部署情况 |
| Ollama | `http://localhost:11434` | `qwen2.5:7b` | `nomic-embed-text` |

DeepSeek 只提供对话模型，没有 Embedding 接口。用它的时候启动知识库会失败并自动降级，建议显式关闭：

```bash
export OPENAI_BASE_URL=https://api.deepseek.com
export OPENAI_CHAT_MODEL=deepseek-chat
export RAG_ENABLED=false
```

## 接口

### 流式对话

```bash
curl -N -X POST http://localhost:8080/api/chat/stream \
  -H 'Content-Type: application/json' \
  -d '{"conversationId":"demo-1","message":"现在几点？再帮我算一下 (128*3+56)/4"}'
```

SSE 事件：

| 事件 | 含义 |
| --- | --- |
| `meta` | 会话 ID、turnId、RAG 是否就绪 |
| `sources` | 本轮检索到的知识库片段 |
| `tool` | 工具调用开始 / 结束 / 失败，带参数和结果 |
| `delta` | 模型输出的文本增量 |
| `checkpoint` | 本轮 Graph 执行完成后保存的检查点 ID |
| `error` | 可读的错误信息 |
| `done` | 本轮结束 |

### 同步对话

```bash
curl -s -X POST http://localhost:8080/api/chat \
  -H 'Content-Type: application/json' \
  -d '{"conversationId":"demo-1","message":"出差住宿标准是多少？"}'
```

返回：

```json
{
  "conversationId": "demo-1",
  "reply": "……",
  "sources": [{"index": 1, "source": "company-travel-policy.md", "snippet": "……"}],
  "toolCalls": [{"name": "getWeather", "phase": "start", "input": "{\"city\":\"杭州\"}", "output": null}]
}
```

### 服务信息

```bash
curl -s http://localhost:8080/api/meta
```

返回模型名、知识库状态、已加载文档、工具清单，页面启动时会调用它填充侧边栏。

### 检查点

列出某个会话的全部检查点，最新在前：

```bash
curl -s 'http://localhost:8080/api/checkpoints?conversationId=demo-1'
```

```json
{
  "conversationId": "demo-1",
  "count": 4,
  "checkpoints": [
    {"checkpointId": "1f0c9a..."},
    {"checkpointId": "b72d41..."},
    {"checkpointId": "c91e77..."},
    {"checkpointId": "e30a52..."}
  ]
}
```

一轮包含工具调用的对话通常会产生 4 个以上检查点：`prepare` 一个、每次 `agent` 一个、每次 `tools` 一个。

从指定检查点回放：

```bash
curl -s -X POST \
  'http://localhost:8080/api/checkpoints/1f0c9a.../resume?conversationId=demo-1'
```

返回 `reply` 和 `sources`。回放会用同一个 `threadId` 和对应 `checkPointId` 继续执行 Graph，因此检查点之后的节点会重新运行。

## 项目结构

```
spring-ai/
├── pom.xml
├── src/main/java/com/example/springaiagent/
│   ├── SpringAiAgentApplication.java
│   ├── agent/
│   │   ├── AgentEvent.java          # 流式事件：文本 / 引用 / 检查点 / 错误
│   │   ├── AgentEventBus.java       # 按 turnId 推送 Agent 事件
│   │   ├── AgentErrors.java         # 异常转可读提示
│   │   ├── ToolEvent.java           # 工具调用事件
│   │   ├── ToolCall.java
│   │   ├── ToolEventBus.java        # 按 turnId 收集并推送工具事件
│   │   └── ToolEventPublisher.java  # 工具埋点辅助类
│   ├── config/AgentConfig.java      # ChatMemory、VectorStore
│   ├── service/
│   │   ├── AgentService.java        # ReAct StateGraph、checkpoint、RAG、工具循环
│   │   ├── KnowledgeBaseService.java
│   │   └── ChatResult.java
│   ├── tools/                       # 时间 / 计算器 / 天气 / 待办
│   └── web/
│       ├── ChatController.java      # SSE、同步接口、meta
│       └── CheckpointController.java # 检查点列表 / 回放
└── src/main/resources/
    ├── application.yml
    ├── knowledge/*.md               # 三个示例文档
    └── static/index.html            # 单页聊天界面
```

## 实现要点

**ReAct 循环由图控制。** `agent` 节点用 `ChatModel` 调一次模型，把 `@Tool` 生成的 JSON Schema 随请求发给模型；如果响应里带 `tool_calls`，条件边进入 `tools` 节点，由 `ToolCallback.call(...)` 逐个执行工具，再把 `ToolResponseMessage` 写回 transcript，回到 `agent` 继续决策。没有 `tool_calls` 时直接结束。循环上限由 `app.react.max-iterations` 控制，避免模型无限调工具。

**为什么不用 ChatClient 自动工具循环。** Spring AI 1.1 中，`ChatModel` 只负责请求/响应，工具循环由 `ChatClient` 的 `ToolCallingAdvisor` 驱动。要拿到每一次 `tool_calls` 并让 Graph 在每次工具调用后保存检查点，就必须绕开 `ChatClient` 的自动循环，直接使用 `ChatModel` + 手动执行的 `ToolCallback`。代价是模型决策调用是非流式的，最终回答生成完成后按 48 字符一块推给前端，而不是逐 token 推送。

**StateGraph 与 checkpoint。** 一轮对话的节点是 `prepare → agent ⇄ tools`。`prepare` 负责 RAG 检索、system prompt 和本轮 transcript 初始化；`agent` 每次模型调用后保存一个检查点；`tools` 每次工具批次执行后保存一个检查点。`CompileConfig` 注册 `MemorySaver`，`CompiledGraph.getStateHistory(config)` 读回历史，`RunnableConfig.checkPointId(...)` 从指定快照继续执行。Graph 的 `threadId` 直接复用 `conversationId`。

**回放的副作用。** 从检查点继续执行会重跑该检查点之后的节点：从 `agent` 检查点回放会重新询问模型，从 `tools` 检查点回放会重新执行工具，并可能在 `ChatMemory` 里追加一轮消息。演示场景可以接受；生产环境应结合幂等键、人工确认或只读回放使用。

**工具事件怎么实时到前端。** 工具方法接收 `ToolContext`，里面带着本轮 `turnId`。`ToolEventPublisher` 把开始 / 结果写入 `ToolEventBus` 的 Reactor Sink，Controller 用 `Flux.merge` 把工具事件和模型文本合并成一条 SSE 流。`ToolContext` 的内容不会发送给模型。

**为什么 RAG 没用 `QuestionAnswerAdvisor`。** 这里手动做了 `similaritySearch` + 提示词拼接，因为需要把命中的文档和摘要作为 `sources` 事件推给前端，让引用可点击。想换成框架自带 Advisor 的话，在 `AgentService` 里注册 `spring-ai-advisors-vector-store` 依赖并使用 `QuestionAnswerAdvisor.builder(vectorStore).build()` 即可，但就拿不到页面上的来源卡片了。

**内存存储仅用于演示。** `MessageWindowChatMemory`、`SimpleVectorStore` 和 `MemorySaver` 都保存在 JVM 内存里，重启即清空。生产环境应替换为 `JdbcChatMemoryRepository`、PgVector / Milvus / Elasticsearch 等持久化向量库，以及 `RedisSaver` / `MysqlSaver` 等持久化 checkpoint saver。

## 配置项

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `OPENAI_API_KEY` | `demo-key` | 模型服务 Key |
| `OPENAI_BASE_URL` | `https://api.openai.com` | OpenAI 兼容地址，不带 `/v1` |
| `OPENAI_CHAT_MODEL` | `gpt-4o-mini` | 对话模型 |
| `OPENAI_EMBEDDING_MODEL` | `text-embedding-3-small` | 向量模型 |
| `RAG_ENABLED` | `true` | 是否启用知识库 |
| `REACT_MAX_ITERATIONS` | `8` | agent ⇄ tools 最大循环轮数 |
| `PORT` | `8080` | 服务端口 |

知识库参数在 `application.yml` 的 `app.rag` 下：`top-k`（默认 4）、`similarity-threshold`（默认 0.5）。检索不到足够相似的片段时不会注入资料，模型会直接基于通用知识回答。

## 常见问题

**启动时报 401 / invalid_api_key**

Key 无效或 base-url 不匹配。注意 base-url 不要带 `/v1`，也不要带 `/chat/completions`。

**侧边栏知识库显示「未启用」**

Embedding 模型不可用。日志里会有 `知识库初始化失败`。检查 embedding model 是否被当前服务支持；不支持就把 `RAG_ENABLED=false`，应用会自动降级成纯工具 Agent。

**模型不调用工具**

确认模型支持 Function Calling。部分小参数模型、部分 Ollama 模型不支持工具调用，只能普通聊天。换用 `gpt-4o-mini`、`qwen-plus`、`deepseek-chat` 等模型再试。

**页面能打开但消息不逐字出现**

ReAct 的模型决策调用是非流式的（要等完整 `tool_calls` 才能执行工具），最终回答在生成完成后按块推送。如果连分块都没有，通常是前置代理（Nginx 等）开了响应缓冲，需要关闭 `proxy_buffering`，或者直接访问 8080 端口。

**怎么确认 ReAct 循环真的生效了**

发一个需要多步工具的问题，例如「现在几点？查一下杭州天气，再把结果记成待办」。页面上应该出现多张工具卡片，侧边栏检查点数量会按 `prepare → agent → tools → agent...` 增长，而不是一轮只有两个。如果工具卡片有、但检查点只有两个，说明模型服务把工具调用放在了一次响应里返回，或者 Graph 没有按预期路由，可以看启动日志和 `/api/checkpoints` 返回。

**改 `REACT_MAX_ITERATIONS` 不生效**

这个值在应用启动时注入，改环境变量或 `application.yml` 后需要重启。

**修改了 `knowledge/*.md` 不生效**

知识库只在启动时加载，改完文档需要重启应用。

**检查点重启后还在吗**

不在。当前用的是 `MemorySaver`，检查点保存在 JVM 内存中，重启即清空。需要跨重启保留时，把 `AgentService.buildGraph()` 里的 `MemorySaver` 换成 `RedisSaver`、`MysqlSaver` 或 `PostgresSaver` 等实现即可。

**回放检查点为什么又调用了一次模型**

回放不是读取缓存的回答，而是用同一个 `threadId` 和 `checkPointId` 让 Graph 从该快照继续执行。检查点之后的 `agent` / `tools` 节点会重新运行，因此会再次调用模型或执行工具；如果这轮对话已经写入 `ChatMemory`，还可能追加一轮重复消息。

**为什么侧边栏没有检查点**

检查点只会在 Graph 成功执行并写入 saver 后出现。先正常发一条消息；如果仍然为空，查看启动日志里是否有 Graph 构建或 saver 相关异常。

## 说明

知识库中的差旅制度、杭州指南均为**虚构的演示内容**，天气工具返回的也是模拟数据，请勿用于真实业务决策。
