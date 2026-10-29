package com.example.springaiagent.service;

import com.alibaba.cloud.ai.graph.CompileConfig;
import com.alibaba.cloud.ai.graph.CompiledGraph;
import com.alibaba.cloud.ai.graph.KeyStrategy;
import com.alibaba.cloud.ai.graph.KeyStrategyFactory;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.RunnableConfig;
import com.alibaba.cloud.ai.graph.StateGraph;
import com.alibaba.cloud.ai.graph.action.AsyncEdgeAction;
import com.alibaba.cloud.ai.graph.checkpoint.config.SaverConfig;
import com.alibaba.cloud.ai.graph.checkpoint.savers.MemorySaver;
import com.alibaba.cloud.ai.graph.state.StateSnapshot;
import com.alibaba.cloud.ai.graph.state.strategy.ReplaceStrategy;
import com.example.springaiagent.agent.AgentErrors;
import com.example.springaiagent.agent.AgentEvent;
import com.example.springaiagent.agent.AgentEventBus;
import com.example.springaiagent.approval.ApprovalService;
import com.example.springaiagent.tools.CalculatorTools;
import com.example.springaiagent.tools.TimeTools;
import com.example.springaiagent.tools.TodoTools;
import com.example.springaiagent.tools.WeatherTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.ToolResponseMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.ToolContext;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.document.Document;
import org.springframework.ai.openai.OpenAiChatOptions;
import org.springframework.ai.support.ToolCallbacks;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Service
public class AgentService {

    private static final Logger log = LoggerFactory.getLogger(AgentService.class);

    private static final String NODE_PREPARE = "prepare";
    private static final String NODE_AGENT = "agent";
    private static final String NODE_TOOLS = "tools";
    private static final String ROUTE_END = "end";

    /**
     * 工具名 -> 给用户看的中文名，用于确认弹窗。
     */
    private static final Map<String, String> TOOL_LABELS = Map.of(
            "getCurrentDateTime", "查询当前时间",
            "getTimeInZone", "查询时区时间",
            "getWeather", "查询城市天气",
            "calculate", "计算表达式",
            "addTodo", "添加待办",
            "listTodos", "查看待办",
            "completeTodo", "完成待办");

    private static final String BASE_SYSTEM_PROMPT = """
            你是「Spring AI 小助手」，一个基于 ReAct 模式的网页版智能体。

            你的行为准则：
            1. 始终使用中文回答，使用 Markdown 排版，代码用代码块包裹。
            2. 需要实时信息（日期、时间、天气）或精确计算时，必须调用提供的工具，不要凭记忆编造。
            3. 用户提到待办事项时，主动使用待办工具记录或查询。
            4. 如果提供了「知识库参考资料」，优先依据资料回答，并在相关句子末尾用 [1]、[2] 标注来源编号。
            5. 资料没有覆盖的内容要明确说明，不要编造公司政策、价格或承诺。
            6. 拿到工具结果后先判断信息是否足够；不够可以继续调用工具，足够就给出最终回答。
            7. 回答简洁直接，必要时才展开细节。
            """;

    private final ChatModel chatModel;
    private final ChatMemory chatMemory;
    private final KnowledgeBaseService knowledgeBase;
    private final AgentEventBus agentEventBus;
    private final ApprovalService approvalService;
    private final List<ToolCallback> toolCallbacks;
    private final Map<String, ToolCallback> callbacksByName;
    private final String modelName;
    private final Double temperature;
    private final int maxIterations;
    private final CompiledGraph compiledGraph;

    public AgentService(
            ChatModel chatModel,
            ChatMemory chatMemory,
            KnowledgeBaseService knowledgeBase,
            AgentEventBus agentEventBus,
            ApprovalService approvalService,
            TimeTools timeTools,
            CalculatorTools calculatorTools,
            WeatherTools weatherTools,
            TodoTools todoTools,
            @Value("${spring.ai.openai.chat.options.model:gpt-4o-mini}") String modelName,
            @Value("${spring.ai.openai.chat.options.temperature:0.6}") Double temperature,
            @Value("${app.react.max-iterations:8}") int maxIterations) {
        this.chatModel = chatModel;
        this.chatMemory = chatMemory;
        this.knowledgeBase = knowledgeBase;
        this.agentEventBus = agentEventBus;
        this.approvalService = approvalService;
        this.modelName = modelName;
        this.temperature = temperature;
        this.maxIterations = maxIterations;

        ToolCallback[] callbacks = ToolCallbacks.from(timeTools, calculatorTools, weatherTools, todoTools);
        this.toolCallbacks = List.of(callbacks);
        Map<String, ToolCallback> byName = new HashMap<>();
        for (ToolCallback callback : callbacks) {
            byName.put(callback.getToolDefinition().name(), callback);
        }
        this.callbacksByName = Map.copyOf(byName);

        this.compiledGraph = buildGraph();
    }

    /**
     * ReAct 图：
     * prepare → agent ──有工具调用──→ tools ──→ agent
     *                └──没有工具调用──→ END
     *
     * <p>编译时配置了 interruptBefore(tools)：每次执行工具前都会停下来，
     * 由 {@link #advanceUntilBlocked} 决定是直接放行（会话已授权）还是推一条确认请求给前端。
     * 每个节点执行后 MemorySaver 都会保存一份状态快照，因此工具循环的每一步都能回放。</p>
     */
    private CompiledGraph buildGraph() {
        try {
            AsyncEdgeAction routeAfterAgent = state ->
                    CompletableFuture.completedFuture(readText(state, "next", ROUTE_END));

            // StateGraph 的 addNode 只接受 AsyncNodeAction（返回 CompletableFuture），
            // 所以这里把同步的节点方法包一层。
            StateGraph graph = new StateGraph(keyStrategies())
                    .addNode(NODE_PREPARE, state -> CompletableFuture.completedFuture(prepare(state)))
                    .addNode(NODE_AGENT, state -> CompletableFuture.completedFuture(agent(state)))
                    .addNode(NODE_TOOLS, state -> CompletableFuture.completedFuture(tools(state)))
                    .addEdge(StateGraph.START, NODE_PREPARE)
                    .addEdge(NODE_PREPARE, NODE_AGENT)
                    .addConditionalEdges(
                            NODE_AGENT,
                            routeAfterAgent,
                            Map.of(NODE_TOOLS, NODE_TOOLS, ROUTE_END, StateGraph.END))
                    .addEdge(NODE_TOOLS, NODE_AGENT);

            CompileConfig compileConfig = CompileConfig.builder()
                    .saverConfig(SaverConfig.builder()
                            .register(new MemorySaver())
                            .build())
                    // 工具执行前暂停，等待用户确认；确认后从检查点恢复进 tools 节点。
                    .interruptBefore(NODE_TOOLS)
                    .build();

            return graph.compile(compileConfig);
        } catch (Exception e) {
            throw new IllegalStateException("构建 ReAct StateGraph 失败", e);
        }
    }

    private KeyStrategyFactory keyStrategies() {
        return () -> {
            Map<String, KeyStrategy> strategies = new HashMap<>();
            strategies.put("conversationId", new ReplaceStrategy());
            strategies.put("turnId", new ReplaceStrategy());
            strategies.put("userMessage", new ReplaceStrategy());
            strategies.put("systemPrompt", new ReplaceStrategy());
            strategies.put("sources", new ReplaceStrategy());
            strategies.put("transcript", new ReplaceStrategy());
            strategies.put("iterations", new ReplaceStrategy());
            strategies.put("next", new ReplaceStrategy());
            strategies.put("answer", new ReplaceStrategy());
            strategies.put("lastNode", new ReplaceStrategy());
            return strategies;
        };
    }

    /**
     * 流式回答：Graph 在 boundedElastic 上执行，节点把文本增量、引用来源、检查点写入事件通道。
     */
    public Flux<AgentEvent> stream(String message, String conversationId, String turnId) {
        agentEventBus.open(turnId);
        return Flux.defer(() -> {
            Mono.fromRunnable(() -> runGraph(message, conversationId, turnId))
                    .subscribeOn(Schedulers.boundedElastic())
                    .subscribe();
            return agentEventBus.events(turnId);
        });
    }

    /**
     * 非流式回答，方便 curl / 接口测试。
     */
    public ChatResult chat(String message, String conversationId, String turnId) {
        try {
            RunnableConfig config = RunnableConfig.builder().threadId(conversationId).build();
            Outcome outcome = advanceUntilBlocked(
                    compiledGraph.invoke(inputs(message, conversationId, turnId), config),
                    config, conversationId, turnId);
            return toChatResult(outcome);
        } catch (Exception e) {
            throw new IllegalStateException("Graph 执行失败：" + e.getMessage(), e);
        }
    }

    /**
     * 从指定检查点回放：复用同一个 threadId 对应的状态，从 checkpointId 继续执行。
     * 如果回放途中又要调用工具，会再次生成确认请求。
     */
    public ChatResult resume(String conversationId, String checkpointId) {
        try {
            RunnableConfig config = RunnableConfig.builder()
                    .threadId(conversationId)
                    .checkPointId(checkpointId)
                    .build();
            Outcome outcome = advanceUntilBlocked(
                    compiledGraph.invoke(Map.of(), config), config, conversationId, null);
            return toChatResult(outcome);
        } catch (Exception e) {
            throw new IllegalStateException("从检查点回放失败：" + e.getMessage(), e);
        }
    }

    /**
     * 返回该会话的全部检查点，最新的在前。
     */
    public List<String> checkpointIds(String conversationId) {
        try {
            RunnableConfig config = RunnableConfig.builder().threadId(conversationId).build();
            Collection<StateSnapshot> history = compiledGraph.getStateHistory(config);
            if (history == null) {
                return List.of();
            }
            return history.stream()
                    .map(snapshot -> snapshot.config().checkPointId().orElse(null))
                    .filter(Objects::nonNull)
                    .toList();
        } catch (Exception e) {
            log.warn("读取检查点失败：{}", e.toString());
            return List.of();
        }
    }

    private void runGraph(String message, String conversationId, String turnId) {
        try {
            RunnableConfig config = RunnableConfig.builder().threadId(conversationId).build();
            advanceUntilBlocked(
                    compiledGraph.invoke(inputs(message, conversationId, turnId), config),
                    config, conversationId, turnId);
        } catch (Exception e) {
            log.warn("ReAct Graph 执行失败", e);
            agentEventBus.emit(turnId, new AgentEvent.Error(AgentErrors.friendly(e)));
        } finally {
            agentEventBus.complete(turnId);
        }
    }

    /**
     * 用户在确认接口里做出决定后，流式继续执行这一轮。
     */
    public Flux<AgentEvent> decide(String approvalId, boolean approved, boolean remember) {
        return Flux.defer(() -> {
            Optional<ApprovalService.Pending> consumed = approvalService.consumePending(approvalId);
            if (consumed.isEmpty()) {
                return Flux.just(new AgentEvent.Error("确认请求不存在、已被处理或已过期。"));
            }
            ApprovalService.Pending pending = consumed.get();
            boolean expired = pending.expiresAt() < System.currentTimeMillis();
            boolean effectiveApproved = approved && !expired;
            boolean effectiveRemember = effectiveApproved && remember;
            if (approved && expired) {
                log.info("确认请求已过期，按拒绝处理（turnId={}）", pending.turnId());
            }
            agentEventBus.open(pending.turnId());
            Mono.fromRunnable(() -> resumeAfterDecision(
                            pending, effectiveApproved, effectiveRemember))
                    .subscribeOn(Schedulers.boundedElastic())
                    .subscribe();
            return agentEventBus.events(pending.turnId());
        });
    }

    /**
     * 该会话是否还有未处理的确认请求。有的话不允许发起新消息，
     * 否则会从“等待确认”的检查点继续，而不是开始新一轮。
     */
    public boolean hasPendingApproval(String conversationId) {
        return approvalService.hasPending(conversationId);
    }

    private void resumeAfterDecision(ApprovalService.Pending pending, boolean approved, boolean remember) {
        try {
            if (approved && remember) {
                approvalService.grant(pending.conversationId(), pending.calls());
            }
            approvalService.putDecision(pending.turnId(), approved);

            // 该会话在等待确认期间不允许发起新请求，因此最新检查点就是
            // “暂停在 tools 前”的那个快照，按 threadId 恢复即可。
            RunnableConfig config = RunnableConfig.builder()
                    .threadId(pending.conversationId())
                    .build();
            advanceUntilBlocked(compiledGraph.invoke(Map.of(), config), config,
                    pending.conversationId(), pending.turnId());
        } catch (Exception e) {
            approvalService.consumeDecision(pending.turnId());
            log.warn("确认后继续执行失败", e);
            agentEventBus.emit(pending.turnId(), new AgentEvent.Error(AgentErrors.friendly(e)));
        } finally {
            agentEventBus.complete(pending.turnId());
        }
    }

    /**
     * 处理 Graph 在 tools 节点前的暂停。
     *
     * <p>如果该会话已经授权了全部待执行工具，就直接补一个“允许”决定并继续跑；
     * 否则生成确认请求推给前端，本轮结束。用户决定后由 {@link #decide} 恢复。</p>
     */
    private Outcome advanceUntilBlocked(Optional<OverAllState> initial, RunnableConfig config,
                                        String conversationId, String fallbackTurnId) {
        Optional<OverAllState> result = initial;
        for (int round = 0; round <= maxIterations; round++) {
            if (result.isEmpty() || !awaitingApproval(result.get())) {
                emitCheckpoint(config, turnIdOf(result.orElse(null), fallbackTurnId));
                return new Outcome(result, null);
            }

            OverAllState state = result.get();
            String turnId = turnIdOf(state, fallbackTurnId);
            List<AgentEvent.ApprovalCall> calls = approvalCalls(state);
            if (calls.isEmpty()) {
                log.warn("Graph 在 tools 前暂停，但 state 中没有待执行的工具调用，按结束处理");
                emitCheckpoint(config, turnId);
                return new Outcome(result, null);
            }

            if (approvalService.isFullyGranted(conversationId, calls)) {
                approvalService.putDecision(turnId, true);
                result = compiledGraph.invoke(Map.of(), config);
                continue;
            }

            AgentEvent.Approval approval = approvalService.create(conversationId, turnId, calls);
            agentEventBus.emit(turnId, approval);
            emitCheckpoint(config, turnId);
            return new Outcome(result, approval);
        }
        throw new IllegalStateException("工具确认循环超过最大轮数（" + maxIterations + "），已终止");
    }

    private boolean awaitingApproval(OverAllState state) {
        return NODE_TOOLS.equals(readText(state, "next", ""));
    }

    private List<AgentEvent.ApprovalCall> approvalCalls(OverAllState state) {
        List<AgentEvent.ApprovalCall> calls = new ArrayList<>();
        for (Map<String, Object> call : pendingToolCalls(readTranscript(state))) {
            String name = asText(call.get("name"));
            calls.add(new AgentEvent.ApprovalCall(
                    asText(call.get("id")),
                    name,
                    labelOf(name),
                    asText(call.getOrDefault("arguments", "{}"))));
        }
        return calls;
    }

    private static String labelOf(String name) {
        return TOOL_LABELS.getOrDefault(name, name);
    }

    private String turnIdOf(OverAllState state, String fallback) {
        String safeFallback = fallback == null ? "" : fallback;
        if (state == null) {
            return safeFallback;
        }
        String turnId = readText(state, "turnId", safeFallback);
        return turnId == null || turnId.isBlank() ? safeFallback : turnId;
    }

    private void emitCheckpoint(RunnableConfig config, String turnId) {
        if (turnId == null || turnId.isBlank()) {
            return;
        }
        latestCheckpoint(config).ifPresent(checkpointId ->
                agentEventBus.emit(turnId, new AgentEvent.Checkpoint(checkpointId)));
    }

    private record Outcome(Optional<OverAllState> state, AgentEvent.Approval approval) {
    }

    private Optional<String> latestCheckpoint(RunnableConfig config) {
        try {
            Collection<StateSnapshot> history = compiledGraph.getStateHistory(config);
            if (history == null || history.isEmpty()) {
                return Optional.empty();
            }
            return history.stream()
                    .findFirst()
                    .flatMap(snapshot -> snapshot.config().checkPointId());
        } catch (Exception e) {
            log.debug("读取最新检查点失败：{}", e.toString());
            return Optional.empty();
        }
    }

    private Map<String, Object> inputs(String message, String conversationId, String turnId) {
        Map<String, Object> inputs = new HashMap<>();
        inputs.put("conversationId", conversationId);
        inputs.put("turnId", turnId);
        inputs.put("userMessage", message);
        return inputs;
    }

    /**
     * 节点一：检索知识库、组装 system prompt，并把历史对话与当前问题放进本轮 transcript。
     */
    private Map<String, Object> prepare(OverAllState state) {
        String message = readText(state, "userMessage", "");
        String conversationId = readText(state, "conversationId", "default");
        String turnId = readText(state, "turnId", "");
        RagContext rag = retrieve(message);

        if (!rag.sources().isEmpty()) {
            agentEventBus.emit(turnId, new AgentEvent.Sources(rag.sources()));
        }

        List<Map<String, Object>> transcript = new ArrayList<>(historyTranscript(conversationId));
        transcript.add(userEntry(message));

        Map<String, Object> updates = new LinkedHashMap<>();
        updates.put("systemPrompt", systemPrompt(rag));
        updates.put("sources", sourceMaps(rag.sources()));
        updates.put("transcript", transcript);
        updates.put("iterations", 0);
        updates.put("next", NODE_AGENT);
        updates.put("lastNode", NODE_PREPARE);
        return updates;
    }

    /**
     * 节点二：一次模型调用。有 tool_calls 就交给 tools 节点，没有就是最终回答。
     */
    private Map<String, Object> agent(OverAllState state) {
        String conversationId = readText(state, "conversationId", "default");
        String turnId = readText(state, "turnId", "");
        String userMessage = readText(state, "userMessage", "");
        String systemPrompt = readText(state, "systemPrompt", BASE_SYSTEM_PROMPT);
        int iterations = readInt(state, "iterations", 0);
        List<Map<String, Object>> transcript = readTranscript(state);

        Map<String, Object> updates = new LinkedHashMap<>();
        if (iterations >= maxIterations) {
            String answer = "已达到最大工具调用轮数（" + maxIterations + "），请把问题拆小一些再试。";
            finishTurn(conversationId, turnId, userMessage, answer, transcript, updates);
            return updates;
        }

        Prompt prompt = new Prompt(toMessages(systemPrompt, transcript), options(turnId, conversationId));
        ChatResponse response = chatModel.call(prompt);
        if (response == null || response.getResult() == null) {
            throw new IllegalStateException("模型没有返回结果");
        }

        AssistantMessage assistant = response.getResult().getOutput();
        String assistantText = assistant.getText() == null ? "" : assistant.getText();
        List<AssistantMessage.ToolCall> toolCalls = assistant.getToolCalls() == null
                ? List.of()
                : assistant.getToolCalls();

        transcript.add(assistantEntry(assistantText, toolCalls));

        if (toolCalls.isEmpty()) {
            String answer = assistantText.isBlank() ? "（模型没有返回内容）" : assistantText;
            finishTurn(conversationId, turnId, userMessage, answer, transcript, updates);
        } else {
            updates.put("transcript", transcript);
            updates.put("iterations", iterations + 1);
            updates.put("next", NODE_TOOLS);
            updates.put("lastNode", NODE_AGENT);
        }
        return updates;
    }

    /**
     * 节点三：执行上一步模型请求的所有工具，把结果写回 transcript，再回到 agent。
     */
    private Map<String, Object> tools(OverAllState state) {
        String conversationId = readText(state, "conversationId", "default");
        String turnId = readText(state, "turnId", "");
        List<Map<String, Object>> transcript = readTranscript(state);
        List<Map<String, Object>> pendingCalls = pendingToolCalls(transcript);

        // 决定只消费一次，作用于本批全部工具调用；缺失时按拒绝处理，避免绕过确认。
        Boolean approved = approvalService.consumeDecision(turnId);
        boolean allowed = Boolean.TRUE.equals(approved);
        if (approved == null) {
            log.warn("tools 节点没有拿到确认结果，按拒绝处理（turnId={}）", turnId);
        }

        for (Map<String, Object> call : pendingCalls) {
            String id = asText(call.get("id"));
            String name = asText(call.get("name"));
            String arguments = asText(call.getOrDefault("arguments", "{}"));

            ToolCallback callback = callbacksByName.get(name);
            String result;
            if (callback == null) {
                result = "未找到工具：" + name;
            } else if (!allowed) {
                result = "用户拒绝了这次工具调用（" + labelOf(name) + "），操作未执行。"
                        + "请根据用户的选择调整回答，不要假装已经执行。";
            } else {
                try {
                    result = callback.call(arguments, new ToolContext(Map.of(
                            "turnId", turnId,
                            "conversationId", conversationId)));
                } catch (Exception e) {
                    result = "工具执行失败：" + e.getMessage();
                }
            }
            transcript.add(toolEntry(id, name, result == null ? "" : result));
        }

        Map<String, Object> updates = new LinkedHashMap<>();
        updates.put("transcript", transcript);
        updates.put("next", NODE_AGENT);
        updates.put("lastNode", NODE_TOOLS);
        return updates;
    }

    /**
     * 工具调用参数由模型生成，这里关闭 Spring AI 的自动执行，交给 Graph 的 tools 节点逐步执行。
     * Spring AI 1.1 中 ChatModel 只负责请求/响应，工具循环由 ChatClient 的 ToolCallingAdvisor 驱动。
     */
    private OpenAiChatOptions options(String turnId, String conversationId) {
        return OpenAiChatOptions.builder()
                .model(modelName)
                .temperature(temperature)
                .toolCallbacks(toolCallbacks)
                .toolContext(Map.of(
                        "turnId", turnId,
                        "conversationId", conversationId))
                .build();
    }

    private void finishTurn(String conversationId, String turnId, String userMessage, String answer,
                            List<Map<String, Object>> transcript, Map<String, Object> updates) {
        List<Message> memoryMessages = new ArrayList<>();
        memoryMessages.add(new UserMessage(userMessage));
        memoryMessages.add(AssistantMessage.builder().content(answer).build());
        chatMemory.add(conversationId, memoryMessages);

        emitAnswer(turnId, answer);

        updates.put("transcript", transcript);
        updates.put("answer", answer);
        updates.put("next", ROUTE_END);
        updates.put("lastNode", NODE_AGENT);
    }

    private void emitAnswer(String turnId, String answer) {
        int chunkSize = 48;
        for (int start = 0; start < answer.length(); start += chunkSize) {
            int end = Math.min(answer.length(), start + chunkSize);
            agentEventBus.emit(turnId, new AgentEvent.Delta(answer.substring(start, end)));
        }
    }

    private List<Message> toMessages(String systemPrompt, List<Map<String, Object>> transcript) {
        List<Message> messages = new ArrayList<>();
        messages.add(new SystemMessage(systemPrompt));

        for (Map<String, Object> entry : transcript) {
            String role = asText(entry.get("role"));
            if ("assistant".equals(role)) {
                messages.add(AssistantMessage.builder()
                        .content(asText(entry.get("text")))
                        .toolCalls(toolCallsOf(entry))
                        .build());
            } else if ("tool".equals(role)) {
                messages.add(ToolResponseMessage.builder()
                        .responses(List.of(new ToolResponseMessage.ToolResponse(
                                asText(entry.get("toolCallId")),
                                asText(entry.get("name")),
                                asText(entry.get("result")))))
                        .build());
            } else {
                messages.add(new UserMessage(asText(entry.get("text"))));
            }
        }
        return messages;
    }

    private List<AssistantMessage.ToolCall> toolCallsOf(Map<String, Object> entry) {
        List<AssistantMessage.ToolCall> calls = new ArrayList<>();
        Object raw = entry.get("toolCalls");
        if (!(raw instanceof List<?> list)) {
            return calls;
        }
        for (Object item : list) {
            if (!(item instanceof Map<?, ?> map)) {
                continue;
            }
            // Map<?, ?> 的 getOrDefault 第二个参数是捕获类型，不能直接传 String，这里手动补默认值。
            Object type = map.get("type");
            Object arguments = map.get("arguments");
            calls.add(new AssistantMessage.ToolCall(
                    asText(map.get("id")),
                    type == null ? "function" : asText(type),
                    asText(map.get("name")),
                    arguments == null ? "{}" : asText(arguments)));
        }
        return calls;
    }

    private List<Map<String, Object>> pendingToolCalls(List<Map<String, Object>> transcript) {
        for (int index = transcript.size() - 1; index >= 0; index--) {
            Map<String, Object> entry = transcript.get(index);
            if (!"assistant".equals(asText(entry.get("role")))) {
                continue;
            }
            List<Map<String, Object>> calls = new ArrayList<>();
            Object raw = entry.get("toolCalls");
            if (raw instanceof List<?> list) {
                for (Object item : list) {
                    if (item instanceof Map<?, ?> map) {
                        Map<String, Object> call = new LinkedHashMap<>();
                        map.forEach((key, value) -> call.put(String.valueOf(key), value));
                        calls.add(call);
                    }
                }
            }
            return calls;
        }
        return List.of();
    }

    private List<Map<String, Object>> historyTranscript(String conversationId) {
        List<Map<String, Object>> transcript = new ArrayList<>();
        for (Message message : chatMemory.get(conversationId)) {
            if (message instanceof UserMessage) {
                transcript.add(userEntry(asText(message.getText())));
            } else if (message instanceof AssistantMessage assistant) {
                transcript.add(assistantEntry(asText(assistant.getText()), List.of()));
            }
        }
        return transcript;
    }

    private Map<String, Object> userEntry(String text) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("role", "user");
        entry.put("text", text == null ? "" : text);
        return entry;
    }

    private Map<String, Object> assistantEntry(String text, List<AssistantMessage.ToolCall> calls) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("role", "assistant");
        entry.put("text", text == null ? "" : text);

        List<Map<String, Object>> toolCalls = new ArrayList<>();
        for (AssistantMessage.ToolCall call : calls) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", call.id());
            item.put("type", call.type());
            item.put("name", call.name());
            item.put("arguments", call.arguments());
            toolCalls.add(item);
        }
        entry.put("toolCalls", toolCalls);
        return entry;
    }

    private Map<String, Object> toolEntry(String id, String name, String result) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("role", "tool");
        entry.put("toolCallId", id);
        entry.put("name", name);
        entry.put("result", result);
        return entry;
    }

    private List<Map<String, Object>> readTranscript(OverAllState state) {
        List<Map<String, Object>> transcript = new ArrayList<>();
        Object raw = state.value("transcript", List.of());
        if (!(raw instanceof List<?> list)) {
            return transcript;
        }
        for (Object item : list) {
            if (item instanceof Map<?, ?> map) {
                Map<String, Object> entry = new LinkedHashMap<>();
                map.forEach((key, value) -> entry.put(String.valueOf(key), value));
                transcript.add(entry);
            }
        }
        return transcript;
    }

    private String readText(OverAllState state, String key, String fallback) {
        Object value = state.value(key, fallback);
        return value == null ? fallback : String.valueOf(value);
    }

    private int readInt(OverAllState state, String key, int fallback) {
        Object value = state.value(key, fallback);
        return value instanceof Number number ? number.intValue() : fallback;
    }

    private static String asText(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private ChatResult toChatResult(Outcome outcome) {
        if (outcome.state().isEmpty()) {
            return new ChatResult("", List.of(), outcome.approval());
        }
        OverAllState state = outcome.state().get();
        String answer = readText(state, "answer", "");
        Object sources = state.value("sources", List.of());
        return new ChatResult(answer, extractSources(sources), outcome.approval());
    }

    private List<AgentEvent.Source> extractSources(Object raw) {
        if (!(raw instanceof List<?> list)) {
            return List.of();
        }
        List<AgentEvent.Source> sources = new ArrayList<>();
        for (Object item : list) {
            if (item instanceof AgentEvent.Source source) {
                sources.add(source);
            } else if (item instanceof Map<?, ?> map) {
                int index = map.get("index") instanceof Number number ? number.intValue() : sources.size() + 1;
                Object source = map.get("source");
                Object snippet = map.get("snippet");
                sources.add(new AgentEvent.Source(
                        index,
                        source == null ? "unknown.md" : String.valueOf(source),
                        snippet == null ? "" : String.valueOf(snippet)));
            }
        }
        return List.copyOf(sources);
    }

    private List<Map<String, Object>> sourceMaps(List<AgentEvent.Source> sources) {
        List<Map<String, Object>> maps = new ArrayList<>();
        for (AgentEvent.Source source : sources) {
            Map<String, Object> map = new HashMap<>();
            map.put("index", source.index());
            map.put("source", source.source());
            map.put("snippet", source.snippet());
            maps.add(map);
        }
        return maps;
    }

    private String systemPrompt(RagContext rag) {
        if (rag.context().isBlank()) {
            return BASE_SYSTEM_PROMPT;
        }
        return BASE_SYSTEM_PROMPT + """

                【知识库参考资料】
                %s
                请优先使用以上资料回答，并在相关句子末尾标注来源编号，例如 [1]。
                如果资料不足以回答，请明确说明，再给出基于通用知识的建议。
                """.formatted(rag.context());
    }

    private RagContext retrieve(String message) {
        List<Document> documents = knowledgeBase.search(message);
        if (documents.isEmpty()) {
            return RagContext.EMPTY;
        }

        StringBuilder context = new StringBuilder();
        List<AgentEvent.Source> sources = new ArrayList<>();
        int index = 1;
        for (Document document : documents) {
            String text = document.getText() == null ? "" : document.getText();
            Object source = document.getMetadata().getOrDefault("source", "unknown.md");

            context.append('[').append(index).append("] 来源：").append(source).append('\n')
                    .append(abbreviate(text, 1200))
                    .append("\n\n");

            sources.add(new AgentEvent.Source(index, String.valueOf(source), snippet(text)));
            index++;
        }
        return new RagContext(context.toString(), List.copyOf(sources));
    }

    private static String abbreviate(String text, int maxLength) {
        String normalized = text.strip();
        return normalized.length() <= maxLength ? normalized : normalized.substring(0, maxLength) + "…";
    }

    private static String snippet(String text) {
        String normalized = text.replaceAll("\\s+", " ").strip();
        return normalized.length() <= 80 ? normalized : normalized.substring(0, 80) + "…";
    }

    private record RagContext(String context, List<AgentEvent.Source> sources) {
        static final RagContext EMPTY = new RagContext("", List.of());
    }
}
