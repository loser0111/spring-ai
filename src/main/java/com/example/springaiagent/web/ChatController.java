package com.example.springaiagent.web;

import com.example.springaiagent.agent.AgentErrors;
import com.example.springaiagent.agent.AgentEvent;
import com.example.springaiagent.agent.AgentEventBus;
import com.example.springaiagent.agent.ToolEventBus;
import com.example.springaiagent.service.AgentService;
import com.example.springaiagent.service.ChatResult;
import com.example.springaiagent.service.KnowledgeBaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.*;

@RestController
@RequestMapping("/api")
public class ChatController {

    private static final Logger log = LoggerFactory.getLogger(ChatController.class);

    private final AgentService agentService;
    private final ToolEventBus eventBus;
    private final AgentEventBus agentEventBus;
    private final KnowledgeBaseService knowledgeBase;
    private final SseWriter sse;
    private final String modelName;

    public ChatController(
            AgentService agentService,
            ToolEventBus eventBus,
            AgentEventBus agentEventBus,
            KnowledgeBaseService knowledgeBase,
            SseWriter sse,
            @Value("${spring.ai.openai.chat.options.model:unknown}") String modelName) {
        this.agentService = agentService;
        this.eventBus = eventBus;
        this.agentEventBus = agentEventBus;
        this.knowledgeBase = knowledgeBase;
        this.sse = sse;
        this.modelName = modelName;
    }

    /**
     * 流式对话，SSE 事件：meta / sources / tool / approval / delta / error / done。
     */
    @PostMapping(value = "/chat/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> stream(@RequestBody ChatRequest request) {
        String message = request.safeMessage();
        if (message.isBlank()) {
            return Flux.just(sse.event("error", Map.of("message", "消息不能为空")));
        }

        String conversationId = request.safeConversationId();
        if (agentService.hasPendingApproval(conversationId)) {
            return Flux.just(sse.event("error",
                    Map.of("message", "该会话还有未处理的工具确认请求，请先允许或拒绝。")));
        }

        String turnId = UUID.randomUUID().toString();
        eventBus.open(turnId);

        Flux<ServerSentEvent<String>> toolEvents = eventBus.events(turnId)
                .map(event -> sse.event("tool", event));

        Flux<ServerSentEvent<String>> agentEvents = agentService.stream(message, conversationId, turnId)
                .onErrorResume(error -> {
                    log.warn("对话调用失败", error);
                    return Flux.just(new AgentEvent.Error(AgentErrors.friendly(error)));
                })
                .map(sse::agent)
                // 先让工具事件流结束，merge 才能正常收尾
                .doFinally(signal -> eventBus.complete(turnId));

        Flux<ServerSentEvent<String>> body = Flux.merge(toolEvents, agentEvents);

        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("conversationId", conversationId);
        meta.put("turnId", turnId);
        meta.put("ragReady", knowledgeBase.isReady());

        return Flux.concat(
                        Flux.just(sse.event("meta", meta)),
                        body,
                        Flux.just(sse.event("done", Map.of("turnId", turnId))))
                .doFinally(signal -> {
                    eventBus.remove(turnId);
                    // 如果本轮停在工具确认上，先不要清掉 Agent 事件通道：
                    // 用户可能马上点确认，新的 SSE 连接会复用它。
                    if (!agentService.hasPendingApproval(conversationId)) {
                        agentEventBus.remove(turnId);
                    }
                });
    }

    /**
     * 非流式对话，返回完整回答、引用来源和工具调用记录。
     */
    @PostMapping("/chat")
    public Mono<ResponseEntity<Map<String, Object>>> chat(@RequestBody ChatRequest request) {
        String message = request.safeMessage();
        if (message.isBlank()) {
            ResponseEntity<Map<String, Object>> badRequest =
                    ResponseEntity.badRequest().body(errorBody("消息不能为空"));
            return Mono.just(badRequest);
        }

        String conversationId = request.safeConversationId();
        if (agentService.hasPendingApproval(conversationId)) {
            ResponseEntity<Map<String, Object>> conflict = ResponseEntity
                    .status(HttpStatus.CONFLICT)
                    .body(errorBody("该会话还有未处理的工具确认请求，请先允许或拒绝。"));
            return Mono.just(conflict);
        }
        String turnId = UUID.randomUUID().toString();
        eventBus.open(turnId);

        return Mono.fromCallable(() -> {
                    ChatResult result = agentService.chat(message, conversationId, turnId);
                    Map<String, Object> body = new LinkedHashMap<>();
                    body.put("conversationId", conversationId);
                    body.put("reply", result.reply());
                    body.put("sources", result.sources());
                    body.put("toolCalls", eventBus.history(turnId));
                    if (result.approval() != null) {
                        body.put("approval", result.approval());
                    }
                    return ResponseEntity.ok(body);
                })
                .subscribeOn(Schedulers.boundedElastic())
                .onErrorResume(error -> {
                    log.warn("对话调用失败", error);
                    ResponseEntity<Map<String, Object>> failed = ResponseEntity
                            .status(HttpStatus.BAD_GATEWAY)
                            .body(errorBody(AgentErrors.friendly(error)));
                    return Mono.just(failed);
                })
                .doFinally(signal -> {
                    eventBus.complete(turnId);
                    eventBus.remove(turnId);
                });
    }

    @GetMapping("/meta")
    public Map<String, Object> meta() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("model", modelName);
        body.put("ragReady", knowledgeBase.isReady());
        body.put("knowledgeChunks", knowledgeBase.chunkCount());
        body.put("knowledgeSources", knowledgeBase.sources());
        body.put("tools", List.of(
                tool("getCurrentDateTime", "查询当前时间", "获取日期、时间与星期"),
                tool("getTimeInZone", "查询时区时间", "按 IANA 时区查询当前时间"),
                tool("getWeather", "查询城市天气", "演示用的模拟天气数据"),
                tool("calculate", "计算表达式", "支持四则运算、括号与乘方"),
                tool("addTodo", "添加待办", "把事项加入当前会话清单"),
                tool("listTodos", "查看待办", "列出当前会话的全部待办"),
                tool("completeTodo", "完成待办", "标记并移除一条待办")));
        return body;
    }

    private Map<String, Object> tool(String name, String label, String description) {
        Map<String, Object> tool = new LinkedHashMap<>();
        tool.put("name", name);
        tool.put("label", label);
        tool.put("description", description);
        return tool;
    }

    private Map<String, Object> errorBody(String message) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", message);
        return body;
    }
}
