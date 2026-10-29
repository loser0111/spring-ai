package com.example.springaiagent.web;

import com.example.springaiagent.agent.AgentErrors;
import com.example.springaiagent.agent.AgentEvent;
import com.example.springaiagent.agent.AgentEventBus;
import com.example.springaiagent.agent.ToolEventBus;
import com.example.springaiagent.approval.ApprovalService;
import com.example.springaiagent.service.AgentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 工具执行前的人工确认接口。
 *
 * <p>确认请求由 Graph 在 tools 节点前暂停时生成，用户决定后这里从检查点恢复执行，
 * 并以 SSE 流式返回后续的工具事件和回答——和 /api/chat/stream 的事件格式一致，
 * 前端可以直接接着渲染。</p>
 */
@RestController
@RequestMapping("/api/approvals")
public class ApprovalController {

    private static final Logger log = LoggerFactory.getLogger(ApprovalController.class);

    private final AgentService agentService;
    private final ApprovalService approvalService;
    private final ToolEventBus toolEventBus;
    private final AgentEventBus agentEventBus;
    private final SseWriter sse;

    public ApprovalController(
            AgentService agentService,
            ApprovalService approvalService,
            ToolEventBus toolEventBus,
            AgentEventBus agentEventBus,
            SseWriter sse) {
        this.agentService = agentService;
        this.approvalService = approvalService;
        this.toolEventBus = toolEventBus;
        this.agentEventBus = agentEventBus;
        this.sse = sse;
    }

    @PostMapping(value = "/{approvalId}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> decide(
            @PathVariable String approvalId,
            @RequestBody(required = false) ApprovalDecisionRequest request) {
        boolean approved = request != null && request.isApproved();
        boolean remember = request != null && request.shouldRemember();

        Optional<ApprovalService.Pending> found = approvalService.find(approvalId);
        if (found.isEmpty()) {
            return Flux.just(sse.event("error",
                    Map.of("message", "确认请求不存在、已被处理或已过期，请重新发起对话。")));
        }

        ApprovalService.Pending pending = found.get();
        String turnId = pending.turnId();
        toolEventBus.open(turnId);

        Flux<ServerSentEvent<String>> toolEvents = toolEventBus.events(turnId)
                .map(event -> sse.event("tool", event));

        Flux<ServerSentEvent<String>> agentEvents = agentService.decide(approvalId, approved, remember)
                .onErrorResume(error -> {
                    log.warn("确认后继续执行失败", error);
                    return Flux.just(new AgentEvent.Error(AgentErrors.friendly(error)));
                })
                .map(sse::agent)
                .doFinally(signal -> toolEventBus.complete(turnId));

        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("conversationId", pending.conversationId());
        meta.put("turnId", turnId);
        meta.put("approvalId", approvalId);
        meta.put("approved", approved);

        return Flux.concat(
                        Flux.just(sse.event("meta", meta)),
                        Flux.merge(toolEvents, agentEvents),
                        Flux.just(sse.event("done", Map.of("turnId", turnId))))
                .doFinally(signal -> {
                    toolEventBus.remove(turnId);
                    agentEventBus.remove(turnId);
                });
    }
}
