package com.example.springaiagent.web;

import com.example.springaiagent.agent.AgentErrors;
import com.example.springaiagent.service.AgentService;
import com.example.springaiagent.service.ChatResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/checkpoints")
public class CheckpointController {

    private static final Logger log = LoggerFactory.getLogger(CheckpointController.class);

    private final AgentService agentService;

    public CheckpointController(AgentService agentService) {
        this.agentService = agentService;
    }

    /**
     * 列出某个会话的检查点，最新的在前。
     */
    @GetMapping
    public Map<String, Object> list(@RequestParam String conversationId) {
        String threadId = normalize(conversationId);
        List<String> ids = agentService.checkpointIds(threadId);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("conversationId", threadId);
        body.put("count", ids.size());
        body.put("checkpoints", ids.stream()
                .map(id -> Map.of("checkpointId", id))
                .toList());
        return body;
    }

    /**
     * 从指定检查点回放。回放结果一次性返回，不做流式输出。
     */
    @PostMapping("/{checkpointId}/resume")
    public Mono<ResponseEntity<Map<String, Object>>> resume(
            @PathVariable String checkpointId,
            @RequestParam String conversationId) {
        String threadId = normalize(conversationId);

        return Mono.fromCallable(() -> {
                    ChatResult result = agentService.resume(threadId, checkpointId);
                    Map<String, Object> body = new LinkedHashMap<>();
                    body.put("conversationId", threadId);
                    body.put("checkpointId", checkpointId);
                    body.put("reply", result.reply());
                    body.put("sources", result.sources());
                    return ResponseEntity.ok(body);
                })
                .subscribeOn(Schedulers.boundedElastic())
                .onErrorResume(error -> {
                    log.warn("从检查点回放失败", error);
                    Map<String, Object> body = new LinkedHashMap<>();
                    body.put("error", AgentErrors.friendly(error));
                    ResponseEntity<Map<String, Object>> failed =
                            ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(body);
                    return Mono.just(failed);
                });
    }

    private String normalize(String conversationId) {
        return conversationId == null || conversationId.isBlank() ? "default" : conversationId.trim();
    }
}
