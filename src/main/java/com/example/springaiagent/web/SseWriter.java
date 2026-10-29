package com.example.springaiagent.web;

import com.example.springaiagent.agent.AgentEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 统一把 AgentEvent / 任意 payload 转成 SSE 事件，供对话和确认两个接口复用。
 */
@Component
public class SseWriter {

    private final ObjectMapper objectMapper;

    public SseWriter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public ServerSentEvent<String> event(String event, Object payload) {
        String json;
        try {
            json = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            json = "{\"message\":\"事件序列化失败\"}";
        }
        return ServerSentEvent.<String>builder()
                .event(event)
                .data(json)
                .build();
    }

    public ServerSentEvent<String> agent(AgentEvent event) {
        if (event instanceof AgentEvent.Delta delta) {
            return event("delta", Map.of("text", delta.text()));
        }
        if (event instanceof AgentEvent.Sources sources) {
            return event("sources", Map.of("sources", sources.sources()));
        }
        if (event instanceof AgentEvent.Checkpoint checkpoint) {
            return event("checkpoint", Map.of("checkpointId", checkpoint.checkpointId()));
        }
        if (event instanceof AgentEvent.Approval approval) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("approvalId", approval.approvalId());
            payload.put("conversationId", approval.conversationId());
            payload.put("turnId", approval.turnId());
            payload.put("expiresAt", approval.expiresAt());
            payload.put("calls", approval.calls());
            return event("approval", payload);
        }
        AgentEvent.Error error = (AgentEvent.Error) event;
        return event("error", Map.of("message", error.message()));
    }
}
