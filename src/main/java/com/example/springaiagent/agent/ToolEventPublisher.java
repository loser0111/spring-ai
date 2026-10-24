package com.example.springaiagent.agent;

import org.springframework.ai.chat.model.ToolContext;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * 给工具方法用的轻量埋点辅助类。
 * turnId 由 Graph 通过 ToolContext 传入，不会发送给大模型。
 */
@Component
public class ToolEventPublisher {

    public static final String TURN_ID = "turnId";
    public static final String CONVERSATION_ID = "conversationId";

    private final ToolEventBus eventBus;

    public ToolEventPublisher(ToolEventBus eventBus) {
        this.eventBus = eventBus;
    }

    public ToolCall start(ToolContext context, String name, String label, String input) {
        ToolCall call = new ToolCall(turnId(context), UUID.randomUUID().toString(), name, label);
        publish(call, "start", input, null);
        return call;
    }

    public void end(ToolCall call, String output) {
        publish(call, "end", null, output);
    }

    public void error(ToolCall call, String message) {
        publish(call, "error", null, message);
    }

    public String conversationId(ToolContext context) {
        Object value = lookup(context, CONVERSATION_ID);
        return value instanceof String id ? id : "default";
    }

    private void publish(ToolCall call, String phase, String input, String output) {
        if (call == null || call.turnId() == null) {
            return;
        }
        eventBus.emit(call.turnId(), new ToolEvent(
                call.id(), call.name(), call.label(), phase, input, output));
    }

    private String turnId(ToolContext context) {
        Object value = lookup(context, TURN_ID);
        return value instanceof String id ? id : null;
    }

    private Object lookup(ToolContext context, String key) {
        if (context == null || context.getContext() == null) {
            return null;
        }
        return context.getContext().get(key);
    }
}
