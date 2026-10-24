package com.example.springaiagent.agent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 一轮对话内 Agent 事件（来源、文本增量、检查点、错误）的传输通道。
 * 使用 unicast sink：Graph 在后台线程启动时可能先于 HTTP 订阅产生事件，sink 会先缓存。
 */
@Component
public class AgentEventBus {

    private static final Logger log = LoggerFactory.getLogger(AgentEventBus.class);

    private final Map<String, Sinks.Many<AgentEvent>> sinks = new ConcurrentHashMap<>();

    public void open(String turnId) {
        sinks.put(turnId, Sinks.many().unicast().onBackpressureBuffer());
    }

    public Flux<AgentEvent> events(String turnId) {
        Sinks.Many<AgentEvent> sink = sinks.get(turnId);
        return sink == null ? Flux.empty() : sink.asFlux();
    }

    public void emit(String turnId, AgentEvent event) {
        Sinks.Many<AgentEvent> sink = sinks.get(turnId);
        if (sink == null) {
            return;
        }
        Sinks.EmitResult result = sink.tryEmitNext(event);
        if (result.isFailure()) {
            log.debug("Agent 事件推送失败: {} ({})", result, turnId);
        }
    }

    public void complete(String turnId) {
        Sinks.Many<AgentEvent> sink = sinks.get(turnId);
        if (sink != null) {
            sink.tryEmitComplete();
        }
    }

    public void remove(String turnId) {
        sinks.remove(turnId);
    }
}
