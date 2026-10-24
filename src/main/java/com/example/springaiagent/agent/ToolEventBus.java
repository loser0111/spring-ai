package com.example.springaiagent.agent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 按「一轮对话」收集工具调用事件：
 * - 流式接口订阅 sink，把事件实时推给前端；
 * - 同步接口直接读 history，随响应一起返回。
 */
@Component
public class ToolEventBus {

    private static final Logger log = LoggerFactory.getLogger(ToolEventBus.class);

    private final Map<String, Session> sessions = new ConcurrentHashMap<>();

    public void open(String turnId) {
        sessions.put(turnId, new Session(
                Sinks.many().multicast().onBackpressureBuffer(64),
                new CopyOnWriteArrayList<>()));
    }

    public Flux<ToolEvent> events(String turnId) {
        Session session = sessions.get(turnId);
        return session == null ? Flux.empty() : session.sink().asFlux();
    }

    public void emit(String turnId, ToolEvent event) {
        Session session = sessions.get(turnId);
        if (session == null) {
            return;
        }
        session.history().add(event);
        Sinks.EmitResult result = session.sink().tryEmitNext(event);
        if (result.isFailure()) {
            log.debug("工具事件推送失败: {} ({})", result, turnId);
        }
    }

    /**
     * 让对应的 Flux 正常结束。流式响应结束后必须调用，否则 merge 会一直等待。
     */
    public void complete(String turnId) {
        Session session = sessions.get(turnId);
        if (session != null) {
            session.sink().tryEmitComplete();
        }
    }

    public List<ToolEvent> history(String turnId) {
        Session session = sessions.get(turnId);
        return session == null ? List.of() : List.copyOf(session.history());
    }

    public void remove(String turnId) {
        sessions.remove(turnId);
    }

    private record Session(Sinks.Many<ToolEvent> sink, List<ToolEvent> history) {
    }
}
