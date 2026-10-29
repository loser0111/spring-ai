package com.example.springaiagent.approval;

import com.example.springaiagent.agent.AgentEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 工具执行前的人工确认。
 *
 * <p>三份内存数据：</p>
 * <ul>
 *   <li>pending：等待用户决定的确认请求，approvalId -> 请求，带 TTL；</li>
 *   <li>grants：会话级授权，conversationId -> 已授权的工具名；</li>
 *   <li>decisions：一次性的用户决定，turnId -> 是否允许，由 tools 节点消费。</li>
 * </ul>
 *
 * <p>决定不放进 Graph state，而是放在服务端：恢复执行时 tools 节点直接消费，
 * 避免依赖框架对 resume 入参的合并行为。代价是应用重启后未处理的确认会丢失，
 * 这和 MemorySaver 的语义一致。</p>
 */
@Service
public class ApprovalService {

    /**
     * 一个等待用户决定的确认请求。
     */
    public record Pending(String approvalId, String conversationId, String turnId,
                          List<AgentEvent.ApprovalCall> calls, long expiresAt) {
    }

    /**
     * 过期的确认请求不会立刻删除：保留一段时间，等用户点确认时按“拒绝”收尾，
     * 避免 Graph 永远停在 tools 前、该会话后续消息无法正常开始新一轮。
     */
    private static final long EXPIRE_GRACE_MILLIS = 60 * 60 * 1000L;

    private final Map<String, Pending> pending = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> grants = new ConcurrentHashMap<>();
    private final Map<String, Boolean> decisions = new ConcurrentHashMap<>();
    private final long ttlMillis;

    public ApprovalService(@Value("${app.approval.ttl-seconds:600}") long ttlSeconds) {
        this.ttlMillis = Math.max(ttlSeconds, 30) * 1000L;
    }

    /**
     * 创建确认请求，返回可直接推给前端的事件。
     */
    public AgentEvent.Approval create(String conversationId, String turnId,
                                      List<AgentEvent.ApprovalCall> calls) {
        cleanupExpired();
        String approvalId = UUID.randomUUID().toString();
        long expiresAt = System.currentTimeMillis() + ttlMillis;
        List<AgentEvent.ApprovalCall> snapshot = List.copyOf(calls);
        pending.put(approvalId,
                new Pending(approvalId, conversationId, turnId, snapshot, expiresAt));
        return new AgentEvent.Approval(approvalId, conversationId, turnId, expiresAt, snapshot);
    }

    /**
     * 查询未过期的确认请求，不会消费。
     */
    public Optional<Pending> find(String approvalId) {
        cleanupExpired();
        return Optional.ofNullable(pending.get(approvalId));
    }

    /**
     * 取出并删除确认请求，保证一次决定只能使用一次。
     */
    public Optional<Pending> consumePending(String approvalId) {
        cleanupExpired();
        return Optional.ofNullable(pending.remove(approvalId));
    }

    public boolean hasPending(String conversationId) {
        cleanupExpired();
        return pending.values().stream()
                .anyMatch(item -> item.conversationId().equals(conversationId));
    }

    /**
     * 该会话是否已经授权了本次全部工具调用。任一工具未授权都需要再次确认。
     */
    public boolean isFullyGranted(String conversationId, List<AgentEvent.ApprovalCall> calls) {
        if (calls.isEmpty()) {
            return false;
        }
        Set<String> granted = grants.get(conversationId);
        if (granted == null || granted.isEmpty()) {
            return false;
        }
        return calls.stream().allMatch(call -> granted.contains(call.name()));
    }

    /**
     * 记住“本次会话内允许这些工具”。
     */
    public void grant(String conversationId, List<AgentEvent.ApprovalCall> calls) {
        Set<String> granted = grants.computeIfAbsent(conversationId,
                key -> ConcurrentHashMap.newKeySet());
        calls.forEach(call -> granted.add(call.name()));
    }

    public void putDecision(String turnId, boolean approved) {
        decisions.put(turnId, approved);
    }

    /**
     * tools 节点执行前消费本轮决定；没有决定时返回 null（调用方按拒绝处理）。
     */
    public Boolean consumeDecision(String turnId) {
        return decisions.remove(turnId);
    }

    private void cleanupExpired() {
        long now = System.currentTimeMillis();
        pending.entrySet().removeIf(entry ->
                entry.getValue().expiresAt() + EXPIRE_GRACE_MILLIS < now);
    }
}
