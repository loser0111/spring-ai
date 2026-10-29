package com.example.springaiagent.agent;

import java.util.List;

/**
 * Agent 流式输出的事件类型。
 */
public sealed interface AgentEvent permits AgentEvent.Delta, AgentEvent.Sources,
        AgentEvent.Checkpoint, AgentEvent.Error, AgentEvent.Approval {

    record Delta(String text) implements AgentEvent {
    }

    record Sources(List<Source> sources) implements AgentEvent {
    }

    /**
     * 本轮执行完成后由 checkpoint saver 生成的最新检查点。
     */
    record Checkpoint(String checkpointId) implements AgentEvent {
    }

    record Error(String message) implements AgentEvent {
    }

    /**
     * 工具执行前的确认请求。前端据此弹窗，用户决定后调用
     * POST /api/approvals/{approvalId} 从检查点继续执行。
     *
     * @param approvalId     本次确认的唯一 ID，一次性使用
     * @param conversationId 会话 ID
     * @param turnId         对应的一轮对话 ID
     * @param expiresAt      过期时间（epoch millis）
     * @param calls          等待确认的工具调用
     */
    record Approval(String approvalId, String conversationId, String turnId,
                    long expiresAt, List<ApprovalCall> calls) implements AgentEvent {
    }

    /**
     * 一条等待确认的工具调用。
     */
    record ApprovalCall(String id, String name, String label, String input) {
    }

    /**
     * 一条知识库引用。
     *
     * @param index   引用编号，与回答里的 [1] 对应
     * @param source  文档文件名
     * @param snippet 片段摘要
     */
    record Source(int index, String source, String snippet) {
    }
}
