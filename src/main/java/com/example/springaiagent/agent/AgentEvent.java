package com.example.springaiagent.agent;

import java.util.List;

/**
 * Agent 流式输出的事件类型。
 */
public sealed interface AgentEvent permits AgentEvent.Delta, AgentEvent.Sources,
        AgentEvent.Checkpoint, AgentEvent.Error {

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
     * 一条知识库引用。
     *
     * @param index   引用编号，与回答里的 [1] 对应
     * @param source  文档文件名
     * @param snippet 片段摘要
     */
    record Source(int index, String source, String snippet) {
    }
}
