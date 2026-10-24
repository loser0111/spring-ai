package com.example.springaiagent.agent;

/**
 * 工具方法的句柄，由 ToolEventPublisher.start 返回，结束时再传回去。
 */
public record ToolCall(String turnId, String id, String name, String label) {
}
