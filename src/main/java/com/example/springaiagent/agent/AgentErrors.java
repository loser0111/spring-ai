package com.example.springaiagent.agent;

import java.util.Locale;

/**
 * 把底层模型/网络异常转成页面上可读的提示。
 */
public final class AgentErrors {

    private AgentErrors() {
    }

    public static String friendly(Throwable error) {
        String raw = error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage();
        String lower = raw.toLowerCase(Locale.ROOT);

        if (lower.contains("401") || lower.contains("unauthorized")
                || lower.contains("invalid_api_key") || lower.contains("api key")) {
            return "模型鉴权失败，请检查 OPENAI_API_KEY、OPENAI_BASE_URL 是否正确。原始错误：" + abbreviate(raw);
        }
        if (lower.contains("unknownhost") || lower.contains("connection refused")
                || lower.contains("timeout") || lower.contains("connect timed out")) {
            return "无法连接模型服务，请检查网络和 OPENAI_BASE_URL。原始错误：" + abbreviate(raw);
        }
        if (lower.contains("does not support tools") || lower.contains("tool_calls")
                || lower.contains("function calling")) {
            return "当前模型可能不支持工具调用，请换用支持 Function Calling 的模型。原始错误：" + abbreviate(raw);
        }
        return "对话失败：" + abbreviate(raw);
    }

    private static String abbreviate(String message) {
        return message.length() <= 400 ? message : message.substring(0, 400) + "…";
    }
}
