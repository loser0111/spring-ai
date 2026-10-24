package com.example.springaiagent.web;

public record ChatRequest(String conversationId, String message) {

    public String safeConversationId() {
        if (conversationId == null || conversationId.isBlank()) {
            return "default";
        }
        return conversationId.trim();
    }

    public String safeMessage() {
        return message == null ? "" : message.trim();
    }
}
