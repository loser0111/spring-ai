package com.example.springaiagent.service;

import com.example.springaiagent.agent.AgentEvent;

import java.util.List;

public record ChatResult(String reply, List<AgentEvent.Source> sources, AgentEvent.Approval approval) {

    public ChatResult(String reply, List<AgentEvent.Source> sources) {
        this(reply, sources, null);
    }
}
