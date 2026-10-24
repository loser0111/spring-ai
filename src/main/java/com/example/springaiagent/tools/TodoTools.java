package com.example.springaiagent.tools;

import com.example.springaiagent.agent.ToolCall;
import com.example.springaiagent.agent.ToolEventPublisher;
import org.springframework.ai.chat.model.ToolContext;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 待办工具：演示「工具带状态」以及按会话隔离数据（conversationId 由 ToolContext 传入）。
 */
@Component
public class TodoTools {

    private final Map<String, List<String>> todos = new ConcurrentHashMap<>();
    private final ToolEventPublisher events;

    public TodoTools(ToolEventPublisher events) {
        this.events = events;
    }

    @Tool(description = "向当前会话的待办清单添加一条事项。")
    public String addTodo(
            @ToolParam(description = "待办事项内容") String item,
            ToolContext context) {
        String text = item == null ? "" : item.trim();
        String input = "{\"item\":\"" + text.replace("\"", "'") + "\"}";
        ToolCall call = events.start(context, "addTodo", "添加待办", input);

        if (text.isBlank()) {
            events.error(call, "待办内容为空");
            return "待办内容不能为空。";
        }

        List<String> list = listOf(context);
        list.add(text);
        String output = "已添加待办：" + text + "（当前共 " + list.size() + " 条）";
        events.end(call, output);
        return output;
    }

    @Tool(description = "查看当前会话的全部待办事项。")
    public String listTodos(ToolContext context) {
        ToolCall call = events.start(context, "listTodos", "查看待办", "{}");
        List<String> list = listOf(context);
        if (list.isEmpty()) {
            String output = "当前待办清单为空。";
            events.end(call, output);
            return output;
        }
        StringBuilder sb = new StringBuilder("当前待办：\n");
        for (int i = 0; i < list.size(); i++) {
            sb.append(i + 1).append(". ").append(list.get(i)).append('\n');
        }
        String output = sb.toString().trim();
        events.end(call, output);
        return output;
    }

    @Tool(description = "把当前会话中的某条待办标记为完成并移除。")
    public String completeTodo(
            @ToolParam(description = "要完成的待办事项内容，需要与清单中的文字一致") String item,
            ToolContext context) {
        String text = item == null ? "" : item.trim();
        String input = "{\"item\":\"" + text.replace("\"", "'") + "\"}";
        ToolCall call = events.start(context, "completeTodo", "完成待办", input);

        List<String> list = listOf(context);
        boolean removed = list.removeIf(todo -> todo.equalsIgnoreCase(text));
        String output = removed
                ? "已完成并移除待办：" + text
                : "没有找到待办：" + text + "，可以先用 list_todos 查看清单。";
        events.end(call, output);
        return output;
    }

    private List<String> listOf(ToolContext context) {
        String conversationId = events.conversationId(context);
        return todos.computeIfAbsent(conversationId, key -> new CopyOnWriteArrayList<>());
    }
}
