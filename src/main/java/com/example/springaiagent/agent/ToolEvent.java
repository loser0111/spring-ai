package com.example.springaiagent.agent;

/**
 * 一次工具调用产生的可视化事件。
 *
 * @param id     本次工具调用的唯一 ID，用于把 start / end 两个事件对应起来
 * @param name   工具名（模型侧的函数名）
 * @param label  给用户看的中文名
 * @param phase  start / end / error
 * @param input  入参（JSON 字符串）
 * @param output 出参或错误信息
 */
public record ToolEvent(
        String id,
        String name,
        String label,
        String phase,
        String input,
        String output
) {
}
