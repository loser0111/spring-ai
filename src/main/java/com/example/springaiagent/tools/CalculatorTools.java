package com.example.springaiagent.tools;

import com.example.springaiagent.agent.ToolCall;
import com.example.springaiagent.agent.ToolEventPublisher;
import org.springframework.ai.chat.model.ToolContext;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.regex.Pattern;

/**
 * 计算器工具：先做白名单校验，再交给 SpEL 求值，避免任意表达式执行。
 */
@Component
public class CalculatorTools {

    private static final Pattern ALLOWED = Pattern.compile("[0-9+\\-*/(). %^]+");
    private static final int MAX_LENGTH = 200;

    private final ExpressionParser parser = new SpelExpressionParser();
    private final ToolEventPublisher events;

    public CalculatorTools(ToolEventPublisher events) {
        this.events = events;
    }

    @Tool(description = "计算数学表达式，支持 + - * / % ^ 和括号，^ 表示乘方。用户要求做算术时调用。")
    public String calculate(
            @ToolParam(description = "数学表达式，例如 (128*3+56)/4 或 2^10") String expression,
            ToolContext context) {
        String input = "{\"expression\":\"" + (expression == null ? "" : expression.replace("\"", "'")) + "\"}";
        ToolCall call = events.start(context, "calculate", "计算表达式", input);

        if (expression == null || expression.isBlank()) {
            events.error(call, "表达式为空");
            return "表达式不能为空。";
        }
        String expr = expression.trim();
        if (expr.length() > MAX_LENGTH) {
            events.error(call, "表达式过长");
            return "表达式过长，请控制在 " + MAX_LENGTH + " 个字符以内。";
        }
        if (!ALLOWED.matcher(expr).matches()) {
            events.error(call, "包含不支持的字符");
            return "表达式只支持数字、空格、小数点以及 + - * / % ^ ( ) 这些符号。";
        }

        try {
            // SpEL 对整数做除法会得到整数，这里把整数字面量统一转成小数，保证 1/2 = 0.5。
            String normalized = expr.replaceAll("(?<![\\d.])(\\d+)(?![\\d.])", "$1.0");
            Object value = parser.parseExpression(normalized).getValue();
            if (!(value instanceof Number number)) {
                events.error(call, "结果不是数字");
                return "这个表达式没有计算出数字结果。";
            }
            String result = format(number.doubleValue());
            String output = expr + " = " + result;
            events.end(call, output);
            return output;
        } catch (Exception e) {
            String message = "无法计算：" + e.getMessage();
            events.error(call, message);
            return message;
        }
    }

    private String format(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return "未定义（除数为 0 或结果超出范围）";
        }
        if (value == Math.rint(value) && Math.abs(value) < 1e15) {
            return String.valueOf((long) value);
        }
        return BigDecimal.valueOf(value).stripTrailingZeros().toPlainString();
    }
}
