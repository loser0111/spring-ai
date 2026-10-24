package com.example.springaiagent.tools;

import com.example.springaiagent.agent.ToolCall;
import com.example.springaiagent.agent.ToolEventPublisher;
import org.springframework.ai.chat.model.ToolContext;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.stereotype.Component;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * 时间类工具：演示「模型按需获取实时信息」。
 */
@Component
public class TimeTools {

    private static final ZoneId DEFAULT_ZONE = ZoneId.of("Asia/Shanghai");
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss EEEE", Locale.CHINA);

    private final ToolEventPublisher events;

    public TimeTools(ToolEventPublisher events) {
        this.events = events;
    }

    @Tool(description = "获取当前日期、时间和星期，使用 Asia/Shanghai 时区。用户询问今天几号、现在几点、今天星期几时调用。")
    public String getCurrentDateTime(ToolContext context) {
        ToolCall call = events.start(context, "getCurrentDateTime", "查询当前时间", "{}");
        try {
            String result = "当前时间：" + ZonedDateTime.now(DEFAULT_ZONE).format(FORMATTER)
                    + "（Asia/Shanghai）";
            events.end(call, result);
            return result;
        } catch (Exception e) {
            events.error(call, e.getMessage());
            return "查询时间失败：" + e.getMessage();
        }
    }

    @Tool(description = "查询指定 IANA 时区的当前时间，例如 America/New_York、Europe/London、Asia/Tokyo。")
    public String getTimeInZone(
            @ToolParam(description = "IANA 时区 ID，例如 Asia/Tokyo") String timeZone,
            ToolContext context) {
        String input = "{\"timeZone\":\"" + (timeZone == null ? "" : timeZone) + "\"}";
        ToolCall call = events.start(context, "getTimeInZone", "查询指定时区时间", input);
        if (timeZone == null || timeZone.isBlank()) {
            events.error(call, "缺少时区参数");
            return "请提供 IANA 时区 ID，例如 Asia/Tokyo。";
        }
        try {
            String result = "当前时间：" + ZonedDateTime.now(ZoneId.of(timeZone.trim())).format(FORMATTER)
                    + "（" + timeZone.trim() + "）";
            events.end(call, result);
            return result;
        } catch (DateTimeException e) {
            String message = "无法识别的时区：" + timeZone;
            events.error(call, message);
            return message + "，请使用类似 Asia/Tokyo 的 IANA 时区 ID。";
        }
    }
}
