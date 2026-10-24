package com.example.springaiagent.tools;

import com.example.springaiagent.agent.ToolCall;
import com.example.springaiagent.agent.ToolEventPublisher;
import org.springframework.ai.chat.model.ToolContext;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 天气工具：用固定的模拟数据演示工具调用，不依赖第三方天气 API。
 */
@Component
public class WeatherTools {

    private static final Map<String, String> WEATHER = Map.of(
            "北京", "晴，18~27℃，西北风 2 级，紫外线较强",
            "上海", "多云转小雨，21~28℃，东南风 3 级，建议带伞",
            "杭州", "小雨，19~25℃，微风，湿度 82%",
            "深圳", "雷阵雨，26~32℃，南风 3 级，午后注意强对流",
            "成都", "阴，17~23℃，微风，空气湿度较大",
            "广州", "多云，25~33℃，东南风 2 级，体感闷热",
            "西安", "晴转多云，14~24℃，东北风 2 级",
            "南京", "多云，20~29℃，东风 2 级"
    );

    private final ToolEventPublisher events;

    public WeatherTools(ToolEventPublisher events) {
        this.events = events;
    }

    @Tool(description = "查询指定城市今天的天气。注意：这是演示用的模拟数据，不是实时天气。用户询问天气时调用。")
    public String getWeather(
            @ToolParam(description = "城市中文名，例如 北京、上海、杭州") String city,
            ToolContext context) {
        String normalized = city == null ? "" : city.trim().replace("市", "");
        String input = "{\"city\":\"" + normalized + "\"}";
        ToolCall call = events.start(context, "getWeather", "查询城市天气", input);

        if (normalized.isBlank()) {
            events.error(call, "缺少城市参数");
            return "请提供要查询的城市名称。";
        }

        String weather = WEATHER.get(normalized);
        if (weather == null) {
            String message = "暂无「" + normalized + "」的模拟天气数据";
            events.error(call, message);
            return message + "。当前演示数据仅包含：" + String.join("、", WEATHER.keySet()) + "。";
        }

        String output = normalized + "今天：" + weather + "（模拟数据，非实时天气）";
        events.end(call, output);
        return output;
    }
}
