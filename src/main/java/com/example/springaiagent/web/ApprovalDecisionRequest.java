package com.example.springaiagent.web;

/**
 * 用户对一次工具确认请求的决定。
 *
 * @param approved        是否允许执行
 * @param rememberSession 是否在当前会话内记住这些工具的授权
 */
public record ApprovalDecisionRequest(Boolean approved, Boolean rememberSession) {

    public boolean isApproved() {
        return Boolean.TRUE.equals(approved);
    }

    public boolean shouldRemember() {
        return Boolean.TRUE.equals(rememberSession);
    }
}
