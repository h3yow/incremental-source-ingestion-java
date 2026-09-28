package com.github.h3yow.incrementalsourceingestionjava.model;

/**
 * 归一化拒绝原因。每种原因互斥、可区分。
 */
public enum RejectionReason {
    /** 字段整体缺失（fields 为 null） */
    FIELDS_MISSING,
    /** 业务主键字段缺失 */
    MISSING_BUSINESS_KEY,
    /** 业务主键存在但为空串/空白 */
    EMPTY_BUSINESS_KEY,
    /** 业务主键类型无法转为字符串（如对象/数组） */
    BUSINESS_KEY_TYPE_MISMATCH,
    /** 来源游标缺失 */
    MISSING_CURSOR,
    /** 游标类型对不上（无法解析为长整型） */
    CURSOR_TYPE_MISMATCH,
    /** 事件时间格式不完整/无法解析 */
    EVENT_TIME_MALFORMED,
    /** 必要载荷字段缺失 */
    MISSING_REQUIRED_PAYLOAD_FIELD
}
