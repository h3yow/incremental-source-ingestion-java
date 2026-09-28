package com.github.h3yow.incrementalsourceingestionjava.model;

/**
 * 归一阶段互相区分的拒绝原因，每一种对应一类明确的数据问题，
 * 不允许用笼统的"脏数据"悄悄跳过。
 */
public enum RejectReason {
    SEQUENCE_MISSING("缺少采集序号 sequence"),
    SEQUENCE_NOT_A_NUMBER("sequence 不是可解析的数字"),
    MISSING_ID("缺少记录主键 id"),
    ID_TYPE_MISMATCH("id 类型不是字符串"),
    MISSING_NAME("缺少 name 字段"),
    NAME_BLANK("name 为空白字符串"),
    EVENT_TIME_MALFORMED("eventTime 无法解析为时间");

    private final String description;

    RejectReason(String description) {
        this.description = description;
    }

    public String description() {
        return description;
    }
}
