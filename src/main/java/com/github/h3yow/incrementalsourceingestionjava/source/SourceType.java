package com.github.h3yow.incrementalsourceingestionjava.source;

/**
 * 一种来源类型的元信息与字段映射约定。
 * 不同来源类型注册后，由归一化门面按各自约定统一归一。
 */
public interface SourceType {

    /** 类型标识，如 "orders-file"、"metrics-http" */
    String type();

    /** 原始字段里业务主键的字段名 */
    String businessKeyField();

    /** 原始字段里事件时间的字段名 */
    String eventTimeField();

    /** 必须存在的载荷字段（缺失即拒绝） */
    java.util.List<String> requiredPayloadFields();
}
