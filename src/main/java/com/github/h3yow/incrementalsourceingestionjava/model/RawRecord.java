package com.github.h3yow.incrementalsourceingestionjava.model;

import java.util.Map;

/**
 * 来源端返回的原始记录。
 *
 * @param sourceType 来源类型标识
 * @param sourceName 来源实例名（同一来源类型下可有多个实例，水位/隔离均按 sourceName 维度隔离）
 * @param cursor     来源端游标（如递增序号），用于水位推进；null 表示来源未提供
 * @param fields     原始字段
 */
public record RawRecord(
        String sourceType,
        String sourceName,
        Object cursor,
        Map<String, Object> fields
) {
}
