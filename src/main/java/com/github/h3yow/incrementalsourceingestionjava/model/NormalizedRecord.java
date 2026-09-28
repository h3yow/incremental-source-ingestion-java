package com.github.h3yow.incrementalsourceingestionjava.model;

import java.util.Map;

/**
 * 归一化后的标准记录。
 *
 * @param sourceType  来源类型
 * @param sourceName  来源实例
 * @param businessKey 业务主键（幂等键），同一来源内唯一
 * @param cursor      来源端游标
 * @param eventTime   事件时间（epoch millis），无法解析时为 null
 * @param payload     归一后的标准字段
 */
public record NormalizedRecord(
        String sourceType,
        String sourceName,
        String businessKey,
        Long cursor,
        Long eventTime,
        Map<String, Object> payload
) {
}
