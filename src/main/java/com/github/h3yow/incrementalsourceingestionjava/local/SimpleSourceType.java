package com.github.h3yow.incrementalsourceingestionjava.local;

import com.github.h3yow.incrementalsourceingestionjava.source.SourceType;

import java.util.List;

/**
 * 一个最简单的来源类型约定：主键字段 id、时间字段 eventTime、必要载荷字段 name。
 * 不同来源类型可注册不同的映射约定，归一化门面据此统一归一。
 */
public record SimpleSourceType(
        String type,
        String businessKeyField,
        String eventTimeField,
        List<String> requiredPayloadFields
) implements SourceType {

    public static SimpleSourceType of(String type) {
        return new SimpleSourceType(type, "id", "eventTime", List.of("name"));
    }

    public static SimpleSourceType of(String type, String businessKeyField,
                                      String eventTimeField, List<String> required) {
        return new SimpleSourceType(type, businessKeyField, eventTimeField,
                List.copyOf(required));
    }
}
