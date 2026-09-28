package com.github.h3yow.incrementalsourceingestionjava.model;

import java.util.Map;

/**
 * 来源端返回的原始记录，数据尚未经过归一校验。
 */
public record RawRecord(
        String sourceType,
        String sourceId,
        Object rawSequence,
        Map<String, Object> data
) {
}
