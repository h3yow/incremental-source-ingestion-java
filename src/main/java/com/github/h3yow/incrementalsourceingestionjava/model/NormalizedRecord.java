package com.github.h3yow.incrementalsourceingestionjava.model;

/**
 * 通过归一校验后的统一记录格式。
 */
public record NormalizedRecord(
        String sourceType,
        String sourceId,
        long sequence,
        String recordId,
        String name,
        long eventTimeMillis,
        String rawPayload
) {
    public String dedupKey() {
        return sourceId + ":" + recordId;
    }
}
