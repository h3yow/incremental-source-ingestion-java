package com.github.h3yow.incrementalsourceingestionjava.model;

/**
 * 因写入失败被隔离的记录快照，之后可被重新捞回重投。
 */
public record SkippedRecord(
        String sourceId,
        String dedupKey,
        long sequence,
        String recordId,
        NormalizedRecord record,
        String errorReason,
        long skippedAtMillis,
        int attemptCount
) {
}
