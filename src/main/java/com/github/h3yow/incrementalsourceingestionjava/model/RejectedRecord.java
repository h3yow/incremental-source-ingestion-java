package com.github.h3yow.incrementalsourceingestionjava.model;

/**
 * 归一阶段被拒绝的原始记录及其明确原因。
 */
public record RejectedRecord(
        RawRecord rawRecord,
        RejectReason reason,
        String detail
) {
}
