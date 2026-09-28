package com.github.h3yow.incrementalsourceingestionjava.model;

/**
 * 单条记录归一的结果：要么被接受，要么带着明确原因被拒绝。
 * 两种状态互斥，不允许"悄悄跳过"——拒绝时必须带原因。
 */
public record NormalizationResult(
        NormalizedRecord accepted,
        RejectedRecord rejected
) {
    public static NormalizationResult accepted(NormalizedRecord record) {
        return new NormalizationResult(record, null);
    }

    public static NormalizationResult rejected(RawRecord raw, RejectReason reason, String detail) {
        return new NormalizationResult(null, new RejectedRecord(raw, reason, detail));
    }

    public boolean isAccepted() {
        return accepted != null;
    }
}
