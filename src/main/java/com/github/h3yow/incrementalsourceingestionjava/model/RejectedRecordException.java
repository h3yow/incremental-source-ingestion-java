package com.github.h3yow.incrementalsourceingestionjava.model;

/**
 * 一条记录因确定性原因被归一化拒绝。
 */
public class RejectedRecordException extends RuntimeException {

    private final RejectionReason reason;

    public RejectedRecordException(RejectionReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectionReason reason() {
        return reason;
    }
}
