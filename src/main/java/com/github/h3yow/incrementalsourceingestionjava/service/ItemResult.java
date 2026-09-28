package com.github.h3yow.incrementalsourceingestionjava.service;

import com.github.h3yow.incrementalsourceingestionjava.model.RejectionReason;

/**
 * 单条记录的处理结果（含判定依据，便于测试日志打印）。
 */
public record ItemResult(
        String sourceName,
        String businessKey,
        Object cursor,
        ItemOutcome outcome,
        RejectionReason reason,
        String detail,
        int attempts
) {
}
