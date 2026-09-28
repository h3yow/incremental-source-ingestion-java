package com.github.h3yow.incrementalsourceingestionjava.model;

/**
 * 单条记录重试耗尽后的处理策略：隔离跳过，或标记失败并阻塞水位推进。
 */
public enum FailureStrategy {
    SKIP,
    FAIL
}
