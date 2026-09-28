package com.github.h3yow.incrementalsourceingestionjava.model;

/**
 * 目标端写入结果：新写入，或者命中幂等去重。
 */
public enum WriteOutcome {
    WRITTEN,
    DUPLICATE
}
