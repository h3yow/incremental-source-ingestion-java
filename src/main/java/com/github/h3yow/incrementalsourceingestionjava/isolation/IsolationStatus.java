package com.github.h3yow.incrementalsourceingestionjava.isolation;

/**
 * 被隔离记录的状态。
 */
public enum IsolationStatus {
    /** 按策略重试次数用尽后仍失败，隔离待人工/后续重处理 */
    FAILED,
    /** 被策略明确跳过（如重试耗尽），后续可重新捞回 */
    SKIPPED,
    /** 已被重新捞回处理成功（保留记录用于审计，不再参与捞取） */
    RECOVERED
}
