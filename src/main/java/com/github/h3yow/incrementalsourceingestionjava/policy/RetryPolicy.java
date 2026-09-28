package com.github.h3yow.incrementalsourceingestionjava.policy;

/**
 * 单条失败处理策略。归一化拒绝（确定性错误）不重试直接隔离为 FAILED；
 * 处理期瞬时失败按次数重试，耗尽后隔离为 SKIPPED，后续可捞回。
 */
public interface RetryPolicy {

    int maxAttempts();

    /**
     * @param attempt       当前已执行的尝试次数（从 1 开始）
     * @param deterministic 是否确定性失败（归一化拒绝），确定性失败不重试
     */
    RetryDecision decide(int attempt, boolean deterministic);
}
