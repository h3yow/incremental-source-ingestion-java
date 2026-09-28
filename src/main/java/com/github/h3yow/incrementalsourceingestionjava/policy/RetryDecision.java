package com.github.h3yow.incrementalsourceingestionjava.policy;

/**
 * 单条记录失败后的策略判定结果。
 *
 * @param retry     是否立即重试
 * @param backoffMs 重试前等待毫秒（本地模拟可忽略等待，仅记录决策）
 * @param isolated  本次处理结束后是否隔离（重试耗尽 / 明确跳过）
 * @param skipped   隔离时是否标记为 SKIPPED；false 则标记为 FAILED
 */
public record RetryDecision(
        boolean retry,
        long backoffMs,
        boolean isolated,
        boolean skipped
) {

    public static RetryDecision retryNow(long backoffMs) {
        return new RetryDecision(true, backoffMs, false, false);
    }

    public static RetryDecision skip() {
        return new RetryDecision(false, 0L, true, true);
    }

    public static RetryDecision fail() {
        return new RetryDecision(false, 0L, true, false);
    }
}
