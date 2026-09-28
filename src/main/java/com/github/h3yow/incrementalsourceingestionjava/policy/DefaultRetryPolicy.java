package com.github.h3yow.incrementalsourceingestionjava.policy;

/**
 * 默认策略：瞬时失败最多 maxAttempts 次尝试，重试间隔指数退避；
 * 确定性失败（归一化拒绝）立即隔离为 FAILED；瞬时失败耗尽后隔离为 SKIPPED（可捞回）。
 */
public class DefaultRetryPolicy implements RetryPolicy {

    private final int maxAttempts;

    public DefaultRetryPolicy() {
        this(3);
    }

    public DefaultRetryPolicy(int maxAttempts) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts 必须 >= 1");
        }
        this.maxAttempts = maxAttempts;
    }

    @Override
    public int maxAttempts() {
        return maxAttempts;
    }

    @Override
    public RetryDecision decide(int attempt, boolean deterministic) {
        if (deterministic) {
            // 确定性错误：重试无意义，直接失败隔离
            return RetryDecision.fail();
        }
        if (attempt < maxAttempts) {
            long backoff = (long) Math.pow(2, attempt - 1) * 50L; // 50,100,200... 仅记录决策
            return RetryDecision.retryNow(backoff);
        }
        // 瞬时失败重试耗尽：跳过并隔离，之后可重新捞回
        return RetryDecision.skip();
    }
}
