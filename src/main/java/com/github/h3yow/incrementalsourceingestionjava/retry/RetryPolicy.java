package com.github.h3yow.incrementalsourceingestionjava.retry;

/**
 * 单条写入的重试策略：最多尝试次数与两次尝试之间的退避毫秒数。
 */
public record RetryPolicy(int maxAttempts, long backoffMillis) {

    public static RetryPolicy noRetry() {
        return new RetryPolicy(1, 0L);
    }
}
