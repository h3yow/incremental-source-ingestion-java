package com.github.h3yow.incrementalsourceingestionjava.retry;

/**
 * 按策略重试单条操作，返回实际尝试次数；耗尽后抛出最后一次异常。
 * 只重试，不吞异常——是否隔离/失败由上层策略决定。
 */
public class RetryExecutor {

    private final RetryPolicy policy;

    public RetryExecutor(RetryPolicy policy) {
        this.policy = policy;
    }

    @FunctionalInterface
    public interface Operation {
        void run() throws Exception;
    }

    public int execute(Operation operation) throws Exception {
        Exception last = null;
        for (int attempt = 1; attempt <= policy.maxAttempts(); attempt++) {
            try {
                operation.run();
                return attempt;
            } catch (Exception e) {
                last = e;
                if (attempt < policy.maxAttempts() && policy.backoffMillis() > 0) {
                    try {
                        Thread.sleep(policy.backoffMillis());
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw ie;
                    }
                }
            }
        }
        throw last;
    }
}
