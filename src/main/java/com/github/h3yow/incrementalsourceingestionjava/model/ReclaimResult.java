package com.github.h3yow.incrementalsourceingestionjava.model;

import java.util.List;

/**
 * 重捞历史跳过记录的结果：重新写回成功多少、仍然失败多少。
 */
public final class ReclaimResult {
    private final int recoveredCount;
    private final int stillFailedCount;
    private final List<String> recoveredDedupKeys;
    private final List<SkippedRecord> stillFailed;

    public ReclaimResult(int recoveredCount, int stillFailedCount,
                         List<String> recoveredDedupKeys, List<SkippedRecord> stillFailed) {
        this.recoveredCount = recoveredCount;
        this.stillFailedCount = stillFailedCount;
        this.recoveredDedupKeys = List.copyOf(recoveredDedupKeys);
        this.stillFailed = List.copyOf(stillFailed);
    }

    public int recoveredCount() { return recoveredCount; }
    public int stillFailedCount() { return stillFailedCount; }
    public List<String> recoveredDedupKeys() { return recoveredDedupKeys; }
    public List<SkippedRecord> stillFailed() { return stillFailed; }
}
