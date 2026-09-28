package com.github.h3yow.incrementalsourceingestionjava.sink;

import com.github.h3yow.incrementalsourceingestionjava.model.NormalizedRecord;
import com.github.h3yow.incrementalsourceingestionjava.model.WriteOutcome;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本地模拟目标端：按 dedupKey 幂等写入，重复采集不会产生重复数据。
 *
 * 故障注入：让某个 key 的接下来 N 次写入抛出异常，N 次后自动恢复，
 * 用于模拟临时性故障下的重试、跳过与重捞。
 */
public class LocalSink {

    private final Map<String, NormalizedRecord> stored = new ConcurrentHashMap<>();
    private final Map<String, FaultPlan> faults = new ConcurrentHashMap<>();

    public WriteOutcome write(NormalizedRecord record) {
        FaultPlan plan = faults.get(record.dedupKey());
        if (plan != null) {
            synchronized (plan) {
                if (plan.remaining > 0) {
                    plan.remaining--;
                    throw new LocalWriteException(record.dedupKey(), plan.reason);
                }
                faults.remove(record.dedupKey());
            }
        }
        // putIfAbsent 保证并发或重复采集时同一条只会落一份。
        NormalizedRecord previous = stored.putIfAbsent(record.dedupKey(), record);
        return previous == null ? WriteOutcome.WRITTEN : WriteOutcome.DUPLICATE;
    }

    public boolean containsKey(String dedupKey) {
        return stored.containsKey(dedupKey);
    }

    public List<NormalizedRecord> allRecords(String sourceId) {
        return stored.values().stream()
                .filter(r -> r.sourceId().equals(sourceId))
                .sorted(java.util.Comparator.comparingLong(NormalizedRecord::sequence))
                .toList();
    }

    public int totalCount() {
        return stored.size();
    }

    /** 让指定 dedupKey 在接下来 failTimes 次写入时抛异常，之后自动恢复。 */
    public void failNextWrites(String dedupKey, int failTimes, String reason) {
        faults.put(dedupKey, new FaultPlan(failTimes, reason));
    }

    private static final class FaultPlan {
        int remaining;
        final String reason;

        FaultPlan(int remaining, String reason) {
            this.remaining = remaining;
            this.reason = reason;
        }
    }

    /** 模拟目标端写入失败。 */
    public static class LocalWriteException extends RuntimeException {
        public LocalWriteException(String dedupKey, String reason) {
            super("目标端写入失败，dedupKey=" + dedupKey + "，原因=" + reason);
        }
    }
}
