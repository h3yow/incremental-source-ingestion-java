package com.github.h3yow.incrementalsourceingestionjava.watermark;

import com.github.h3yow.incrementalsourceingestionjava.exception.WatermarkRegressionException;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 进程内内存水位存储，用于单测里模拟重启前后的水位恢复。
 *
 * 水位语义：已确认处理完的最大序号；只能单调递增。
 */
public class InMemoryWatermarkStore implements WatermarkStore {

    private final ConcurrentHashMap<String, Long> watermarks = new ConcurrentHashMap<>();

    @Override
    public Optional<Long> load(String sourceId) {
        return Optional.ofNullable(watermarks.get(sourceId));
    }

    @Override
    public boolean compareAndAdvance(String sourceId, long expected, long next) {
        // 用 compute 原子完成"比对 + 提交"，并发拉取时水位不会相互覆盖。
        boolean[] advanced = {false};
        watermarks.compute(sourceId, (key, current) -> {
            long currentValue = current == null ? 0L : current;
            if (currentValue != expected) {
                // 条件不匹配：别人已经提交过。若对方推高了水位，任何更低的提交都属回退。
                if (next < currentValue) {
                    throw new WatermarkRegressionException(sourceId, currentValue, next);
                }
                return current;
            }
            if (next < currentValue) {
                throw new WatermarkRegressionException(sourceId, currentValue, next);
            }
            if (next == currentValue) {
                return current;
            }
            advanced[0] = true;
            return next;
        });
        return advanced[0];
    }

    @Override
    public long verifyStartPoint(String sourceId, long requestedStart) {
        long committed = watermarks.getOrDefault(sourceId, 0L);
        if (requestedStart < committed) {
            throw new WatermarkRegressionException(sourceId, committed, requestedStart);
        }
        // 恢复时永远从已提交水位开始，忽略来路不明的更高起始点也由调用方决定，
        // 这里保证起点不会落在已提交水位之前。
        return Math.max(requestedStart, committed);
    }
}
