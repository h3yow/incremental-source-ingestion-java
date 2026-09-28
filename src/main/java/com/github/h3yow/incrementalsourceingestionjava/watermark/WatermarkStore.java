package com.github.h3yow.incrementalsourceingestionjava.watermark;

import com.github.h3yow.incrementalsourceingestionjava.exception.WatermarkRegressionException;

import java.util.Optional;

/**
 * 采集水位存储：记录每个来源已确认处理到的序号。
 * 水位只能单调递增，回退必须被拒绝。
 */
public interface WatermarkStore {

    Optional<Long> load(String sourceId);

    /**
     * 原子条件更新：仅当当前水位等于 expected 时写入 next。
     * 并发下若当前水位已被别人推高，用当前实际水位做参数抛出回归异常。
     */
    boolean compareAndAdvance(String sourceId, long expected, long next)
            throws WatermarkRegressionException;

    /**
     * 恢复时校验启动点不能低于已提交水位。
     */
    long verifyStartPoint(String sourceId, long requestedStart)
            throws WatermarkRegressionException;
}
