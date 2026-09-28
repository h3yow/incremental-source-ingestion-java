package com.github.h3yow.incrementalsourceingestionjava.exception;

/**
 * 水位被回退或从一个低于已提交水位的位置启动时抛出，
 * 不允许从一个来路不明的中间点继续采集。
 */
public class WatermarkRegressionException extends RuntimeException {

    private final String sourceId;
    private final long existingWatermark;
    private final long requestedWatermark;

    public WatermarkRegressionException(String sourceId, long existingWatermark, long requestedWatermark) {
        super("来源[" + sourceId + "]水位拒绝回退：已提交水位=" + existingWatermark
                + "，请求水位=" + requestedWatermark + "，不允许从更低的位置继续采集");
        this.sourceId = sourceId;
        this.existingWatermark = existingWatermark;
        this.requestedWatermark = requestedWatermark;
    }

    public String sourceId() {
        return sourceId;
    }

    public long existingWatermark() {
        return existingWatermark;
    }

    public long requestedWatermark() {
        return requestedWatermark;
    }
}
