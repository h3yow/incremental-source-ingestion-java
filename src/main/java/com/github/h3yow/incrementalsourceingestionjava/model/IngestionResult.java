package com.github.h3yow.incrementalsourceingestionjava.model;

import java.util.List;

/**
 * 一批采集跑完后的统计结果：成功、重复、跳过、失败各多少，
 * 以及本批推进到的水位。
 */
public final class IngestionResult {
    private final String sourceId;
    private final long fromWatermark;
    private final long advancedWatermark;
    private final int succeededCount;
    private final int duplicateCount;
    private final int skippedCount;
    private final int rejectedCount;
    private final int failedCount;
    private final List<RejectedRecord> rejected;
    private final List<SkippedRecord> skipped;
    private final List<NormalizedRecord> failed;

    public IngestionResult(String sourceId, long fromWatermark, long advancedWatermark,
                           int succeededCount, int duplicateCount, int skippedCount,
                           int rejectedCount, int failedCount,
                           List<RejectedRecord> rejected,
                           List<SkippedRecord> skipped,
                           List<NormalizedRecord> failed) {
        this.sourceId = sourceId;
        this.fromWatermark = fromWatermark;
        this.advancedWatermark = advancedWatermark;
        this.succeededCount = succeededCount;
        this.duplicateCount = duplicateCount;
        this.skippedCount = skippedCount;
        this.rejectedCount = rejectedCount;
        this.failedCount = failedCount;
        this.rejected = List.copyOf(rejected);
        this.skipped = List.copyOf(skipped);
        this.failed = List.copyOf(failed);
    }

    public String sourceId() { return sourceId; }
    public long fromWatermark() { return fromWatermark; }
    public long advancedWatermark() { return advancedWatermark; }
    public int succeededCount() { return succeededCount; }
    public int duplicateCount() { return duplicateCount; }
    public int skippedCount() { return skippedCount; }
    public int rejectedCount() { return rejectedCount; }
    public int failedCount() { return failedCount; }
    public List<RejectedRecord> rejected() { return rejected; }
    public List<SkippedRecord> skipped() { return skipped; }
    public List<NormalizedRecord> failed() { return failed; }
}
