package com.github.h3yow.incrementalsourceingestionjava.engine;

import com.github.h3yow.incrementalsourceingestionjava.exception.SourceTypeNotRegisteredException;
import com.github.h3yow.incrementalsourceingestionjava.model.FailureStrategy;
import com.github.h3yow.incrementalsourceingestionjava.model.IngestionResult;
import com.github.h3yow.incrementalsourceingestionjava.model.NormalizationResult;
import com.github.h3yow.incrementalsourceingestionjava.model.NormalizedRecord;
import com.github.h3yow.incrementalsourceingestionjava.model.RawRecord;
import com.github.h3yow.incrementalsourceingestionjava.model.ReclaimResult;
import com.github.h3yow.incrementalsourceingestionjava.model.RejectedRecord;
import com.github.h3yow.incrementalsourceingestionjava.model.SkippedRecord;
import com.github.h3yow.incrementalsourceingestionjava.model.WriteOutcome;
import com.github.h3yow.incrementalsourceingestionjava.normalize.RecordNormalizer;
import com.github.h3yow.incrementalsourceingestionjava.normalize.StrictRecordNormalizer;
import com.github.h3yow.incrementalsourceingestionjava.retry.RetryExecutor;
import com.github.h3yow.incrementalsourceingestionjava.retry.RetryPolicy;
import com.github.h3yow.incrementalsourceingestionjava.sink.LocalSink;
import com.github.h3yow.incrementalsourceingestionjava.skipped.SkippedStore;
import com.github.h3yow.incrementalsourceingestionjava.source.SourceConnector;
import com.github.h3yow.incrementalsourceingestionjava.source.SourceRegistry;
import com.github.h3yow.incrementalsourceingestionjava.watermark.WatermarkStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 增量采集编排引擎。
 *
 * 处理流程：
 * 恢复水位（拒绝回退） -> 按水位增量拉取 -> 逐条归一（明确拒绝原因）
 * -> 幂等写入（按策略重试）-> 失败隔离或阻塞 -> 水位单调条件提交。
 *
 * 同一个来源的并发拉取用每来源锁串行化，保证断点不会相互覆盖、结果一致；
 * 不同来源之间仍然并行。
 */
public class IngestionEngine {

    private static final Logger log = LoggerFactory.getLogger(IngestionEngine.class);

    private final SourceRegistry registry;
    private final WatermarkStore watermarkStore;
    private final SkippedStore skippedStore;
    private final LocalSink sink;
    private final RetryExecutor retryExecutor;
    private final RetryPolicy retryPolicy;
    private final RecordNormalizer normalizer = new StrictRecordNormalizer();

    /** 每个来源一把锁，保证同一来源并发采集串行化。 */
    private final Map<String, Object> sourceLocks = new ConcurrentHashMap<>();

    public IngestionEngine(SourceRegistry registry,
                           WatermarkStore watermarkStore,
                           SkippedStore skippedStore,
                           LocalSink sink,
                           RetryPolicy retryPolicy) {
        this.registry = registry;
        this.watermarkStore = watermarkStore;
        this.skippedStore = skippedStore;
        this.sink = sink;
        this.retryExecutor = new RetryExecutor(retryPolicy);
        this.retryPolicy = retryPolicy;
    }

    private Object lockFor(String sourceId) {
        return sourceLocks.computeIfAbsent(sourceId, k -> new Object());
    }

    public IngestionResult ingestOnce(String sourceType, String sourceId,
                                      int batchSize, FailureStrategy strategy) {
        // 同一来源并发拉取：后来者排队，保证顺序与水位一致，不会互相覆盖断点。
        synchronized (lockFor(sourceId)) {
            SourceConnector connector = registry.find(sourceType, sourceId)
                    .orElseThrow(() -> new SourceTypeNotRegisteredException(sourceType));

            // 恢复点只能从已提交水位开始，回退直接拒绝。
            long startWatermark = watermarkStore.load(sourceId).orElse(0L);
            long safeStart = watermarkStore.verifyStartPoint(sourceId, startWatermark);

            List<RawRecord> batch = connector.fetch(safeStart, batchSize);

            int succeeded = 0;
            int duplicates = 0;
            int skipped = 0;
            int rejected = 0;
            List<RejectedRecord> rejectedRecords = new ArrayList<>();
            List<SkippedRecord> skippedRecords = new ArrayList<>();
            List<NormalizedRecord> failedRecords = new ArrayList<>();

            // 已"确认处理完"的序号集合：新写入、幂等重复、SKIP 隔离、归一拒绝（序号可解析）
            // 都算确认；FAIL 失败不算确认。水位只能覆盖从起点开始的连续确认前缀。
            java.util.Set<Long> confirmedSequences = new java.util.TreeSet<>();
            // 归一被拒且序号无法解析的记录数：它们不占任何序号位置（排序在末尾），
            // 不阻塞水位，但每轮都会被再次拉到并明确拒绝，不会悄悄消失。
            int unpositionableRejections = 0;

            for (RawRecord raw : batch) {
                NormalizationResult result = normalizer.normalize(raw);
                if (!result.isAccepted()) {
                    rejected++;
                    rejectedRecords.add(result.rejected());
                    log.info("记录被拒绝 | 输入={} | 判定原因={} | 细节={}",
                            raw.data(), result.rejected().reason(), result.rejected().detail());
                    // 拒绝是永久判定：序号可解析时记入确认前缀（已被明确处置，
                    // 重采仍会被同样拒绝）；序号无法解析时不占序号、每轮重复拒绝。
                    Long seq = asLongOrNull(raw.rawSequence());
                    if (seq != null) {
                        confirmedSequences.add(seq);
                    } else {
                        unpositionableRejections++;
                    }
                    continue;
                }

                NormalizedRecord record = result.accepted();
                try {
                    int attempts = retryExecutor.execute(() -> {
                        WriteOutcome outcome = sink.write(record);
                        if (outcome == WriteOutcome.DUPLICATE) {
                            throw new DuplicateMarker();
                        }
                    });
                    succeeded++;
                    confirmedSequences.add(record.sequence());
                    log.info("记录写入成功 | 输入={} | 判定依据=dedupKey幂等写入新数据 | 尝试次数={} | sequence={}",
                            raw.data(), attempts, record.sequence());
                } catch (DuplicateMarker dm) {
                    // 重复采集：目标端已存在，不算新写入也不算失败，不产生重复。
                    duplicates++;
                    confirmedSequences.add(record.sequence());
                    log.info("记录命中幂等去重 | 输入={} | 判定依据=dedupKey已存在 | sequence={}",
                            raw.data(), record.sequence());
                } catch (Exception writeError) {
                    if (strategy == FailureStrategy.SKIP) {
                        SkippedRecord sr = new SkippedRecord(
                                sourceId, record.dedupKey(), record.sequence(),
                                record.recordId(), record, writeError.getMessage(),
                                System.currentTimeMillis(), retryPolicy.maxAttempts());
                        skippedStore.save(sr);
                        skippedRecords.add(sr);
                        skipped++;
                        confirmedSequences.add(record.sequence());
                        log.warn("记录写入失败已隔离跳过 | 输入={} | 判定依据=重试耗尽后SKIP策略 | 原因={} | sequence={}",
                                raw.data(), writeError.getMessage(), record.sequence());
                    } else {
                        failedRecords.add(record);
                        log.error("记录写入失败并阻塞水位 | 输入={} | 判定依据=重试耗尽后FAIL策略 | 原因={} | sequence={}",
                                raw.data(), writeError.getMessage(), record.sequence());
                    }
                }
            }

            long advancedWatermark = safeStart;
            // 从起点后一个序号开始，逐个确认连续前缀；遇到 FAIL 失败或空洞即停。
            long cursor = safeStart + 1;
            while (confirmedSequences.contains(cursor)) {
                advancedWatermark = cursor;
                cursor++;
            }

            if (advancedWatermark > safeStart) {
                // 条件提交：只有当前水位仍是本批次开始时读到的值才允许推进，
                // 并发下被别人推高会返回 false（回退则抛异常）。
                watermarkStore.compareAndAdvance(sourceId, safeStart, advancedWatermark);
            }

            log.info("批次结束 | 来源={} | 起始水位={} | 提交水位={} | 成功={} 重复={} 跳过={} 拒绝={} 失败={}",
                    sourceId, safeStart, advancedWatermark, succeeded, duplicates,
                    skipped, rejected, failedRecords.size());

            return new IngestionResult(sourceId, safeStart, advancedWatermark,
                    succeeded, duplicates, skipped, rejected, failedRecords.size(),
                    rejectedRecords, skippedRecords, failedRecords);
        }
    }

    /**
     * 把隔离区里该来源的跳过记录重新捞回写入。
     * 成功的从隔离区移除；仍失败的保留在隔离区，下次可继续捞。
     * 幂等：即便目标端已经有了（被别的路径写过），也按恢复成功处理，不会重复写入。
     */
    public ReclaimResult reclaimSkipped(String sourceId) {
        synchronized (lockFor(sourceId)) {
            List<SkippedRecord> pending = skippedStore.findBySource(sourceId);
            int recovered = 0;
            List<String> recoveredKeys = new ArrayList<>();
            List<SkippedRecord> stillFailed = new ArrayList<>();

            for (SkippedRecord sr : pending) {
                try {
                    sink.write(sr.record());
                    skippedStore.remove(sourceId, sr.dedupKey());
                    recovered++;
                    recoveredKeys.add(sr.dedupKey());
                    log.info("跳过记录重捞成功 | 来源={} | dedupKey={} | 判定依据=幂等写入成功",
                            sourceId, sr.dedupKey());
                } catch (Exception e) {
                    stillFailed.add(sr);
                    log.warn("跳过记录重捞仍失败 | 来源={} | dedupKey={} | 原因={}",
                            sourceId, sr.dedupKey(), e.getMessage());
                }
            }
            return new ReclaimResult(recovered, stillFailed.size(), recoveredKeys, stillFailed);
        }
    }

    private Long asLongOrNull(Object value) {
        if (value instanceof Number n) {
            return n.longValue();
        }
        if (value instanceof String s) {
            try {
                return Long.parseLong(s.trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    /** 内部信号：用异常在重试闭包里表达"命中重复"，避免重复被当成失败重试。 */
    private static final class DuplicateMarker extends RuntimeException {
        DuplicateMarker() {
            super(null, null, false, false);
        }
    }
}
