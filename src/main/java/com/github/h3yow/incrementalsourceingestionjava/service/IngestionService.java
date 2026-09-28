package com.github.h3yow.incrementalsourceingestionjava.service;

import com.github.h3yow.incrementalsourceingestionjava.isolation.IsolationRecord;
import com.github.h3yow.incrementalsourceingestionjava.isolation.IsolationStatus;
import com.github.h3yow.incrementalsourceingestionjava.isolation.IsolationStore;
import com.github.h3yow.incrementalsourceingestionjava.model.NormalizedRecord;
import com.github.h3yow.incrementalsourceingestionjava.model.RawRecord;
import com.github.h3yow.incrementalsourceingestionjava.model.RejectedRecordException;
import com.github.h3yow.incrementalsourceingestionjava.normalize.Normalizer;
import com.github.h3yow.incrementalsourceingestionjava.policy.RetryDecision;
import com.github.h3yow.incrementalsourceingestionjava.policy.RetryPolicy;
import com.github.h3yow.incrementalsourceingestionjava.sink.TargetSink;
import com.github.h3yow.incrementalsourceingestionjava.source.SourceConnector;
import com.github.h3yow.incrementalsourceingestionjava.source.SourceRegistry;
import com.github.h3yow.incrementalsourceingestionjava.watermark.Watermark;
import com.github.h3yow.incrementalsourceingestionjava.watermark.WatermarkStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 增量采集编排：增量拉取 -> 统一归一 -> 策略重试 -> 幂等写入 ->
 * 逐条提交水位 -> 失败隔离。同来源并发拉取由水位 CAS 保证断点不互相覆盖。
 */
public class IngestionService {

    private static final Logger log = LoggerFactory.getLogger(IngestionService.class);

    private final SourceRegistry registry;
    private final Normalizer normalizer;
    private final WatermarkStore watermarkStore;
    private final TargetSink sink;
    private final IsolationStore isolationStore;
    private final RetryPolicy retryPolicy;
    private final RecordFailureInjector failureInjector;

    public IngestionService(SourceRegistry registry,
                            Normalizer normalizer,
                            WatermarkStore watermarkStore,
                            TargetSink sink,
                            IsolationStore isolationStore,
                            RetryPolicy retryPolicy,
                            RecordFailureInjector failureInjector) {
        this.registry = registry;
        this.normalizer = normalizer;
        this.watermarkStore = watermarkStore;
        this.sink = sink;
        this.isolationStore = isolationStore;
        this.retryPolicy = retryPolicy;
        this.failureInjector = failureInjector == null ? RecordFailureInjector.NOOP : failureInjector;
    }

    public IngestionService(SourceRegistry registry, Normalizer normalizer, WatermarkStore watermarkStore,
                            TargetSink sink, IsolationStore isolationStore, RetryPolicy retryPolicy) {
        this(registry, normalizer, watermarkStore, sink, isolationStore, retryPolicy,
                RecordFailureInjector.NOOP);
    }

    /** 按当前水位增量采集一批 */
    public BatchResult runBatch(String sourceName, int limit) {
        SourceConnector connector = registry.connector(sourceName)
                .orElseThrow(() -> new IllegalArgumentException("未注册的来源实例: " + sourceName));
        return process(sourceName, connector, false, limit);
    }

    /** 把隔离区里仍待处理的记录重新捞回处理（不重复写入、不回退水位） */
    public BatchResult recoverPending(String sourceName) {
        SourceConnector connector = registry.connector(sourceName)
                .orElseThrow(() -> new IllegalArgumentException("未注册的来源实例: " + sourceName));
        List<IsolationRecord> pending = isolationStore.fetchPending(sourceName);
        log.info("来源[{}]开始捞回隔离记录 {} 条", sourceName, pending.size());

        List<ItemResult> items = new ArrayList<>();
        int success = 0, failed = 0, skipped = 0, duplicate = 0;
        for (IsolationRecord rec : pending) {
            ItemResult r = processOne(rec.raw(), connector, rec.attempts());
            items.add(r);
            switch (r.outcome()) {
                case SUCCESS -> { success++; isolationStore.markRecovered(rec.id()); }
                case DUPLICATE -> { duplicate++; isolationStore.markRecovered(rec.id()); }
                case FAILED -> failed++;
                case SKIPPED -> skipped++;
            }
        }
        return new BatchResult(sourceName, true, items.size(), success, failed, skipped, duplicate,
                watermarkStore.load(sourceName).position(),
                watermarkStore.load(sourceName).position(), items);
    }

    private BatchResult process(String sourceName, SourceConnector connector,
                                boolean recoveredRun, int limit) {
        Watermark wm = watermarkStore.load(sourceName);
        Long after = wm.isEmpty() ? null : wm.position();
        List<RawRecord> records = connector.fetch(after, limit);
        log.info("来源[{}]增量拉取: 水位 {} -> 取到 {} 条", sourceName, after, records.size());

        List<ItemResult> items = new ArrayList<>();
        int success = 0, failed = 0, skipped = 0, duplicate = 0;
        long maxCursorInBatch = wm.isEmpty() ? Long.MIN_VALUE : wm.position();

        for (RawRecord raw : records) {
            ItemResult r = processOne(raw, connector, 0);
            items.add(r);
            switch (r.outcome()) {
                case SUCCESS -> success++;
                case FAILED -> failed++;
                case SKIPPED -> skipped++;
                case DUPLICATE -> duplicate++;
            }
            // 仅对"已成功落库或确认重复"的记录推进水位；隔离/跳过的记录不推进，
            // 之后可通过隔离区重新捞回，绝不丢数据。
            if ((r.outcome() == ItemOutcome.SUCCESS || r.outcome() == ItemOutcome.DUPLICATE)
                    && r.cursor() instanceof Long c) {
                maxCursorInBatch = Math.max(maxCursorInBatch, c);
            }
        }

        long before = wm.position();
        Long afterPos = null;
        if (maxCursorInBatch > (wm.isEmpty() ? Long.MIN_VALUE : wm.position())) {
            Watermark committed = watermarkStore.commit(sourceName, wm.version(), maxCursorInBatch);
            afterPos = committed.position();
        } else {
            afterPos = wm.isEmpty() ? null : wm.position();
        }
        log.info("来源[{}]本批结束: 成功={} 失败={} 跳过={} 重复={} 水位 {} -> {}",
                sourceName, success, failed, skipped, duplicate, before, afterPos);
        return new BatchResult(sourceName, recoveredRun, items.size(),
                success, failed, skipped, duplicate, before, afterPos, items);
    }

    /** 处理单条记录：归一化（确定性失败立即隔离）-> 重试 -> 幂等写入 */
    private ItemResult processOne(RawRecord raw, SourceConnector connector, int priorAttempts) {
        String key = rawKey(raw, connector);
        Object cursor = raw.cursor();
        int attempts = priorAttempts;

        // 1. 归一化：确定性错误不重试
        NormalizedRecord normalized;
        try {
            normalized = normalizer.normalize(raw);
        } catch (RejectedRecordException e) {
            attempts++;
            String id = isolationId(raw, key);
            RetryDecision decision = retryPolicy.decide(attempts, true);
            IsolationStatus st = decision.skipped() ? IsolationStatus.SKIPPED : IsolationStatus.FAILED;
            isolationStore.put(id, raw.sourceName(), raw, e.reason(),
                    "归一化拒绝: " + e.getMessage(), attempts, st);
            logReject(raw, connector, e.reason(), e.getMessage());
            return new ItemResult(raw.sourceName(), key, cursor,
                    decision.skipped() ? ItemOutcome.SKIPPED : ItemOutcome.FAILED,
                    e.reason(), e.getMessage(), attempts);
        } catch (RuntimeException e) {
            // 归一化阶段非预期异常，按确定性失败处理
            attempts++;
            String id = isolationId(raw, key);
            isolationStore.put(id, raw.sourceName(), raw, null,
                    "归一化异常: " + e.getMessage(), attempts, IsolationStatus.FAILED);
            logReject(raw, connector, null, e.getMessage());
            return new ItemResult(raw.sourceName(), key, cursor, ItemOutcome.FAILED,
                    null, e.getMessage(), attempts);
        }

        // 2. 处理期：重试 + 失败注入
        while (true) {
            attempts++;
            try {
                failureInjector.check(normalized);
                boolean isNew = sink.put(normalized);
                ItemOutcome outcome = isNew ? ItemOutcome.SUCCESS : ItemOutcome.DUPLICATE;
                String detail = isNew ? "写入目标端" : "目标端已存在，重复采集去重";
                log.info("记录处理: source={} key={} cursor={} 结果={} 依据={}",
                        raw.sourceName(), normalized.businessKey(), normalized.cursor(), outcome, detail);
                return new ItemResult(raw.sourceName(), normalized.businessKey(), normalized.cursor(),
                        outcome, null, detail, attempts);
            } catch (RuntimeException e) {
                RetryDecision decision = retryPolicy.decide(attempts, false);
                log.warn("记录处理失败(第{}次): source={} key={} 依据={} 决策={}",
                        attempts, raw.sourceName(), normalized.businessKey(), e.getMessage(),
                        decision.retry() ? "重试" : (decision.skipped() ? "跳过并隔离" : "失败并隔离"));
                if (decision.retry()) {
                    continue;
                }
                String id = isolationId(raw, normalized.businessKey());
                IsolationStatus st = decision.skipped() ? IsolationStatus.SKIPPED : IsolationStatus.FAILED;
                isolationStore.put(id, raw.sourceName(), raw, null,
                        "处理失败(重试" + attempts + "次): " + e.getMessage(), attempts, st);
                return new ItemResult(raw.sourceName(), normalized.businessKey(), normalized.cursor(),
                        decision.skipped() ? ItemOutcome.SKIPPED : ItemOutcome.FAILED,
                        null, e.getMessage(), attempts);
            }
        }
    }

    private void logReject(RawRecord raw, SourceConnector connector, Object reason, String detail) {
        log.warn("记录拒绝: source={} key={} cursor={} 原因={} 依据={}",
                raw.sourceName(), rawKey(raw, connector), raw.cursor(), reason, detail);
    }

    private String rawKey(RawRecord raw, SourceConnector connector) {
        if (raw.fields() == null) {
            return "<missing-fields>";
        }
        Object k = raw.fields().get(connector.sourceType().businessKeyField());
        return k == null ? "<missing-key>" : k.toString();
    }

    /** 稳定隔离键：来源实例 + 业务主键（与游标无关，重复捞回幂等） */
    static String isolationId(RawRecord raw, String businessKey) {
        return raw.sourceName() + ":" + businessKey;
    }
}
