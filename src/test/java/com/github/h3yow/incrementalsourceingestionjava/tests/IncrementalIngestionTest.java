package com.github.h3yow.incrementalsourceingestionjava.tests;

import com.github.h3yow.incrementalsourceingestionjava.engine.IngestionEngine;
import com.github.h3yow.incrementalsourceingestionjava.model.FailureStrategy;
import com.github.h3yow.incrementalsourceingestionjava.model.IngestionResult;
import com.github.h3yow.incrementalsourceingestionjava.model.ReclaimResult;
import com.github.h3yow.incrementalsourceingestionjava.retry.RetryPolicy;
import com.github.h3yow.incrementalsourceingestionjava.sink.LocalSink;
import com.github.h3yow.incrementalsourceingestionjava.skipped.InMemorySkippedStore;
import com.github.h3yow.incrementalsourceingestionjava.source.local.LocalTableSource;
import com.github.h3yow.incrementalsourceingestionjava.source.LocalSourceRegistry;
import com.github.h3yow.incrementalsourceingestionjava.watermark.InMemoryWatermarkStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 增量采集主干：分批采集不重不漏、重复整批采集目标端无重复、
 * 部分失败隔离跳过、重试成功、跳过记录重捞。
 */
class IncrementalIngestionTest {

    private static final Logger log = LoggerFactory.getLogger(IncrementalIngestionTest.class);

    private Map<String, Object> row(long seq, String id, String name) {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("id", id);
        d.put("name", name);
        d.put("eventTime", "2026-09-29T10:00:" + String.format("%02d", seq % 60) + "Z");
        return d;
    }

    @Test
    void batchedIngestionIsCompleteAndNoDuplicate() {
        LocalTableSource source = new LocalTableSource("tbl-1");
        for (long i = 1; i <= 10; i++) {
            source.addRow(i, row(i, "id-" + i, "name-" + i));
        }
        LocalSourceRegistry registry = new LocalSourceRegistry();
        registry.register(source);
        LocalSink sink = new LocalSink();
        InMemoryWatermarkStore wm = new InMemoryWatermarkStore();
        InMemorySkippedStore skipped = new InMemorySkippedStore();
        IngestionEngine engine = new IngestionEngine(registry, wm, skipped, sink,
                RetryPolicy.noRetry());

        // 分三批：4 + 4 + 2，靠水位续传。
        IngestionResult r1 = engine.ingestOnce(LocalTableSource.TYPE, "tbl-1", 4,
                FailureStrategy.SKIP);
        IngestionResult r2 = engine.ingestOnce(LocalTableSource.TYPE, "tbl-1", 4,
                FailureStrategy.SKIP);
        IngestionResult r3 = engine.ingestOnce(LocalTableSource.TYPE, "tbl-1", 4,
                FailureStrategy.SKIP);

        log.info("分批结果=({},{},{}) | 判定依据=按水位递增拉取，成功数应为4/4/2",
                r1.succeededCount(), r2.succeededCount(), r3.succeededCount());
        assertEquals(4, r1.succeededCount());
        assertEquals(4, r2.succeededCount());
        assertEquals(2, r3.succeededCount());
        assertEquals(10L, wm.load("tbl-1").orElseThrow());
        assertEquals(10, sink.totalCount());

        // 重复采集：模拟水位丢失/重建引擎后从零全量重拉（目标端仍是同一个）。
        // 目标端按 dedupKey 幂等，全部命中重复，不产生重复数据，也不漏。
        InMemoryWatermarkStore rebuiltWatermark = new InMemoryWatermarkStore();
        IngestionEngine rebuiltEngine = new IngestionEngine(registry, rebuiltWatermark,
                skipped, sink, RetryPolicy.noRetry());
        IngestionResult again = rebuiltEngine.ingestOnce(LocalTableSource.TYPE, "tbl-1", 100,
                FailureStrategy.SKIP);
        log.info("重复采集输入=水位丢失后从零重拉10条 | 判定依据=dedupKey全部已存在，重复={} 成功={} 总数={}",
                again.duplicateCount(), again.succeededCount(), sink.totalCount());
        assertEquals(10, again.duplicateCount());
        assertEquals(0, again.succeededCount());
        assertEquals(10, sink.totalCount());
        assertEquals(10L, rebuiltWatermark.load("tbl-1").orElseThrow());
        // 原水位存储未受影响。
        assertEquals(10L, wm.load("tbl-1").orElseThrow());
    }

    @Test
    void partialFailureIsIsolatedWithSkipAndCanBeReclaimed() {
        LocalTableSource source = new LocalTableSource("tbl-2");
        for (long i = 1; i <= 5; i++) {
            source.addRow(i, row(i, "id-" + i, "name-" + i));
        }
        LocalSourceRegistry registry = new LocalSourceRegistry();
        registry.register(source);
        LocalSink sink = new LocalSink();
        // 第 3 条写入先失败 1 次（不重试 => 直接隔离），之后恢复。
        sink.failNextWrites("tbl-2:id-3", 1, "模拟瞬时IO错误");
        InMemoryWatermarkStore wm = new InMemoryWatermarkStore();
        InMemorySkippedStore skipped = new InMemorySkippedStore();
        IngestionEngine engine = new IngestionEngine(registry, wm, skipped, sink,
                RetryPolicy.noRetry());

        IngestionResult result = engine.ingestOnce(LocalTableSource.TYPE, "tbl-2", 100,
                FailureStrategy.SKIP);
        log.info("部分失败批次 | 成功={} 跳过={} 失败={} | 判定依据=id-3首次失败被SKIP隔离，其余不受影响",
                result.succeededCount(), result.skippedCount(), result.failedCount());
        assertEquals(4, result.succeededCount());
        assertEquals(1, result.skippedCount());
        assertEquals(0, result.failedCount());
        assertEquals(4, sink.totalCount());
        assertEquals(1, skipped.count());
        // 被跳过的记录已隔离，水位可以越过它继续推进。
        assertEquals(5L, result.advancedWatermark());

        ReclaimResult reclaim = engine.reclaimSkipped("tbl-2");
        log.info("重捞输入=隔离区1条 | 判定依据=故障已恢复，幂等写回，恢复={} 仍失败={} 总数={}",
                reclaim.recoveredCount(), reclaim.stillFailedCount(), sink.totalCount());
        assertEquals(1, reclaim.recoveredCount());
        assertEquals(0, reclaim.stillFailedCount());
        assertEquals(0, skipped.count());
        assertEquals(5, sink.totalCount());

        // 再重捞一次：目标端幂等，隔离区已空，不产生重复。
        ReclaimResult reclaimAgain = engine.reclaimSkipped("tbl-2");
        assertEquals(0, reclaimAgain.recoveredCount());
        assertEquals(5, sink.totalCount());
    }

    @Test
    void retrySucceedsWithinAttempts() {
        LocalTableSource source = new LocalTableSource("tbl-3");
        source.addRow(1L, row(1, "id-1", "name-1"));
        source.addRow(2L, row(2, "id-2", "name-2"));
        LocalSourceRegistry registry = new LocalSourceRegistry();
        registry.register(source);
        LocalSink sink = new LocalSink();
        // id-1 前 2 次失败，第 3 次成功。
        sink.failNextWrites("tbl-3:id-1", 2, "模拟前两次失败");
        IngestionEngine engine = new IngestionEngine(registry, new InMemoryWatermarkStore(),
                new InMemorySkippedStore(), sink, new RetryPolicy(3, 0L));

        IngestionResult result = engine.ingestOnce(LocalTableSource.TYPE, "tbl-3", 100,
                FailureStrategy.SKIP);
        log.info("重试输入=id-1失败2次，策略=最多3次 | 判定依据=第3次尝试成功，成功={} 跳过={}",
                result.succeededCount(), result.skippedCount());
        assertEquals(2, result.succeededCount());
        assertEquals(0, result.skippedCount());
        assertEquals(2, sink.totalCount());
        assertTrue(sink.containsKey("tbl-3:id-1"));
    }

    @Test
    void failStrategyBlocksWatermarkButDoesNotCrashBatch() {
        LocalTableSource source = new LocalTableSource("tbl-4");
        for (long i = 1; i <= 3; i++) {
            source.addRow(i, row(i, "id-" + i, "name-" + i));
        }
        LocalSourceRegistry registry = new LocalSourceRegistry();
        registry.register(source);
        LocalSink sink = new LocalSink();
        // id-2 永久失败：FAIL 策略下水位必须卡在 2 之前，但 id-1 已成功、id-3 也尝试过。
        sink.failNextWrites("tbl-4:id-2", Integer.MAX_VALUE, "模拟永久故障");
        InMemoryWatermarkStore wm = new InMemoryWatermarkStore();
        IngestionEngine engine = new IngestionEngine(registry, wm, new InMemorySkippedStore(),
                sink, RetryPolicy.noRetry());

        IngestionResult result = engine.ingestOnce(LocalTableSource.TYPE, "tbl-4", 100,
                FailureStrategy.FAIL);
        log.info("FAIL策略输入=id-2永久失败 | 判定依据=单条失败不带崩整批，id-1/id-3均尝试，成功={} 失败={} 水位={}",
                result.succeededCount(), result.failedCount(), result.advancedWatermark());
        // id-1、id-3 写入成功，id-2 失败；整批没崩。
        assertEquals(2, result.succeededCount());
        assertEquals(1, result.failedCount());
        // 连续确认前缀在 id-2 处断开，水位只能卡在 1，不会越过失败点。
        assertEquals(1L, result.advancedWatermark());
        assertFalse(sink.containsKey("tbl-4:id-2"));

        // 再来一轮：从水位 1 继续，id-2 仍失败，id-3 命中幂等重复，不漏数据也不重复。
        IngestionResult second = engine.ingestOnce(LocalTableSource.TYPE, "tbl-4", 100,
                FailureStrategy.FAIL);
        log.info("续跑输入=水位1之后 | 判定依据=id-2重试仍失败，id-3已存在命中重复，成功={} 重复={} 水位={}",
                second.succeededCount(), second.duplicateCount(), second.advancedWatermark());
        assertEquals(0, second.succeededCount());
        assertEquals(1, second.duplicateCount());
        assertEquals(1, second.failedCount());
        assertEquals(1L, second.advancedWatermark());
        assertEquals(2, sink.totalCount());
    }
}
