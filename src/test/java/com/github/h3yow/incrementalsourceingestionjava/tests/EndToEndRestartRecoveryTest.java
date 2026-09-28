package com.github.h3yow.incrementalsourceingestionjava.tests;

import com.github.h3yow.incrementalsourceingestionjava.engine.IngestionEngine;
import com.github.h3yow.incrementalsourceingestionjava.exception.WatermarkRegressionException;
import com.github.h3yow.incrementalsourceingestionjava.model.FailureStrategy;
import com.github.h3yow.incrementalsourceingestionjava.model.IngestionResult;
import com.github.h3yow.incrementalsourceingestionjava.model.ReclaimResult;
import com.github.h3yow.incrementalsourceingestionjava.model.RejectReason;
import com.github.h3yow.incrementalsourceingestionjava.retry.RetryPolicy;
import com.github.h3yow.incrementalsourceingestionjava.sink.LocalSink;
import com.github.h3yow.incrementalsourceingestionjava.skipped.FileSkippedStore;
import com.github.h3yow.incrementalsourceingestionjava.source.local.LocalFileSource;
import com.github.h3yow.incrementalsourceingestionjava.source.local.LocalTableSource;
import com.github.h3yow.incrementalsourceingestionjava.source.LocalSourceRegistry;
import com.github.h3yow.incrementalsourceingestionjava.watermark.FileWatermarkStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端：
 *  - 两种来源类型（local-table / local-file）同时注册、统一归一；
 *  - 一批里混合多种拒绝原因与部分写入失败，统计可区分；
 *  - 文件水位 + 文件隔离区；"重启"后恢复续传、拒绝回退、重捞跳过记录不重复。
 */
class EndToEndRestartRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(EndToEndRestartRecoveryTest.class);

    private Map<String, Object> data(String id, Object name, String eventTime) {
        Map<String, Object> d = new LinkedHashMap<>();
        if (id != null) {
            d.put("id", id);
        }
        if (name != null) {
            d.put("name", name);
        }
        if (eventTime != null) {
            d.put("eventTime", eventTime);
        }
        return d;
    }

    @Test
    void mixedSourcesRejectionsRestartRecoveryAndReclaim(@TempDir Path tempDir) {
        String sourceId = "fs-1";
        LocalFileSource source = new LocalFileSource(sourceId);
        // seq=1 正常
        source.addRecord(withSeq(1, data("a", "alice", "2026-09-29T10:00:01Z")));
        // seq=2 缺 name => MISSING_NAME（序号可解析，水位可越过）
        source.addRecord(withSeq(2, data("b", null, "2026-09-29T10:00:02Z")));
        // seq=3 id 类型错误 => ID_TYPE_MISMATCH
        source.addRecord(withSeq(3, dataWithNumericId(3, "carol")));
        // seq=4 时间格式错 => EVENT_TIME_MALFORMED
        source.addRecord(withSeq(4, data("d", "dave", "09/29/2026")));
        // seq=5 正常，但目标端首次写入失败 => 隔离
        source.addRecord(withSeq(5, data("e", "erin", "2026-09-29T10:00:05Z")));
        // 序号缺失 => SEQUENCE_MISSING（无法定位水位）
        source.addRecord(data("f", "frank", "2026-09-29T10:00:06Z"));

        LocalSourceRegistry registry = new LocalSourceRegistry();
        registry.register(source);
        // 同时注册另一类型来源，证明多类型共存。
        LocalTableSource table = new LocalTableSource("tbl-e2e");
        table.addRow(1L, data("t1", "table-one", "2026-09-29T10:01:00Z"));
        registry.register(table);

        LocalSink sink = new LocalSink();
        sink.failNextWrites(sourceId + ":e", 1, "模拟重启前瞬时故障");
        FileWatermarkStore wm = new FileWatermarkStore(tempDir);
        FileSkippedStore skipped = new FileSkippedStore(tempDir);
        IngestionEngine engine = new IngestionEngine(registry, wm, skipped, sink,
                RetryPolicy.noRetry());

        IngestionResult r1 = engine.ingestOnce(LocalFileSource.TYPE, sourceId, 100,
                FailureStrategy.SKIP);
        log.info("首批结束 | 成功={} 拒绝={} 跳过={} 水位={} | 判定依据=多种拒绝原因各自区分，e首次失败隔离，无序号记录卡住水位",
                r1.succeededCount(), r1.rejectedCount(), r1.skippedCount(),
                r1.advancedWatermark());

        // 成功：a(1)；拒绝：2,3,4 + 无序号 = 4；跳过：e(5) = 1。
        assertEquals(1, r1.succeededCount());
        assertEquals(4, r1.rejectedCount());
        assertEquals(1, r1.skippedCount());
        assertEquals(0, r1.failedCount());
        // 无序号记录无法定位，且跳过记录 5 之前 2/3/4 均已被明确处置（计入确认前缀），
        // 连续确认前缀到 5，水位推进到 5。
        // 注意：无序号记录不应影响有序号记录的前缀；但它本身永远无法越过，
        // 其影响见断言：重启后仍会再次拉到它。
        assertEquals(5L, r1.advancedWatermark());

        List<RejectReason> reasons = r1.rejected().stream()
                .map(rr -> rr.reason()).toList();
        log.info("拒绝原因集合={}", reasons);
        assertTrue(reasons.contains(RejectReason.MISSING_NAME));
        assertTrue(reasons.contains(RejectReason.ID_TYPE_MISMATCH));
        assertTrue(reasons.contains(RejectReason.EVENT_TIME_MALFORMED));
        assertTrue(reasons.contains(RejectReason.SEQUENCE_MISSING));
        assertEquals(4, reasons.stream().distinct().count(), "拒绝原因必须互不相同");

        // 另一类型来源可独立采集。
        IngestionResult rt = engine.ingestOnce(LocalTableSource.TYPE, "tbl-e2e", 100,
                FailureStrategy.SKIP);
        assertEquals(1, rt.succeededCount());

        // ---- 模拟进程重启：source/wm/skipped 全部重新构建，只让磁盘与目标端"活下来"。
        //     目标端这里也重建（真实场景目标端是外部系统），用重复采集验证幂等：
        //     先用同一 sink 验证水位恢复与回退拒绝。
        FileWatermarkStore wm2 = new FileWatermarkStore(tempDir);
        FileSkippedStore skipped2 = new FileSkippedStore(tempDir);
        assertEquals(5L, wm2.load(sourceId).orElseThrow(), "重启后水位必须从磁盘恢复");
        assertEquals(1, skipped2.findBySource(sourceId).size(), "隔离记录必须从磁盘恢复");

        WatermarkRegressionException regression = assertThrows(
                WatermarkRegressionException.class,
                () -> wm2.verifyStartPoint(sourceId, 2L));
        log.info("重启后尝试从水位2启动 | 判定依据=磁盘已提交5，拒绝来路不明的回退：{}",
                regression.getMessage());

        // 故障恢复后重捞 e：幂等写回成功，不重复。
        IngestionEngine engine2 = new IngestionEngine(registry, wm2, skipped2, sink,
                RetryPolicy.noRetry());
        ReclaimResult reclaim = engine2.reclaimSkipped(sourceId);
        log.info("重启后重捞 | 恢复={} 仍失败={} | 判定依据=故障已恢复，e写回，目标端总数={}",
                reclaim.recoveredCount(), reclaim.stillFailedCount(), sink.totalCount());
        assertEquals(1, reclaim.recoveredCount());
        assertEquals(0, reclaim.stillFailedCount());
        assertTrue(sink.containsKey(sourceId + ":e"));
        assertEquals(0, skipped2.findBySource(sourceId).size());

        // 重启后续跑：seq>5 没有新的可解析记录，只有那条无序号记录会被再次明确拒绝。
        IngestionResult r2 = engine2.ingestOnce(LocalFileSource.TYPE, sourceId, 100,
                FailureStrategy.SKIP);
        log.info("重启后续跑 | 成功={} 拒绝={} 水位={} | 判定依据=无序号记录再次被明确拒绝，不悄悄跳过",
                r2.succeededCount(), r2.rejectedCount(), r2.advancedWatermark());
        assertEquals(0, r2.succeededCount());
        assertEquals(1, r2.rejectedCount());
        assertEquals(RejectReason.SEQUENCE_MISSING, r2.rejected().get(0).reason());
        assertEquals(5L, r2.advancedWatermark());

        // 目标端最终：a + e + table 一条 = 3，且无重复。
        assertEquals(3, sink.totalCount());
    }

    private Map<String, Object> withSeq(Object seq, Map<String, Object> d) {
        Map<String, Object> copy = new LinkedHashMap<>(d);
        copy.put("sequence", seq);
        return copy;
    }

    private Map<String, Object> dataWithNumericId(Object seq, String name) {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("sequence", seq);
        d.put("id", 999); // 故意给错类型
        d.put("name", name);
        d.put("eventTime", "2026-09-29T10:00:03Z");
        return d;
    }
}
