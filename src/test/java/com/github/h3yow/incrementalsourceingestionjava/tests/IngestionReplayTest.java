package com.github.h3yow.incrementalsourceingestionjava.tests;

import com.github.h3yow.incrementalsourceingestionjava.isolation.IsolationStore;
import com.github.h3yow.incrementalsourceingestionjava.local.DefaultSourceRegistry;
import com.github.h3yow.incrementalsourceingestionjava.local.FileIsolationStore;
import com.github.h3yow.incrementalsourceingestionjava.local.FileTargetSink;
import com.github.h3yow.incrementalsourceingestionjava.local.FileWatermarkStore;
import com.github.h3yow.incrementalsourceingestionjava.local.InMemorySourceConnector;
import com.github.h3yow.incrementalsourceingestionjava.local.SimpleSourceType;
import com.github.h3yow.incrementalsourceingestionjava.model.RawRecord;
import com.github.h3yow.incrementalsourceingestionjava.normalize.Normalizer;
import com.github.h3yow.incrementalsourceingestionjava.policy.DefaultRetryPolicy;
import com.github.h3yow.incrementalsourceingestionjava.service.BatchResult;
import com.github.h3yow.incrementalsourceingestionjava.service.IngestionService;
import com.github.h3yow.incrementalsourceingestionjava.service.ItemOutcome;
import com.github.h3yow.incrementalsourceingestionjava.service.RecordFailureInjector;
import com.github.h3yow.incrementalsourceingestionjava.sink.TargetSink;
import com.github.h3yow.incrementalsourceingestionjava.source.SourceType;
import com.github.h3yow.incrementalsourceingestionjava.watermark.WatermarkStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 重复采集幂等 + 单条失败不拖垮整批 + 失败计数 + 跳过记录可重新捞回且不重复写入。
 */
class IngestionReplayTest {

    private static final Logger log = LoggerFactory.getLogger(IngestionReplayTest.class);

    @TempDir
    Path dir;

    private SourceRegistryBox box;

    record SourceRegistryBox(DefaultSourceRegistry registry, IngestionService service,
                             FileTargetSink sink, WatermarkStore wmStore,
                             IsolationStore isoStore) {
    }

    private RawRecord rec(long cursor, String id, String name) {
        return rec("orders", "orders-a", cursor, id, name);
    }

    private RawRecord rec(String type, String sourceName, long cursor, String id, String name) {
        return new RawRecord(type, sourceName, cursor, Map.of("id", id, "name", name));
    }

    @BeforeEach
    void setUp() {
        DefaultSourceRegistry registry = new DefaultSourceRegistry();
        SourceType t = SimpleSourceType.of("orders");
        // 3 条好数据
        List<RawRecord> records = List.of(
                rec(1L, "K1", "订单1"),
                rec(2L, "K2", "订单2"),
                rec(3L, "K3", "订单3"));
        registry.registerConnector(new InMemorySourceConnector(t, "orders-a", records));

        WatermarkStore wm = new FileWatermarkStore(dir);
        FileTargetSink sink = new FileTargetSink(dir);
        IsolationStore iso = new FileIsolationStore(dir);
        IngestionService service = new IngestionService(registry, new Normalizer(registry),
                wm, sink, iso, new DefaultRetryPolicy(3));
        box = new SourceRegistryBox(registry, service, sink, wm, iso);
    }

    @Test
    @DisplayName("重复采集：同一批跑两遍，目标端不重复、不漏数据")
    void shouldBeIdempotentOnReplay() {
        BatchResult first = box.service().runBatch("orders-a", 100);
        log.info("[输入] 首次采集 3 条；[判定] 成功={} 失败={} 跳过={} 重复={} 水位后={}",
                first.success(), first.failed(), first.skipped(), first.duplicate(), first.watermarkAfter());
        assertEquals(3, first.success());
        assertEquals(0, first.duplicate());
        assertEquals(3, box.sink().size("orders-a"));

        BatchResult second = box.service().runBatch("orders-a", 100);
        log.info("[输入] 水位 {} 处重复采集；[判定] 成功={} 失败={} 跳过={} 重复={}",
                box.wmStore().load("orders-a").position(),
                second.success(), second.failed(), second.skipped(), second.duplicate());
        // 第二次从水位增量拉取，源端游标都 <= 水位，因此无新数据；目标端仍是 3 条
        assertEquals(0, second.success());
        assertEquals(0, second.duplicate());
        assertEquals(3, box.sink().size("orders-a"));

        // 模拟"同一条数据被重复推送"：手动再写一遍 K1，验证目标端去重
        boolean isNew = box.sink().put(new com.github.h3yow.incrementalsourceingestionjava.model.NormalizedRecord(
                "orders", "orders-a", "K1", 1L, null, Map.of("name", "订单1")));
        log.info("[输入] 重复推送业务主键 K1；[判定] isNew={}（false=去重命中）", isNew);
        assertEquals(false, isNew);
        assertEquals(3, box.sink().size("orders-a"));
    }

    @Test
    @DisplayName("部分失败：一条处理失败被跳过隔离，整批不崩，计数正确，之后捞回成功不重复")
    void shouldIsolateAndRecoverSingleFailure() {
        // 让 K2 前两次处理失败（瞬时），第 3 次成功 -> 重试后成功
        AtomicInteger k2Failures = new AtomicInteger(2);
        RecordFailureInjector injector = record -> {
            if ("K2".equals(record.businessKey()) && k2Failures.getAndDecrement() > 0) {
                throw new RuntimeException("目标端瞬时超时(模拟)");
            }
        };
        IngestionService svc = new IngestionService(box.registry(), new Normalizer(box.registry()),
                box.wmStore(), box.sink(), box.isoStore(), new DefaultRetryPolicy(3), injector);

        BatchResult r1 = svc.runBatch("orders-a", 100);
        log.info("[输入] K2 注入 2 次瞬时失败；[判定] 成功={} 失败={} 跳过={} 重复={}",
                r1.success(), r1.failed(), r1.skipped(), r1.duplicate());
        assertEquals(3, r1.success());
        assertEquals(0, r1.failed());
        assertEquals(0, r1.skipped());
        assertEquals(3, box.sink().size("orders-a"));
        ItemResultAttempt(r1, "K2", 3);

        // 再造一个必失败场景：K? 始终失败 -> 重试耗尽跳过并隔离
        AtomicInteger alwaysFail = new AtomicInteger(Integer.MAX_VALUE);
        List<RawRecord> more = List.of(rec("orders", "orders-b", 4L, "K4", "订单4"));
        DefaultSourceRegistry reg2 = new DefaultSourceRegistry();
        reg2.registerConnector(new InMemorySourceConnector(SimpleSourceType.of("orders"), "orders-b", more));
        FileTargetSink sink2 = new FileTargetSink(dir.resolve("b"));
        FileWatermarkStore wm2 = new FileWatermarkStore(dir.resolve("b"));
        FileIsolationStore iso2 = new FileIsolationStore(dir.resolve("b"));
        RecordFailureInjector alwaysInjector = record -> {
            throw new RuntimeException("目标端持续不可用(模拟)");
        };
        IngestionService svc2 = new IngestionService(reg2, new Normalizer(reg2), wm2, sink2, iso2,
                new DefaultRetryPolicy(3), alwaysInjector);
        BatchResult r2 = svc2.runBatch("orders-b", 100);
        log.info("[输入] K4 注入持续失败；[判定] 成功={} 失败={} 跳过={} 重复={} K4尝试次数={}",
                r2.success(), r2.failed(), r2.skipped(), r2.duplicate(),
                r2.items().get(0).attempts());
        assertEquals(0, r2.success());
        assertEquals(1, r2.skipped());
        assertEquals(3, r2.items().get(0).attempts(), "应重试到上限 3 次");
        assertEquals(0, sink2.size("orders-b"), "跳过记录不得写入目标端");
        assertEquals(1, iso2.fetchPending("orders-b").size(), "隔离区应有 1 条待处理");

        // 故障恢复后捞回：目标端写入 1 条，隔离记录标 RECOVERED；重复捞回不重复写入
        IngestionService svc2Healthy = new IngestionService(reg2, new Normalizer(reg2), wm2, sink2, iso2,
                new DefaultRetryPolicy(3));
        BatchResult rec1 = svc2Healthy.recoverPending("orders-b");
        log.info("[输入] 故障恢复后捞回；[判定] 成功={} 跳过={} 重复={}",
                rec1.success(), rec1.skipped(), rec1.duplicate());
        assertEquals(1, rec1.success());
        assertEquals(1, sink2.size("orders-b"));
        assertEquals(0, iso2.fetchPending("orders-b").size(), "捞回后隔离区无待处理");

        BatchResult rec2 = svc2Healthy.recoverPending("orders-b");
        log.info("[输入] 再次捞回（隔离区已空）；[判定] 总数={} 成功={}", rec2.total(), rec2.success());
        assertEquals(0, rec2.total());
        assertEquals(1, sink2.size("orders-b"), "目标端仍只有 1 条，不重复");
    }

    private void ItemResultAttempt(BatchResult r, String key, int attempts) {
        r.items().stream()
                .filter(i -> key.equals(i.businessKey()))
                .findFirst()
                .ifPresent(i -> {
                    assertEquals(attempts, i.attempts(), key + " 尝试次数不符");
                    assertEquals(ItemOutcome.SUCCESS, i.outcome());
                });
    }
}
