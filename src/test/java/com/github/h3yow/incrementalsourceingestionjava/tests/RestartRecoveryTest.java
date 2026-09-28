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
import com.github.h3yow.incrementalsourceingestionjava.watermark.WatermarkStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 进程重启恢复：水位 / 目标端 / 隔离区全部落盘，
 * 用全新 store 实例模拟重启后，增量采集从断点继续、不重复不遗漏。
 */
class RestartRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(RestartRecoveryTest.class);

    @TempDir
    Path dir;

    private IngestionService buildService(DefaultSourceRegistry registry,
                                          WatermarkStore wm, FileTargetSink sink, IsolationStore iso) {
        return new IngestionService(registry, new Normalizer(registry), wm, sink, iso,
                new DefaultRetryPolicy(3));
    }

    @Test
    @DisplayName("重启后从断点继续增量采集，目标端不重复不遗漏")
    void shouldResumeAfterRestart() {
        DefaultSourceRegistry registry = new DefaultSourceRegistry();
        List<RawRecord> records = new ArrayList<>();
        for (long i = 1; i <= 5; i++) {
            records.add(new RawRecord("orders", "src-restart", i,
                    Map.of("id", "K" + i, "name", "订单" + i)));
        }
        registry.registerConnector(
                new InMemorySourceConnector(SimpleSourceType.of("orders"), "src-restart", records));

        // 进程 1：采集前 3 条（用 limit 模拟分批）
        WatermarkStore wm1 = new FileWatermarkStore(dir);
        FileTargetSink sink1 = new FileTargetSink(dir);
        IsolationStore iso1 = new FileIsolationStore(dir);
        IngestionService app1 = buildService(registry, wm1, sink1, iso1);
        BatchResult b1 = app1.runBatch("src-restart", 3);
        log.info("[输入] 进程1 首批 limit=3；[判定] 成功={} 水位后={} 目标端={}",
                b1.success(), b1.watermarkAfter(), sink1.size("src-restart"));
        assertEquals(3, b1.success());
        assertEquals(3, sink1.size("src-restart"));

        // 进程 2：全新 store 实例（模拟 JVM 重启），从水位 3 继续
        WatermarkStore wm2 = new FileWatermarkStore(dir);
        FileTargetSink sink2 = new FileTargetSink(dir);
        IsolationStore iso2 = new FileIsolationStore(dir);
        IngestionService app2 = buildService(registry, wm2, sink2, iso2);
        long resumedAt = wm2.load("src-restart").position();
        BatchResult b2 = app2.runBatch("src-restart", 100);
        log.info("[输入] 进程2 重启，恢复水位={} 后继续；[判定] 新采集成功={} 重复={} 水位后={} 目标端={}",
                resumedAt, b2.success(), b2.duplicate(), b2.watermarkAfter(), sink2.size("src-restart"));
        assertEquals(3L, resumedAt, "重启后应恢复到断点 3");
        assertEquals(2, b2.success(), "应只增量拉到剩余 2 条");
        assertEquals(5, sink2.size("src-restart"), "目标端总共 5 条，不重复不遗漏");
        assertEquals(5L, wm2.load("src-restart").position());
    }
}
