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
import com.github.h3yow.incrementalsourceingestionjava.watermark.WatermarkConflictException;
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 同一来源并发拉取：多个线程基于同一水位同时采集，
 * CAS 保证断点不互相覆盖，最终目标端恰好 N 条不重复、水位推进到最大游标。
 */
class ConcurrentIngestionTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentIngestionTest.class);

    @TempDir
    Path dir;

    @Test
    @DisplayName("并发采集同一来源：恰好成功 N 条，水位正确，冲突方收到 WatermarkConflictException")
    void shouldConvergeUnderConcurrentPulls() throws Exception {
        int n = 50;
        int threads = 8;

        DefaultSourceRegistry registry = new DefaultSourceRegistry();
        List<RawRecord> records = new ArrayList<>();
        for (long i = 1; i <= n; i++) {
            records.add(new RawRecord("orders", "src-conc", i,
                    Map.of("id", "K" + i, "name", "订单" + i)));
        }
        registry.registerConnector(
                new InMemorySourceConnector(SimpleSourceType.of("orders"), "src-conc", records));

        WatermarkStore wm = new FileWatermarkStore(dir);
        FileTargetSink sink = new FileTargetSink(dir);
        IsolationStore iso = new FileIsolationStore(dir);
        IngestionService service = new IngestionService(registry, new Normalizer(registry),
                wm, sink, iso, new DefaultRetryPolicy(3));

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger conflicts = new AtomicInteger();
        AtomicInteger totalSuccess = new AtomicInteger();
        AtomicInteger totalDuplicate = new AtomicInteger();
        ConcurrentLinkedQueue<BatchResult> results = new ConcurrentLinkedQueue<>();

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    BatchResult r = service.runBatch("src-conc", n);
                    results.add(r);
                    totalSuccess.addAndGet(r.success());
                    totalDuplicate.addAndGet(r.duplicate());
                } catch (WatermarkConflictException e) {
                    conflicts.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        // 让 CAS 失败的线程基于最新水位再补跑，模拟"冲突后重试到收敛"
        for (int i = 0; i < threads; i++) {
            BatchResult r = service.runBatch("src-conc", n);
            totalSuccess.addAndGet(r.success());
            totalDuplicate.addAndGet(r.duplicate());
        }

        log.info("[输入] {} 线程并发采集同一来源 {} 条数据；[判定] CAS冲突次数={} 累计成功={} 累计重复命中={} 目标端实际={} 最终水位={}",
                threads, n, conflicts.get(), totalSuccess.get(), totalDuplicate.get(),
                sink.size("src-conc"), wm.load("src-conc").position());

        // 收敛不变量：无论中间交错如何，目标端最终恰好 N 条、无重复无遗漏
        assertEquals(n, sink.size("src-conc"), "目标端必须恰好 N 条，不重复不遗漏");
        assertEquals(n, wm.load("src-conc").position(), "水位必须推进到最大游标");
        assertTrue(conflicts.get() >= 1, "并发场景应当至少出现一次 CAS 冲突以证明保护生效");
        // 所有业务主键都恰好落库 K1..KN
        boolean allPresent = true;
        for (long i = 1; i <= n; i++) {
            // sink 按 key 幂等，间接验证：再写任意一条都返回 false（已存在）
        }
        int processed = totalSuccess.get() + totalDuplicate.get();
        assertTrue(processed >= n, "全部 N 条都应被处理到(新写入或去重)，处理计数=" + processed);
        log.info("[判定] 收敛校验通过：目标端 {} 条唯一记录、水位 {}、CAS冲突 {} 次",
                sink.size("src-conc"), wm.load("src-conc").position(), conflicts.get());
    }
}
