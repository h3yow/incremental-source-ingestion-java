package com.github.h3yow.incrementalsourceingestionjava.tests;

import com.github.h3yow.incrementalsourceingestionjava.engine.IngestionEngine;
import com.github.h3yow.incrementalsourceingestionjava.model.FailureStrategy;
import com.github.h3yow.incrementalsourceingestionjava.model.IngestionResult;
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 同一来源被并发拉取：
 * 同一把来源锁 + 水位条件提交，保证断点不相互覆盖、最终恰好每条写一次、
 * 水位收敛到最大序号。不同来源互不影响。
 */
class ConcurrentIngestionTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentIngestionTest.class);

    private Map<String, Object> row(long seq) {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("id", "id-" + seq);
        d.put("name", "name-" + seq);
        d.put("eventTime", "2026-09-29T10:00:" + String.format("%02d", seq % 60) + "Z");
        return d;
    }

    @Test
    void concurrentPullsOfSameSourceProduceConsistentResult() throws Exception {
        final int total = 200;
        final int threads = 8;
        LocalTableSource source = new LocalTableSource("conc-1");
        for (long i = 1; i <= total; i++) {
            source.addRow(i, row(i));
        }
        LocalSourceRegistry registry = new LocalSourceRegistry();
        registry.register(source);
        LocalSink sink = new LocalSink();
        InMemoryWatermarkStore wm = new InMemoryWatermarkStore();
        InMemorySkippedStore skipped = new InMemorySkippedStore();
        IngestionEngine engine = new IngestionEngine(registry, wm, skipped, sink,
                RetryPolicy.noRetry());

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        ConcurrentLinkedQueue<IngestionResult> results = new ConcurrentLinkedQueue<>();
        AtomicInteger errors = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    // 每个线程循环拉小批次直到无数据，模拟并发抢同一来源。
                    while (true) {
                        IngestionResult r = engine.ingestOnce(LocalTableSource.TYPE,
                                "conc-1", 13, FailureStrategy.SKIP);
                        results.add(r);
                        if (r.succeededCount() + r.duplicateCount() == 0) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    log.error("并发采集线程异常", e);
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdown();

        int totalSucceeded = results.stream().mapToInt(IngestionResult::succeededCount).sum();
        int totalDuplicate = results.stream().mapToInt(IngestionResult::duplicateCount).sum();

        log.info("并发输入={}条/{}线程 | 判定依据=来源锁串行化+水位CAS，新写入={} 幂等重复={} 异常={} 目标端总数={} 最终水位={}",
                total, threads, totalSucceeded, totalDuplicate, errors.get(),
                sink.totalCount(), wm.load("conc-1").orElse(-1L));

        assertEquals(0, errors.get(), "并发过程不应出现未捕获异常");
        assertEquals(total, sink.totalCount(), "目标端必须恰好每条一份，无重复无漏");
        // 所有批次的"新写入"合计必须恰好等于记录数（其余是幂等重复）。
        assertEquals(total, totalSucceeded, "新写入总数必须等于记录数");
        assertEquals(total, wm.load("conc-1").orElseThrow(), "水位必须收敛到最大序号");
        assertEquals(0, skipped.count(), "不应产生跳过记录");
        // 每条都在目标端，且序号连续。
        for (long i = 1; i <= total; i++) {
            assertTrue(sink.containsKey("conc-1:id-" + i), "缺少记录 id-" + i);
        }
    }
}
