package com.github.h3yow.incrementalsourceingestionjava.local;

import com.github.h3yow.incrementalsourceingestionjava.model.RawRecord;
import com.github.h3yow.incrementalsourceingestionjava.service.BatchResult;
import com.github.h3yow.incrementalsourceingestionjava.service.IngestionService;
import com.github.h3yow.incrementalsourceingestionjava.service.RecordFailureInjector;
import com.github.h3yow.incrementalsourceingestionjava.source.SourceRegistry;
import com.github.h3yow.incrementalsourceingestionjava.source.SourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本地端到端演示：用 --spring.profiles.active=demo 启动。
 * 演示 正常增量 / 坏数据被区分拒绝 / 瞬时失败重试 / 持续失败跳过隔离 / 捞回 / 重复采集去重。
 */
@Component
@Profile("demo")
public class LocalDemoRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(LocalDemoRunner.class);

    private final SourceRegistry registry;
    private final IngestionService service;

    public LocalDemoRunner(SourceRegistry registry, IngestionService service) {
        this.registry = registry;
        this.service = service;
    }

    @Override
    public void run(String... args) {
        String source = "demo-orders";
        SourceType t = SimpleSourceType.of("orders");

        // 好数据 + 各类坏数据（用于验证拒绝原因互不相同）
        List<RawRecord> records = List.of(
                new RawRecord("orders", source, 1L, Map.of("id", "O1", "name", "订单1")),
                new RawRecord("orders", source, 2L, Map.of("id", "O2", "name", "订单2")),
                new RawRecord("orders", source, 3L, Map.of("name", "缺主键")),
                new RawRecord("orders", source, 4L, Map.of("id", "O4")),
                new RawRecord("orders", source, 5L, Map.of("id", "O5", "name", "订单5", "eventTime", "bad-time"))
        );
        registry.registerConnector(new InMemorySourceConnector(t, source, records));

        // O2 注入 2 次瞬时失败后成功（验证重试）
        AtomicInteger o2Fail = new AtomicInteger(2);
        RecordFailureInjector injector = r -> {
            if ("O2".equals(r.businessKey()) && o2Fail.getAndDecrement() > 0) {
                throw new RuntimeException("模拟目标端瞬时超时");
            }
        };
        // 演示使用独立 service 实例注入失败器（落盘目录与主装配相同）
        IngestionService demoService = new IngestionService(registry,
                new com.github.h3yow.incrementalsourceingestionjava.normalize.Normalizer(registry),
                new FileWatermarkStore(), new FileTargetSink(), new FileIsolationStore(),
                new com.github.h3yow.incrementalsourceingestionjava.policy.DefaultRetryPolicy(3),
                injector);

        BatchResult r1 = demoService.runBatch(source, 100);
        log.info("[演示] 首批: 总{} 成功{} 失败{} 跳过{} 重复{}",
                r1.total(), r1.success(), r1.failed(), r1.skipped(), r1.duplicate());

        BatchResult r2 = demoService.runBatch(source, 100);
        log.info("[演示] 重复采集: 总{} 成功{} 重复{}", r2.total(), r2.success(), r2.duplicate());

        BatchResult rec = demoService.recoverPending(source);
        log.info("[演示] 捞回隔离记录: 总{} 成功{}", rec.total(), rec.success());
    }
}
