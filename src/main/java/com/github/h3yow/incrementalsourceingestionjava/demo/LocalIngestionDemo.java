package com.github.h3yow.incrementalsourceingestionjava.demo;

import com.github.h3yow.incrementalsourceingestionjava.engine.IngestionEngine;
import com.github.h3yow.incrementalsourceingestionjava.model.FailureStrategy;
import com.github.h3yow.incrementalsourceingestionjava.model.IngestionResult;
import com.github.h3yow.incrementalsourceingestionjava.model.ReclaimResult;
import com.github.h3yow.incrementalsourceingestionjava.retry.RetryPolicy;
import com.github.h3yow.incrementalsourceingestionjava.sink.LocalSink;
import com.github.h3yow.incrementalsourceingestionjava.skipped.FileSkippedStore;
import com.github.h3yow.incrementalsourceingestionjava.source.local.LocalFileSource;
import com.github.h3yow.incrementalsourceingestionjava.source.LocalSourceRegistry;
import com.github.h3yow.incrementalsourceingestionjava.watermark.FileWatermarkStore;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 纯本地可运行的演示（不依赖 Spring / 数据库）：
 * 在临时目录里用文件水位 + 文件隔离区，跑一遍含拒绝、失败、重捞的采集流程。
 * 可直接在 IDE 运行 main。
 */
public final class LocalIngestionDemo {

    private LocalIngestionDemo() {
    }

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("ingestion-demo-");
        String sourceId = "demo-file";

        LocalFileSource source = new LocalFileSource(sourceId);
        source.addRecord(row(1, "a", "alice", "2026-09-29T10:00:01Z"));
        source.addRecord(row(2, "b", null, "2026-09-29T10:00:02Z"));      // 缺 name
        source.addRecord(row(3, null, "carol", "2026-09-29T10:00:03Z"));  // 缺 id
        source.addRecord(row(4, "d", "dave", "bad-time"));                // 时间格式错
        source.addRecord(row(5, "e", "erin", "2026-09-29T10:00:05Z"));    // 首次写入失败

        LocalSourceRegistry registry = new LocalSourceRegistry();
        registry.register(source);

        LocalSink sink = new LocalSink();
        sink.failNextWrites(sourceId + ":e", 1, "演示用瞬时故障");

        FileWatermarkStore wm = new FileWatermarkStore(dir);
        FileSkippedStore skipped = new FileSkippedStore(dir);
        IngestionEngine engine = new IngestionEngine(registry, wm, skipped, sink,
                RetryPolicy.noRetry());

        print("== 首次采集（部分拒绝 + 一条失败隔离） ==");
        IngestionResult r1 = engine.ingestOnce(
                LocalFileSource.TYPE, sourceId, 100, FailureStrategy.SKIP);
        printResult(r1);
        r1.rejected().forEach(rr ->
                print("  拒绝: seq=" + rr.rawRecord().rawSequence()
                        + " 原因=" + rr.reason() + " - " + rr.detail()));

        print("== 重捞隔离区（故障已恢复） ==");
        ReclaimResult reclaim = engine.reclaimSkipped(sourceId);
        print("  恢复=" + reclaim.recoveredCount()
                + " 仍失败=" + reclaim.stillFailedCount());

        print("== 重启后再次采集（从磁盘水位恢复） ==");
        FileWatermarkStore wm2 = new FileWatermarkStore(dir);
        FileSkippedStore skipped2 = new FileSkippedStore(dir);
        IngestionEngine engine2 = new IngestionEngine(registry, wm2, skipped2, sink,
                RetryPolicy.noRetry());
        IngestionResult r2 = engine2.ingestOnce(
                LocalFileSource.TYPE, sourceId, 100, FailureStrategy.SKIP);
        printResult(r2);

        print("目标端总记录数=" + sink.totalCount()
                + "（a、e 两条成功，b/c/d 被永久拒绝不写入）");
        print("数据目录=" + dir);
    }

    private static void printResult(IngestionResult r) {
        print("  来源=" + r.sourceId()
                + " 水位 " + r.fromWatermark() + " -> " + r.advancedWatermark()
                + " | 成功=" + r.succeededCount()
                + " 重复=" + r.duplicateCount()
                + " 跳过=" + r.skippedCount()
                + " 拒绝=" + r.rejectedCount()
                + " 失败=" + r.failedCount());
    }

    private static Map<String, Object> row(Object seq, String id, String name, String eventTime) {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("sequence", seq);
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

    private static void print(String s) {
        System.out.println(s);
    }
}
