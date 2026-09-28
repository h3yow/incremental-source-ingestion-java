package com.github.h3yow.incrementalsourceingestionjava.tests;

import com.github.h3yow.incrementalsourceingestionjava.exception.WatermarkRegressionException;
import com.github.h3yow.incrementalsourceingestionjava.watermark.FileWatermarkStore;
import com.github.h3yow.incrementalsourceingestionjava.watermark.InMemoryWatermarkStore;
import com.github.h3yow.incrementalsourceingestionjava.watermark.WatermarkStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 水位记录与恢复：
 * 1) 正常条件提交；2) 回退必须明确拒绝；3) 条件不匹配不允许覆盖；
 * 4) 进程"重启"（新建 store，只认磁盘）后水位可恢复。
 */
class WatermarkPersistenceTest {

    private static final Logger log = LoggerFactory.getLogger(WatermarkPersistenceTest.class);

    @Test
    void advanceAndConditionalCommit() {
        WatermarkStore store = new InMemoryWatermarkStore();
        assertTrue(store.compareAndAdvance("s1", 0L, 5L));
        log.info("输入=(expected=0,next=5) | 判定依据=当前水位0匹配期望值，提交成功");
        // 期望已过期：别人已把水位推到 5，本次条件提交失败但不抛异常。
        assertFalse(store.compareAndAdvance("s1", 0L, 5L));
        log.info("输入=(expected=0,next=5) | 判定依据=当前水位已为5，期望0过期，提交失败且不覆盖");
        assertEquals(5L, store.load("s1").orElseThrow());
        assertTrue(store.compareAndAdvance("s1", 5L, 8L));
        assertEquals(8L, store.load("s1").orElseThrow());
        log.info("输入=(expected=5,next=8) | 判定依据=条件匹配，水位推进到8");
    }

    @Test
    void regressionIsRejected() {
        WatermarkStore store = new InMemoryWatermarkStore();
        store.compareAndAdvance("s1", 0L, 10L);
        WatermarkRegressionException ex = assertThrows(WatermarkRegressionException.class,
                () -> store.compareAndAdvance("s1", 10L, 3L));
        log.info("输入=(已提交10, 请求3) | 判定依据=next<当前水位，判定为回退并拒绝");
        assertEquals(10L, ex.existingWatermark());
        assertEquals(3L, ex.requestedWatermark());
        assertEquals(10L, store.load("s1").orElseThrow());
    }

    @Test
    void startPointBehindCommittedIsRejected() {
        WatermarkStore store = new InMemoryWatermarkStore();
        store.compareAndAdvance("s1", 0L, 7L);
        assertThrows(WatermarkRegressionException.class,
                () -> store.verifyStartPoint("s1", 2L));
        log.info("输入=(已提交7, 请求从2启动) | 判定依据=启动点早于已提交水位，来路不明，拒绝");
        assertEquals(7L, store.verifyStartPoint("s1", 7L));
    }

    @Test
    void recoverAfterRestartWithFileStore(@TempDir Path tempDir) {
        String sourceId = "s-file";
        new FileWatermarkStore(tempDir).compareAndAdvance(sourceId, 0L, 11L);
        // 模拟进程重启：丢弃旧实例，新建的 store 只从磁盘恢复。
        FileWatermarkStore restarted = new FileWatermarkStore(tempDir);
        assertEquals(11L, restarted.load(sourceId).orElseThrow());
        log.info("重启后输入=source={} | 判定依据=从磁盘水位文件恢复，水位={}", sourceId, 11L);

        WatermarkRegressionException ex = assertThrows(WatermarkRegressionException.class,
                () -> restarted.compareAndAdvance(sourceId, 11L, 4L));
        log.info("重启后输入=(请求4) | 判定依据=磁盘水位11，回退被拒绝：{}", ex.getMessage());
        assertTrue(restarted.compareAndAdvance(sourceId, 11L, 12L));
    }
}
