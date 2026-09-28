package com.github.h3yow.incrementalsourceingestionjava.tests;

import com.github.h3yow.incrementalsourceingestionjava.local.FileWatermarkStore;
import com.github.h3yow.incrementalsourceingestionjava.watermark.Watermark;
import com.github.h3yow.incrementalsourceingestionjava.watermark.WatermarkConflictException;
import com.github.h3yow.incrementalsourceingestionjava.watermark.WatermarkRegressionException;
import com.github.h3yow.incrementalsourceingestionjava.watermark.WatermarkStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 水位记录/恢复：回退明确拒绝、版本号 CAS 防并发覆盖、文件落盘支持重启恢复。
 */
class WatermarkTest {

    private static final Logger log = LoggerFactory.getLogger(WatermarkTest.class);

    @TempDir
    Path dir;

    @Test
    @DisplayName("水位回退必须被明确拒绝")
    void shouldRejectRegression() {
        WatermarkStore store = new FileWatermarkStore(dir);
        Watermark w1 = store.commit("src-a", 0L, 100L);
        log.info("[输入] 先提交水位 100 (v{})；再尝试提交到 50", w1.version());
        WatermarkRegressionException ex = assertThrows(WatermarkRegressionException.class,
                () -> store.commit("src-a", w1.version(), 50L));
        log.info("[判定] 回退被拒绝: {}", ex.getMessage());
        assertEquals(100L, store.load("src-a").position(), "回退不得改动水位");
    }

    @Test
    @DisplayName("版本号不匹配时 CAS 提交冲突，防止并发互相覆盖")
    void shouldRejectStaleVersion() {
        WatermarkStore store = new FileWatermarkStore(dir);
        store.commit("src-a", 0L, 100L);
        // 两个拉取方都读到 v1，一方先提交成功
        Watermark ok = store.commit("src-a", 1L, 200L);
        log.info("[输入] 拉取方A 用 v1 提交 200 成功(v{})；拉取方B 仍用旧 v1 提交 150", ok.version());
        WatermarkConflictException ex = assertThrows(WatermarkConflictException.class,
                () -> store.commit("src-a", 1L, 150L));
        log.info("[判定] 陈旧版本被拒绝: {}", ex.getMessage());
        assertEquals(200L, store.load("src-a").position(), "断点不能被并发提交覆盖");
        assertEquals(2L, store.load("src-a").version());
    }

    @Test
    @DisplayName("水位落盘后，重新实例化（模拟进程重启）可恢复")
    void shouldSurviveRestart() {
        new FileWatermarkStore(dir).commit("src-a", 0L, 42L);
        WatermarkStore restarted = new FileWatermarkStore(dir);
        Watermark wm = restarted.load("src-a");
        log.info("[输入] 重启后重新加载水位文件；[判定] position={} version={}", wm.position(), wm.version());
        assertEquals(42L, wm.position());
        assertEquals(1L, wm.version());
        assertTrue(wm.version() > 0);
    }
}
