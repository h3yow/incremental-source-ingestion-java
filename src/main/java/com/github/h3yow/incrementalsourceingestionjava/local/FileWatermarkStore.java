package com.github.h3yow.incrementalsourceingestionjava.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.incrementalsourceingestionjava.watermark.Watermark;
import com.github.h3yow.incrementalsourceingestionjava.watermark.WatermarkConflictException;
import com.github.h3yow.incrementalsourceingestionjava.watermark.WatermarkRegressionException;
import com.github.h3yow.incrementalsourceingestionjava.watermark.WatermarkStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * 文件水位存储（本地模拟重启恢复）：每来源一个 JSON 文件，
 * commit 走 CAS（版本校验 + 回退拒绝），写入用临时文件原子移动。
 */
public class FileWatermarkStore implements WatermarkStore {

    private static final Logger log = LoggerFactory.getLogger(FileWatermarkStore.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path baseDir;
    private final Object writeLock = new Object();

    public FileWatermarkStore(Path baseDir) {
        this.baseDir = baseDir;
    }

    public FileWatermarkStore() {
        this(Path.of("data"));
    }

    private Path file(String sourceName) {
        return baseDir.resolve("watermark-" + sourceName + ".json");
    }

    @Override
    public Watermark load(String sourceName) {
        Path f = file(sourceName);
        if (!Files.exists(f)) {
            return Watermark.initial(sourceName);
        }
        try {
            return MAPPER.readValue(f.toFile(), Watermark.class);
        } catch (IOException e) {
            throw new IllegalStateException("读取水位文件失败: " + f, e);
        }
    }

    @Override
    public Watermark commit(String sourceName, long expectedVersion, long newPosition) {
        synchronized (writeLock) {
            Watermark current = load(sourceName);
            if (current.version() != expectedVersion) {
                throw new WatermarkConflictException(
                        "来源[" + sourceName + "]水位 CAS 冲突: 期望版本 " + expectedVersion
                                + "，实际版本 " + current.version() + "（可能被并发拉取提交）");
            }
            if (!current.isEmpty() && newPosition < current.position()) {
                throw new WatermarkRegressionException(
                        "来源[" + sourceName + "]水位回退被拒绝: 当前位置 " + current.position()
                                + "，提交位置 " + newPosition);
            }
            Watermark next = new Watermark(sourceName, newPosition,
                    current.version() + 1, System.currentTimeMillis());
            persist(sourceName, next);
            log.info("来源[{}]水位提交: {} -> {} (v{} -> v{})",
                    sourceName, current.position(), newPosition, current.version(), next.version());
            return next;
        }
    }

    private void persist(String sourceName, Watermark wm) {
        try {
            Files.createDirectories(baseDir);
            Path tmp = Files.createTempFile(baseDir, "wm-", ".tmp");
            MAPPER.writeValue(tmp.toFile(), wm);
            Files.move(tmp, file(sourceName),
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new IllegalStateException("写入水位文件失败: " + sourceName, e);
        }
    }
}
