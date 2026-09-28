package com.github.h3yow.incrementalsourceingestionjava.watermark;

import com.github.h3yow.incrementalsourceingestionjava.exception.WatermarkRegressionException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;

/**
 * 文件持久化水位存储，用于演示真实的进程重启恢复。
 * 每个来源一个文件，提交时先写临时文件再原子 move，避免写到一半损坏。
 */
public class FileWatermarkStore implements WatermarkStore {

    private final Path directory;

    public FileWatermarkStore(Path directory) {
        this.directory = directory;
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path fileOf(String sourceId) {
        return directory.resolve("watermark-" + sourceId + ".txt");
    }

    @Override
    public Optional<Long> load(String sourceId) {
        // 每次都从文件读，模拟一个全新进程只认磁盘上的已提交水位。
        Path file = fileOf(sourceId);
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        try {
            String content = Files.readString(file, StandardCharsets.UTF_8).trim();
            return content.isEmpty() ? Optional.empty() : Optional.of(Long.parseLong(content));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean compareAndAdvance(String sourceId, long expected, long next) {
        synchronized (fileLock(sourceId)) {
            long onDisk = load(sourceId).orElse(0L);
            if (onDisk != expected) {
                if (next < onDisk) {
                    throw new WatermarkRegressionException(sourceId, onDisk, next);
                }
                return false;
            }
            if (next < onDisk) {
                throw new WatermarkRegressionException(sourceId, onDisk, next);
            }
            if (next == onDisk) {
                return false;
            }
            persist(sourceId, next);
            return true;
        }
    }

    @Override
    public long verifyStartPoint(String sourceId, long requestedStart) {
        long committed = load(sourceId).orElse(0L);
        if (requestedStart < committed) {
            throw new WatermarkRegressionException(sourceId, committed, requestedStart);
        }
        return Math.max(requestedStart, committed);
    }

    private void persist(String sourceId, long value) {
        Path target = fileOf(sourceId);
        Path tmp = directory.resolve("watermark-" + sourceId + ".tmp");
        try {
            Files.writeString(tmp, Long.toString(value), StandardCharsets.UTF_8);
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String fileLock(String sourceId) {
        // 同一来源路径一把锁，跨线程提交串行化。
        return fileOf(sourceId).toAbsolutePath().normalize().toString().intern();
    }
}
