package com.github.h3yow.incrementalsourceingestionjava.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.incrementalsourceingestionjava.isolation.IsolationRecord;
import com.github.h3yow.incrementalsourceingestionjava.isolation.IsolationStatus;
import com.github.h3yow.incrementalsourceingestionjava.isolation.IsolationStore;
import com.github.h3yow.incrementalsourceingestionjava.model.RawRecord;
import com.github.h3yow.incrementalsourceingestionjava.model.RejectionReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * 文件隔离区（本地模拟）：每条隔离记录一个 JSON 文件，重启后仍可重新捞回。
 */
public class FileIsolationStore implements IsolationStore {

    private static final Logger log = LoggerFactory.getLogger(FileIsolationStore.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path baseDir;
    private final Object writeLock = new Object();

    public FileIsolationStore(Path baseDir) {
        this.baseDir = baseDir;
    }

    public FileIsolationStore() {
        this(Path.of("data"));
    }

    private Path dir() {
        return baseDir.resolve("isolation");
    }

    private Path file(String id) {
        return dir().resolve(id + ".json");
    }

    @Override
    public IsolationRecord put(String id, String sourceName, RawRecord raw,
                               RejectionReason reason, String detail, int attempts,
                               IsolationStatus status) {
        synchronized (writeLock) {
            long now = System.currentTimeMillis();
            IsolationRecord existing = find(id).orElse(null);
            IsolationRecord rec = new IsolationRecord(
                    id, raw.sourceType(), sourceName, raw, reason, detail,
                    attempts, status,
                    existing == null ? now : existing.createdAt(), now);
            persist(rec);
            log.warn("隔离记录: id={} source={} status={} reason={} attempts={} detail={}",
                    id, sourceName, status, reason, attempts, detail);
            return rec;
        }
    }

    @Override
    public List<IsolationRecord> fetchPending(String sourceName) {
        Path d = dir();
        if (!Files.exists(d)) {
            return List.of();
        }
        try (Stream<Path> files = Files.list(d)) {
            return files.filter(p -> p.toString().endsWith(".json"))
                    .map(this::read)
                    .filter(r -> r.sourceName().equals(sourceName))
                    .filter(r -> r.status() == IsolationStatus.FAILED
                            || r.status() == IsolationStatus.SKIPPED)
                    .sorted(Comparator.comparing(IsolationRecord::updatedAt))
                    .toList();
        } catch (IOException e) {
            throw new IllegalStateException("读取隔离区失败", e);
        }
    }

    @Override
    public Optional<IsolationRecord> find(String id) {
        Path f = file(id);
        if (!Files.exists(f)) {
            return Optional.empty();
        }
        return Optional.of(read(f));
    }

    @Override
    public IsolationRecord markRecovered(String id) {
        synchronized (writeLock) {
            IsolationRecord rec = find(id)
                    .orElseThrow(() -> new IllegalArgumentException("隔离记录不存在: " + id));
            IsolationRecord updated = new IsolationRecord(
                    rec.id(), rec.sourceType(), rec.sourceName(), rec.raw(), rec.reason(),
                    rec.detail(), rec.attempts(), IsolationStatus.RECOVERED,
                    rec.createdAt(), System.currentTimeMillis());
            persist(updated);
            log.info("隔离记录已捞回: id={} source={}", id, rec.sourceName());
            return updated;
        }
    }

    private IsolationRecord read(Path f) {
        try {
            return MAPPER.readValue(f.toFile(), IsolationRecord.class);
        } catch (IOException e) {
            throw new IllegalStateException("读取隔离记录失败: " + f, e);
        }
    }

    private void persist(IsolationRecord rec) {
        try {
            Files.createDirectories(dir());
            Path tmp = Files.createTempFile(dir(), "iso-", ".tmp");
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), rec);
            Files.move(tmp, file(rec.id()),
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                    java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new IllegalStateException("写入隔离记录失败: " + rec.id(), e);
        }
    }
}
