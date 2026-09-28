package com.github.h3yow.incrementalsourceingestionjava.skipped;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.incrementalsourceingestionjava.model.SkippedRecord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 文件持久化隔离区：每个来源一个 JSON 文件，重启后被跳过的数据仍可重新捞回。
 * 写入先落临时文件再原子 move。
 */
public class FileSkippedStore implements SkippedStore {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path directory;

    public FileSkippedStore(Path directory) {
        this.directory = directory;
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path fileOf(String sourceId) {
        return directory.resolve("skipped-" + sourceId + ".json");
    }

    @Override
    public synchronized void save(SkippedRecord record) {
        List<SkippedRecord> all = readAll(record.sourceId());
        all.removeIf(existing -> existing.dedupKey().equals(record.dedupKey()));
        all.add(record);
        writeAll(record.sourceId(), all);
    }

    @Override
    public List<SkippedRecord> findBySource(String sourceId) {
        return readAll(sourceId).stream()
                .sorted(java.util.Comparator.comparingLong(SkippedRecord::sequence))
                .toList();
    }

    @Override
    public synchronized void remove(String sourceId, String dedupKey) {
        List<SkippedRecord> all = readAll(sourceId);
        boolean removed = all.removeIf(r -> r.dedupKey().equals(dedupKey));
        if (removed) {
            writeAll(sourceId, all);
        }
    }

    @Override
    public Optional<SkippedRecord> find(String sourceId, String dedupKey) {
        return readAll(sourceId).stream()
                .filter(r -> r.dedupKey().equals(dedupKey))
                .findFirst();
    }

    @Override
    public int count() {
        try (var paths = Files.list(directory)) {
            return (int) paths.filter(p -> p.getFileName().toString().startsWith("skipped-"))
                    .flatMap(p -> readAll(fileToSource(p)).stream())
                    .count();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String fileToSource(Path file) {
        String name = file.getFileName().toString();
        return name.substring("skipped-".length(), name.length() - ".json".length());
    }

    private List<SkippedRecord> readAll(String sourceId) {
        Path file = fileOf(sourceId);
        if (!Files.exists(file)) {
            return new ArrayList<>();
        }
        try {
            String json = Files.readString(file, StandardCharsets.UTF_8);
            if (json.isBlank()) {
                return new ArrayList<>();
            }
            return MAPPER.readValue(json, new TypeReference<List<SkippedRecord>>() {
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeAll(String sourceId, List<SkippedRecord> all) {
        Path target = fileOf(sourceId);
        Path tmp = directory.resolve("skipped-" + sourceId + ".tmp");
        try {
            MAPPER.writeValue(tmp.toFile(), all);
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
