package com.github.h3yow.incrementalsourceingestionjava.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.incrementalsourceingestionjava.model.NormalizedRecord;
import com.github.h3yow.incrementalsourceingestionjava.sink.TargetSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 文件目标端（本地模拟）：每个来源一个 JSON 文件，按业务主键幂等，
 * 进程重启后重新加载，重复采集不产生重复数据。
 */
public class FileTargetSink implements TargetSink {

    private static final Logger log = LoggerFactory.getLogger(FileTargetSink.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path baseDir;
    /** sourceName -> (businessKey -> record) */
    private final Map<String, Map<String, NormalizedRecord>> data = new LinkedHashMap<>();

    public FileTargetSink(Path baseDir) {
        this.baseDir = baseDir;
    }

    public FileTargetSink() {
        this(Path.of("data"));
    }

    private synchronized Map<String, NormalizedRecord> table(String sourceName) {
        return data.computeIfAbsent(sourceName, this::load);
    }

    private Path file(String sourceName) {
        return baseDir.resolve("sink-" + sourceName + ".json");
    }

    @SuppressWarnings("unchecked")
    private Map<String, NormalizedRecord> load(String sourceName) {
        Path f = file(sourceName);
        if (!Files.exists(f)) {
            return new LinkedHashMap<>();
        }
        try {
            Map<String, Object> raw = MAPPER.readValue(f.toFile(), LinkedHashMap.class);
            Map<String, NormalizedRecord> table = new LinkedHashMap<>();
            for (Map.Entry<String, Object> e : raw.entrySet()) {
                table.put(e.getKey(), MAPPER.convertValue(e.getValue(), NormalizedRecord.class));
            }
            return table;
        } catch (IOException e) {
            throw new IllegalStateException("读取目标端文件失败: " + f, e);
        }
    }

    @Override
    public boolean put(NormalizedRecord record) {
        synchronized (this) {
            Map<String, NormalizedRecord> table = table(record.sourceName());
            if (table.containsKey(record.businessKey())) {
                log.debug("目标端去重命中: source={} key={}", record.sourceName(), record.businessKey());
                return false;
            }
            table.put(record.businessKey(), record);
            persist(record.sourceName(), table);
            return true;
        }
    }

    @Override
    public int size() {
        synchronized (this) {
            return data.values().stream().mapToInt(Map::size).sum();
        }
    }

    /** 某来源已写入条数（便于测试断言） */
    public int size(String sourceName) {
        synchronized (this) {
            return table(sourceName).size();
        }
    }

    private void persist(String sourceName, Map<String, NormalizedRecord> table) {
        try {
            Files.createDirectories(baseDir);
            Path tmp = Files.createTempFile(baseDir, "sink-", ".tmp");
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), table);
            Files.move(tmp, file(sourceName),
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new IllegalStateException("写入目标端文件失败: " + sourceName, e);
        }
    }
}
