package com.github.h3yow.incrementalsourceingestionjava.skipped;

import com.github.h3yow.incrementalsourceingestionjava.model.SkippedRecord;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 进程内内存隔离区。key 为 sourceId:dedupKey，
 * 同一条数据反复失败只保留一条隔离记录（更新尝试次数与原因），不会堆积重复。
 */
public class InMemorySkippedStore implements SkippedStore {

    private final ConcurrentHashMap<String, SkippedRecord> records = new ConcurrentHashMap<>();

    static String keyOf(String sourceId, String dedupKey) {
        return sourceId + "|" + dedupKey;
    }

    @Override
    public void save(SkippedRecord record) {
        records.put(keyOf(record.sourceId(), record.dedupKey()), record);
    }

    @Override
    public List<SkippedRecord> findBySource(String sourceId) {
        return records.values().stream()
                .filter(r -> r.sourceId().equals(sourceId))
                .sorted(java.util.Comparator.comparingLong(SkippedRecord::sequence))
                .toList();
    }

    @Override
    public void remove(String sourceId, String dedupKey) {
        records.remove(keyOf(sourceId, dedupKey));
    }

    @Override
    public Optional<SkippedRecord> find(String sourceId, String dedupKey) {
        return Optional.ofNullable(records.get(keyOf(sourceId, dedupKey)));
    }

    @Override
    public int count() {
        return records.size();
    }
}
