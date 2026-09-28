package com.github.h3yow.incrementalsourceingestionjava.source.local;

import com.github.h3yow.incrementalsourceingestionjava.model.RawRecord;
import com.github.h3yow.incrementalsourceingestionjava.source.SourceConnector;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 本地表来源：模拟数据库风格来源，数据以 Map 行的形式给出，
 * 行内自带 sequence 列。存储按序号排序，支持乱序插入、按水位增量拉取。
 */
public class LocalTableSource implements SourceConnector {

    public static final String TYPE = "local-table";

    private final String sourceId;
    private final TreeMap<Long, Map<String, Object>> rows = new TreeMap<>();

    public LocalTableSource(String sourceId) {
        this.sourceId = sourceId;
    }

    /** 追加一行；sequence 作为行内字段同时写入，交由归一器统一校验。 */
    public void addRow(long sequence, Map<String, Object> row) {
        Map<String, Object> copy = new java.util.HashMap<>(row);
        copy.put("sequence", sequence);
        rows.put(sequence, copy);
    }

    @Override
    public String sourceType() {
        return TYPE;
    }

    @Override
    public String sourceId() {
        return sourceId;
    }

    @Override
    public List<RawRecord> fetch(long afterSequence, int limit) {
        return rows.tailMap(afterSequence, false).values().stream()
                .limit(limit)
                .map(row -> new RawRecord(TYPE, sourceId, row.get("sequence"), row))
                .toList();
    }
}
