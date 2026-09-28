package com.github.h3yow.incrementalsourceingestionjava.local;

import com.github.h3yow.incrementalsourceingestionjava.model.RawRecord;
import com.github.h3yow.incrementalsourceingestionjava.source.SourceConnector;
import com.github.h3yow.incrementalsourceingestionjava.source.SourceType;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * 本地内存来源：预置一批原始记录，按游标增量返回（模拟源端）。
 * 返回游标严格大于 afterCursor 的记录、按游标升序、最多 limit 条。
 * 游标以 Object 承载：合法为 Long；非法字符串用于验证归一化拒绝。
 */
public class InMemorySourceConnector implements SourceConnector {

    private final SourceType sourceType;
    private final String sourceName;
    private final List<RawRecord> records;

    public InMemorySourceConnector(SourceType sourceType, String sourceName, List<RawRecord> records) {
        this.sourceType = sourceType;
        this.sourceName = sourceName;
        this.records = records.stream()
                .sorted(Comparator.comparingLong(InMemorySourceConnector::cursorAsLong))
                .toList();
    }

    private static long cursorAsLong(RawRecord r) {
        if (r.cursor() instanceof Number n) {
            return n.longValue();
        }
        return Long.MIN_VALUE; // 非法游标在归一化阶段会被拒绝，排序时垫底
    }

    @Override
    public String sourceName() {
        return sourceName;
    }

    @Override
    public SourceType sourceType() {
        return sourceType;
    }

    @Override
    public List<RawRecord> fetch(Long afterCursor, int limit) {
        long lower = afterCursor == null ? Long.MIN_VALUE : afterCursor;
        return records.stream()
                .filter(r -> r.cursor() instanceof Number n && n.longValue() > lower)
                .limit(limit <= 0 ? records.size() : limit)
                .map(Objects::requireNonNull)
                .toList();
    }
}
