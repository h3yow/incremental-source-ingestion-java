package com.github.h3yow.incrementalsourceingestionjava.source.local;

import com.github.h3yow.incrementalsourceingestionjava.model.RawRecord;
import com.github.h3yow.incrementalsourceingestionjava.source.SourceConnector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * 本地文件来源：模拟 JSON 行文件。sequence 与字段都以 JSON 反序列化后的
 * 宽松类型（Integer/Long/String，甚至故意给错类型）给出，
 * 用于在归一阶段暴露缺失、类型对不上、格式不完整等问题。
 *
 * 排序仅对能转成数字的序号生效；无法解析的记录保持插入顺序排在最后，
 * 保证它们仍会被拉到、被明确拒绝，而不是悄悄消失。
 */
public class LocalFileSource implements SourceConnector {

    public static final String TYPE = "local-file";

    private final String sourceId;
    private final List<Map<String, Object>> lines = new ArrayList<>();

    public LocalFileSource(String sourceId) {
        this.sourceId = sourceId;
    }

    /** 直接追加一条已经解析好的宽松类型记录，map 中应带 sequence 字段。 */
    public void addRecord(Map<String, Object> data) {
        lines.add(Map.copyOf(data));
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
        List<RawRecord> all = new ArrayList<>();
        for (Map<String, Object> data : lines) {
            all.add(new RawRecord(TYPE, sourceId, data.get("sequence"), data));
        }
        // 可解析序号按数字升序；不可解析序号排最后（不能因为排序失败而丢记录）。
        all.sort(Comparator.comparingLong(r -> asLongOrMax(r.rawSequence())));
        List<RawRecord> result = new ArrayList<>();
        for (RawRecord r : all) {
            Long seq = tryAsLong(r.rawSequence());
            if (seq != null) {
                if (seq <= afterSequence) {
                    continue;
                }
            }
            result.add(r);
            if (result.size() >= limit) {
                break;
            }
        }
        return result;
    }

    private static long asLongOrMax(Object value) {
        Long l = tryAsLong(value);
        return l == null ? Long.MAX_VALUE : l;
    }

    private static Long tryAsLong(Object value) {
        if (value instanceof Number n) {
            return n.longValue();
        }
        if (value instanceof String s) {
            try {
                return Long.parseLong(s.trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }
}
