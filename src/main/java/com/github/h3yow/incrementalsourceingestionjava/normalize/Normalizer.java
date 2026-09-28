package com.github.h3yow.incrementalsourceingestionjava.normalize;

import com.github.h3yow.incrementalsourceingestionjava.model.NormalizedRecord;
import com.github.h3yow.incrementalsourceingestionjava.model.RawRecord;
import com.github.h3yow.incrementalsourceingestionjava.model.RejectedRecordException;
import com.github.h3yow.incrementalsourceingestionjava.model.RejectionReason;
import com.github.h3yow.incrementalsourceingestionjava.source.SourceRegistry;
import com.github.h3yow.incrementalsourceingestionjava.source.SourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 统一归一化门面：按记录所属来源类型的约定，将原始记录归一为标准记录。
 * 字段缺失/类型不符/格式不完整时抛 {@link RejectedRecordException}，绝不静默跳过。
 */
public class Normalizer {

    private static final Logger log = LoggerFactory.getLogger(Normalizer.class);

    private final SourceRegistry registry;

    public Normalizer(SourceRegistry registry) {
        this.registry = registry;
    }

    public NormalizedRecord normalize(RawRecord raw) {
        if (raw == null) {
            throw new RejectedRecordException(RejectionReason.FIELDS_MISSING, "原始记录为 null");
        }
        SourceType type = registry.requireType(raw.sourceType());
        return normalize(raw, type);
    }

    /** 按来源类型约定做字段映射与校验；包内可见便于单测 */
    NormalizedRecord normalize(RawRecord raw, SourceType type) {
        Map<String, Object> fields = raw.fields();
        if (fields == null) {
            throw new RejectedRecordException(RejectionReason.FIELDS_MISSING,
                    "来源[" + raw.sourceName() + "]原始字段整体缺失");
        }

        // 1. 业务主键
        Object keyValue = fields.get(type.businessKeyField());
        if (keyValue == null) {
            throw new RejectedRecordException(RejectionReason.MISSING_BUSINESS_KEY,
                    "来源[" + raw.sourceName() + "]缺少业务主键字段 '" + type.businessKeyField() + "'");
        }
        String businessKey;
        if (keyValue instanceof CharSequence) {
            businessKey = keyValue.toString().trim();
            if (businessKey.isEmpty()) {
                throw new RejectedRecordException(RejectionReason.EMPTY_BUSINESS_KEY,
                        "来源[" + raw.sourceName() + "]业务主键字段 '" + type.businessKeyField() + "' 为空白");
            }
        } else if (keyValue instanceof Number || keyValue instanceof Boolean) {
            businessKey = keyValue.toString();
        } else {
            throw new RejectedRecordException(RejectionReason.BUSINESS_KEY_TYPE_MISMATCH,
                    "来源[" + raw.sourceName() + "]业务主键字段 '" + type.businessKeyField()
                            + "' 类型无法归一: " + keyValue.getClass().getSimpleName());
        }

        // 2. 游标
        Long cursor = parseCursor(raw, type);

        // 3. 事件时间
        Long eventTime = parseEventTime(raw, fields, type);

        // 4. 必要载荷字段
        for (String required : type.requiredPayloadFields()) {
            Object v = fields.get(required);
            if (v == null || (v instanceof CharSequence cs && cs.toString().trim().isEmpty())) {
                throw new RejectedRecordException(RejectionReason.MISSING_REQUIRED_PAYLOAD_FIELD,
                        "来源[" + raw.sourceName() + "]缺少必要载荷字段 '" + required + "'");
            }
        }

        Map<String, Object> payload = new LinkedHashMap<>(fields);
        return new NormalizedRecord(raw.sourceType(), raw.sourceName(), businessKey,
                cursor, eventTime, payload);
    }

    private Long parseCursor(RawRecord raw, SourceType type) {
        Object cursorValue = raw.cursor();
        if (cursorValue == null) {
            throw new RejectedRecordException(RejectionReason.MISSING_CURSOR,
                    "来源[" + raw.sourceName() + "]未提供采集游标");
        }
        if (cursorValue instanceof Number n) {
            return n.longValue();
        }
        String s = cursorValue.toString().trim();
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            throw new RejectedRecordException(RejectionReason.CURSOR_TYPE_MISMATCH,
                    "来源[" + raw.sourceName() + "]游标类型无法解析为长整型: " + cursorValue);
        }
    }

    private Long parseEventTime(RawRecord raw, Map<String, Object> fields, SourceType type) {
        Object v = fields.get(type.eventTimeField());
        if (v == null) {
            return null; // 事件时间允许缺失，不算拒绝
        }
        if (v instanceof Number n) {
            return n.longValue();
        }
        String s = v.toString().trim();
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            log.warn("来源[{}]事件时间字段 '{}' 格式不完整，无法解析: '{}'",
                    raw.sourceName(), type.eventTimeField(), s);
            throw new RejectedRecordException(RejectionReason.EVENT_TIME_MALFORMED,
                    "来源[" + raw.sourceName() + "]事件时间格式不完整: '" + s + "'");
        }
    }
}
