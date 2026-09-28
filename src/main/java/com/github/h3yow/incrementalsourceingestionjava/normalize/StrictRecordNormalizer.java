package com.github.h3yow.incrementalsourceingestionjava.normalize;

import com.github.h3yow.incrementalsourceingestionjava.model.NormalizationResult;
import com.github.h3yow.incrementalsourceingestionjava.model.NormalizedRecord;
import com.github.h3yow.incrementalsourceingestionjava.model.RawRecord;
import com.github.h3yow.incrementalsourceingestionjava.model.RejectReason;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;

/**
 * 严格归一器：字段缺失、类型对不上、格式不完整都给出互不相同的拒绝原因，
 * 绝不悄悄跳过。校验顺序固定，每条拒绝只报第一个失败点，保证原因唯一、可定位。
 */
public class StrictRecordNormalizer implements RecordNormalizer {

    @Override
    public NormalizationResult normalize(RawRecord raw) {
        if (raw == null || raw.data() == null) {
            return NormalizationResult.rejected(raw, RejectReason.SEQUENCE_MISSING,
                    "记录或数据体为 null");
        }
        Map<String, Object> data = raw.data();

        Object rawSequence = raw.rawSequence();
        if (rawSequence == null) {
            return reject(raw, RejectReason.SEQUENCE_MISSING, "sequence 字段不存在");
        }
        Long sequence = asLong(rawSequence);
        if (sequence == null) {
            return reject(raw, RejectReason.SEQUENCE_NOT_A_NUMBER,
                    "sequence 实际值=" + rawSequence + "，类型=" + rawSequence.getClass().getSimpleName());
        }

        Object id = data.get("id");
        if (id == null) {
            return reject(raw, RejectReason.MISSING_ID, "id 字段不存在");
        }
        if (!(id instanceof String)) {
            return reject(raw, RejectReason.ID_TYPE_MISMATCH,
                    "id 实际值=" + id + "，类型=" + id.getClass().getSimpleName());
        }
        String recordId = ((String) id).trim();
        if (recordId.isEmpty()) {
            return reject(raw, RejectReason.MISSING_ID, "id 为空字符串");
        }

        Object name = data.get("name");
        if (name == null) {
            return reject(raw, RejectReason.MISSING_NAME, "name 字段不存在");
        }
        if (!(name instanceof String)) {
            return reject(raw, RejectReason.NAME_BLANK,
                    "name 类型不是字符串，实际类型=" + name.getClass().getSimpleName());
        }
        if (((String) name).isBlank()) {
            return reject(raw, RejectReason.NAME_BLANK, "name 为空白字符串");
        }

        Object eventTime = data.get("eventTime");
        long eventTimeMillis;
        if (eventTime == null) {
            // eventTime 缺省允许，用当前时间，不算拒绝。
            eventTimeMillis = System.currentTimeMillis();
        } else if (eventTime instanceof Number n) {
            eventTimeMillis = n.longValue();
        } else if (eventTime instanceof String s) {
            try {
                eventTimeMillis = Instant.parse(s).toEpochMilli();
            } catch (DateTimeParseException e) {
                return reject(raw, RejectReason.EVENT_TIME_MALFORMED,
                        "eventTime 不是合法 ISO-8601 时间，实际值=" + s);
            }
        } else {
            return reject(raw, RejectReason.EVENT_TIME_MALFORMED,
                    "eventTime 类型无法识别=" + eventTime.getClass().getSimpleName());
        }

        NormalizedRecord normalized = new NormalizedRecord(
                raw.sourceType(), raw.sourceId(), sequence,
                recordId, (String) name, eventTimeMillis, String.valueOf(data));
        return NormalizationResult.accepted(normalized);
    }

    private static NormalizationResult reject(RawRecord raw, RejectReason reason, String detail) {
        return NormalizationResult.rejected(raw, reason, reason.description() + "；" + detail);
    }

    private static Long asLong(Object value) {
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
