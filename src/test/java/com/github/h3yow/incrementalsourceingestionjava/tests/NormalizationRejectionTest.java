package com.github.h3yow.incrementalsourceingestionjava.tests;

import com.github.h3yow.incrementalsourceingestionjava.model.NormalizationResult;
import com.github.h3yow.incrementalsourceingestionjava.model.RawRecord;
import com.github.h3yow.incrementalsourceingestionjava.model.RejectReason;
import com.github.h3yow.incrementalsourceingestionjava.normalize.StrictRecordNormalizer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 归一校验：每种字段问题都必须给出互不相同、说得清楚的拒绝原因，
 * 合法记录必须被接受。单测日志打印输入数据与判定依据。
 */
class NormalizationRejectionTest {

    private static final Logger log = LoggerFactory.getLogger(NormalizationRejectionTest.class);

    private final StrictRecordNormalizer normalizer = new StrictRecordNormalizer();

    private RawRecord raw(Object sequence, Map<String, Object> data) {
        return new RawRecord("local-file", "src-normalize", sequence, data);
    }

    private Map<String, Object> validData() {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("id", "rec-1");
        d.put("name", "alice");
        d.put("eventTime", "2026-09-29T10:00:00Z");
        return d;
    }

    private void assertRejected(String caseName, RawRecord input, RejectReason expected) {
        NormalizationResult result = normalizer.normalize(input);
        log.info("[{}] 输入数据={} | 判定依据=期望原因[{}]", caseName, input.data(), expected.description());
        assertFalse(result.isAccepted(), "用例[" + caseName + "]应当被拒绝");
        assertEquals(expected, result.rejected().reason(),
                "用例[" + caseName + "]拒绝原因不匹配，实际=" + result.rejected().detail());
        log.info("[{}] 实际判定={} | 细节={}", caseName, result.rejected().reason(),
                result.rejected().detail());
    }

    @Test
    void validRecordAccepted() {
        RawRecord input = raw(1L, validData());
        log.info("[合法记录] 输入数据={}", input.data());
        NormalizationResult result = normalizer.normalize(input);
        assertTrue(result.isAccepted());
        assertEquals(1L, result.accepted().sequence());
        assertEquals("rec-1", result.accepted().recordId());
        log.info("[合法记录] 判定依据=字段齐全且类型正确，sequence={} recordId={}",
                result.accepted().sequence(), result.accepted().recordId());
    }

    @Test
    void sequenceMissing() {
        assertRejected("缺少序号", raw(null, validData()), RejectReason.SEQUENCE_MISSING);
    }

    @Test
    void sequenceNotANumber() {
        Map<String, Object> d = validData();
        assertRejected("序号不是数字", raw("abc", d), RejectReason.SEQUENCE_NOT_A_NUMBER);
    }

    @Test
    void missingId() {
        Map<String, Object> d = validData();
        d.remove("id");
        assertRejected("缺少id", raw(1L, d), RejectReason.MISSING_ID);
    }

    @Test
    void idTypeMismatch() {
        Map<String, Object> d = validData();
        d.put("id", 12345);
        assertRejected("id类型不符", raw(1L, d), RejectReason.ID_TYPE_MISMATCH);
    }

    @Test
    void missingName() {
        Map<String, Object> d = validData();
        d.remove("name");
        assertRejected("缺少name", raw(1L, d), RejectReason.MISSING_NAME);
    }

    @Test
    void nameBlank() {
        Map<String, Object> d = validData();
        d.put("name", "   ");
        assertRejected("name空白", raw(1L, d), RejectReason.NAME_BLANK);
    }

    @Test
    void eventTimeMalformed() {
        Map<String, Object> d = validData();
        d.put("eventTime", "not-a-time");
        assertRejected("时间格式错误", raw(1L, d), RejectReason.EVENT_TIME_MALFORMED);
    }
}
