package com.github.h3yow.incrementalsourceingestionjava.tests;

import com.github.h3yow.incrementalsourceingestionjava.local.DefaultSourceRegistry;
import com.github.h3yow.incrementalsourceingestionjava.local.SimpleSourceType;
import com.github.h3yow.incrementalsourceingestionjava.model.NormalizedRecord;
import com.github.h3yow.incrementalsourceingestionjava.model.RawRecord;
import com.github.h3yow.incrementalsourceingestionjava.model.RejectedRecordException;
import com.github.h3yow.incrementalsourceingestionjava.model.RejectionReason;
import com.github.h3yow.incrementalsourceingestionjava.normalize.Normalizer;
import com.github.h3yow.incrementalsourceingestionjava.source.SourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 归一化拒绝原因测试：字段缺失 / 类型对不上 / 格式不完整必须得到互不相同、说清楚的拒绝原因，
 * 且不能静默跳过。每条用例打印输入数据与判定依据。
 */
class NormalizerTest {

    private static final Logger log = LoggerFactory.getLogger(NormalizerTest.class);

    private Normalizer normalizer;
    private final SourceType type = SimpleSourceType.of("orders");

    @BeforeEach
    void setUp() {
        DefaultSourceRegistry registry = new DefaultSourceRegistry();
        registry.registerType(type);
        normalizer = new Normalizer(registry);
    }

    private RawRecord raw(Object cursor, Map<String, Object> fields) {
        return new RawRecord("orders", "orders-a", cursor, fields);
    }

    private Map<String, Object> good() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", "K1");
        m.put("name", "订单1");
        m.put("eventTime", "1700000000000");
        return m;
    }

    private void assertReason(Object cursor, Map<String, Object> input,
                              RejectionReason expected, String note) {
        log.info("[输入] {} | cursor={} fields={}", note, cursor, input);
        RejectedRecordException ex = assertThrows(RejectedRecordException.class,
                () -> normalizer.normalize(raw(cursor, input)));
        log.info("[判定] 期望={} 实际={} 依据={}", expected, ex.reason(), ex.getMessage());
        assertEquals(expected, ex.reason());
    }

    @Test
    @DisplayName("字段完整时归一成功")
    void shouldNormalizeValidRecord() {
        Map<String, Object> input = good();
        log.info("[输入] 合法记录 cursor={} fields={}", 1L, input);
        NormalizedRecord n = normalizer.normalize(raw(1L, input));
        log.info("[判定] 归一成功 key={} cursor={} eventTime={}", n.businessKey(), n.cursor(), n.eventTime());
        assertEquals("K1", n.businessKey());
        assertEquals(1L, n.cursor());
        assertEquals(1700000000000L, n.eventTime());
    }

    @Test
    @DisplayName("8 类坏数据得到互不相同的拒绝原因")
    void shouldRejectWithDistinctReasons() {
        assertReason(1L, null, RejectionReason.FIELDS_MISSING, "字段整体为 null");

        Map<String, Object> missingKey = good();
        missingKey.remove("id");
        assertReason(1L, missingKey, RejectionReason.MISSING_BUSINESS_KEY, "缺少主键字段 id");

        Map<String, Object> emptyKey = good();
        emptyKey.put("id", "   ");
        assertReason(1L, emptyKey, RejectionReason.EMPTY_BUSINESS_KEY, "主键为空白串");

        Map<String, Object> badKeyType = good();
        badKeyType.put("id", new LinkedHashMap<>(Map.of("nested", 1)));
        assertReason(1L, badKeyType, RejectionReason.BUSINESS_KEY_TYPE_MISMATCH, "主键为嵌套对象");

        assertReason(null, good(), RejectionReason.MISSING_CURSOR, "游标缺失");
        assertReason("not-a-number", good(), RejectionReason.CURSOR_TYPE_MISMATCH, "游标类型对不上");

        Map<String, Object> badTime = good();
        badTime.put("eventTime", "2024-13-99T99:99");
        assertReason(1L, badTime, RejectionReason.EVENT_TIME_MALFORMED, "事件时间格式不完整");

        Map<String, Object> missingPayload = good();
        missingPayload.remove("name");
        assertReason(1L, missingPayload, RejectionReason.MISSING_REQUIRED_PAYLOAD_FIELD, "缺少必要载荷 name");

        long distinct = Arrays.stream(RejectionReason.values()).distinct().count();
        log.info("[判定] RejectionReason 共 {} 个，去重后 {} 个，应包含 8 类坏数据场景",
                RejectionReason.values().length, distinct);
        assertEquals(RejectionReason.values().length, distinct, "拒绝原因必须互不相同");
    }
}
