package com.github.h3yow.incrementalsourceingestionjava.sink;

import com.github.h3yow.incrementalsourceingestionjava.model.NormalizedRecord;

/**
 * 目标端（本地模拟）。按业务主键幂等写入：重复写入同 key 不产生重复数据。
 */
public interface TargetSink {

    /**
     * 幂等写入。
     *
     * @return true 表示本次新写入；false 表示该 key 已存在（重复采集命中去重）
     */
    boolean put(NormalizedRecord record);

    /** 当前已写入的不重复 key 数量（便于断言与演示） */
    int size();
}
