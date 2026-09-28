package com.github.h3yow.incrementalsourceingestionjava.service;

import com.github.h3yow.incrementalsourceingestionjava.model.NormalizedRecord;

/**
 * 处理期失败注入点（模拟目标端瞬时故障，如超时/连接失败）。
 * 归一化阶段的确定性失败（拒绝）不走这里。
 */
@FunctionalInterface
public interface RecordFailureInjector {

    /** 对已归一记录做处理期检查；需要模拟失败时抛异常 */
    void check(NormalizedRecord record);

    RecordFailureInjector NOOP = record -> {
    };
}
