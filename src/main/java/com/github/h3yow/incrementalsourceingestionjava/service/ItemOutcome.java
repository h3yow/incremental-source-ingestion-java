package com.github.h3yow.incrementalsourceingestionjava.service;

/**
 * 单条记录的最终去向（重复采集命中去重单独计数，不挤占 成功/失败/跳过）。
 */
public enum ItemOutcome {
    SUCCESS,
    FAILED,
    SKIPPED,
    DUPLICATE
}
