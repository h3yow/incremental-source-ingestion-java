package com.github.h3yow.incrementalsourceingestionjava.service;

import java.util.List;

/**
 * 一批采集的汇总：成功 / 失败 / 跳过 / 重复 各多少，以及逐条明细。
 */
public record BatchResult(
        String sourceName,
        boolean recoveredRun,
        int total,
        int success,
        int failed,
        int skipped,
        int duplicate,
        long watermarkBefore,
        Long watermarkAfter,
        List<ItemResult> items
) {
}
