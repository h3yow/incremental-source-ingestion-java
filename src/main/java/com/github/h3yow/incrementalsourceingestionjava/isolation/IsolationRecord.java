package com.github.h3yow.incrementalsourceingestionjava.isolation;

import com.github.h3yow.incrementalsourceingestionjava.model.RejectionReason;
import com.github.h3yow.incrementalsourceingestionjava.model.RawRecord;

/**
 * 一条被隔离的记录：保留原始数据、失败原因与重试信息，之后可以原样重处理。
 *
 * @param id           来源实例 + 业务主键/游标 拼出的稳定隔离键
 * @param sourceType   来源类型
 * @param sourceName   来源实例
 * @param raw          原始数据快照
 * @param reason       归一化拒绝原因；处理期瞬时失败为 null
 * @param detail       判定依据描述
 * @param attempts     已尝试次数
 * @param status       隔离状态
 * @param createdAt    首次隔离时间
 * @param updatedAt    最近更新时间
 */
public record IsolationRecord(
        String id,
        String sourceType,
        String sourceName,
        RawRecord raw,
        RejectionReason reason,
        String detail,
        int attempts,
        IsolationStatus status,
        long createdAt,
        long updatedAt
) {
}
