package com.github.h3yow.incrementalsourceingestionjava.isolation;

import com.github.h3yow.incrementalsourceingestionjava.model.RawRecord;
import com.github.h3yow.incrementalsourceingestionjava.model.RejectionReason;

import java.util.List;
import java.util.Optional;

/**
 * 坏数据隔离区（本地模拟）。按稳定隔离键幂等保存，支持之后重新捞回。
 */
public interface IsolationStore {

    /** 幂等放入/更新一条隔离记录，返回保存后的记录 */
    IsolationRecord put(String id,
                        String sourceName,
                        RawRecord raw,
                        RejectionReason reason,
                        String detail,
                        int attempts,
                        IsolationStatus status);

    /** 捞出某来源当前仍待处理（FAILED / SKIPPED）的记录 */
    List<IsolationRecord> fetchPending(String sourceName);

    /** 按隔离键查询 */
    Optional<IsolationRecord> find(String id);

    /** 标记为已捞回成功 */
    IsolationRecord markRecovered(String id);
}
