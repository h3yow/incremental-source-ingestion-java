package com.github.h3yow.incrementalsourceingestionjava.skipped;

import com.github.h3yow.incrementalsourceingestionjava.model.SkippedRecord;

import java.util.List;
import java.util.Optional;

/**
 * 失败隔离区：保存重试耗尽后被跳过的记录，支持之后重新捞回。
 */
public interface SkippedStore {

    void save(SkippedRecord record);

    List<SkippedRecord> findBySource(String sourceId);

    /** 重投成功后移除隔离记录。 */
    void remove(String sourceId, String dedupKey);

    Optional<SkippedRecord> find(String sourceId, String dedupKey);

    int count();
}
