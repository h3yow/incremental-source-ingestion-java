package com.github.h3yow.incrementalsourceingestionjava.source;

import com.github.h3yow.incrementalsourceingestionjava.model.RawRecord;

import java.util.List;

/**
 * 数据源连接器：按起始序号（不含）拉取本批数据。
 */
public interface SourceConnector {

    String sourceType();

    String sourceId();

    /**
     * 拉取序号严格大于 afterSequence 的记录，按序号升序，最多 limit 条。
     */
    List<RawRecord> fetch(long afterSequence, int limit);
}
