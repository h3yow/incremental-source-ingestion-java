package com.github.h3yow.incrementalsourceingestionjava.source;

import com.github.h3yow.incrementalsourceingestionjava.model.RawRecord;

import java.util.List;

/**
 * 一个来源实例（本地模拟）。
 */
public interface SourceConnector {

    /** 来源实例名，水位/隔离按此维度隔离 */
    String sourceName();

    /** 所属来源类型 */
    SourceType sourceType();

    /**
     * 按游标增量拉取：返回游标严格大于 afterCursor 的记录，按游标升序。
     *
     * @param afterCursor 起始水位（不含）；首次为 null
     * @param limit       本批最大条数
     */
    List<RawRecord> fetch(Long afterCursor, int limit);
}
