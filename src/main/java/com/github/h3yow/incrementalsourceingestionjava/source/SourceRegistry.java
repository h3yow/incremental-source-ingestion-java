package com.github.h3yow.incrementalsourceingestionjava.source;

import java.util.Optional;

/**
 * 来源类型注册表：支持多种来源类型注册与按类型查找连接器。
 */
public interface SourceRegistry {

    void register(SourceConnector connector);

    Optional<SourceConnector> find(String sourceType, String sourceId);
}
