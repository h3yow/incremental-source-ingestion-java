package com.github.h3yow.incrementalsourceingestionjava.source;

import java.util.Optional;

/**
 * 来源类型 / 来源实例的注册中心。
 */
public interface SourceRegistry {

    void registerType(SourceType type);

    void registerConnector(SourceConnector connector);

    Optional<SourceConnector> connector(String sourceName);

    SourceType requireType(String type);
}
