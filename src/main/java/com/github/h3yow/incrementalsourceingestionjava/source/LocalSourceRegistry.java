package com.github.h3yow.incrementalsourceingestionjava.source;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本地来源注册表：按 (来源类型, 来源ID) 注册并查找连接器，
 * 支持多种来源类型共存，类型不存在时返回空（由上层给出明确报错）。
 */
public class LocalSourceRegistry implements SourceRegistry {

    private final Map<String, SourceConnector> connectors = new ConcurrentHashMap<>();

    static String keyOf(String sourceType, String sourceId) {
        return sourceType + "|" + sourceId;
    }

    @Override
    public void register(SourceConnector connector) {
        connectors.put(keyOf(connector.sourceType(), connector.sourceId()), connector);
    }

    @Override
    public Optional<SourceConnector> find(String sourceType, String sourceId) {
        return Optional.ofNullable(connectors.get(keyOf(sourceType, sourceId)));
    }
}
