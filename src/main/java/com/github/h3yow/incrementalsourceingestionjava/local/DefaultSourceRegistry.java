package com.github.h3yow.incrementalsourceingestionjava.local;

import com.github.h3yow.incrementalsourceingestionjava.model.UnknownSourceTypeException;
import com.github.h3yow.incrementalsourceingestionjava.source.SourceConnector;
import com.github.h3yow.incrementalsourceingestionjava.source.SourceRegistry;
import com.github.h3yow.incrementalsourceingestionjava.source.SourceType;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** 基于内存 Map 的来源注册中心：注册多种来源类型与来源实例。 */
public class DefaultSourceRegistry implements SourceRegistry {

    private final Map<String, SourceType> types = new ConcurrentHashMap<>();
    private final Map<String, SourceConnector> connectors = new ConcurrentHashMap<>();

    @Override
    public void registerType(SourceType type) {
        types.put(type.type(), type);
    }

    @Override
    public void registerConnector(SourceConnector connector) {
        types.putIfAbsent(connector.sourceType().type(), connector.sourceType());
        connectors.put(connector.sourceName(), connector);
    }

    @Override
    public Optional<SourceConnector> connector(String sourceName) {
        return Optional.ofNullable(connectors.get(sourceName));
    }

    @Override
    public SourceType requireType(String type) {
        SourceType t = types.get(type);
        if (t == null) {
            throw new UnknownSourceTypeException("未注册的来源类型: " + type);
        }
        return t;
    }
}
