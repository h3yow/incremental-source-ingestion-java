package com.github.h3yow.incrementalsourceingestionjava.local;

import com.github.h3yow.incrementalsourceingestionjava.isolation.IsolationStore;
import com.github.h3yow.incrementalsourceingestionjava.normalize.Normalizer;
import com.github.h3yow.incrementalsourceingestionjava.policy.DefaultRetryPolicy;
import com.github.h3yow.incrementalsourceingestionjava.policy.RetryPolicy;
import com.github.h3yow.incrementalsourceingestionjava.service.IngestionService;
import com.github.h3yow.incrementalsourceingestionjava.sink.TargetSink;
import com.github.h3yow.incrementalsourceingestionjava.source.SourceRegistry;
import com.github.h3yow.incrementalsourceingestionjava.watermark.WatermarkStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

/**
 * 本地模拟组件的 Spring 装配：水位 / 目标端 / 隔离区均落盘到 {@code ingestion.data-dir}（默认 data/），
 * 进程重启后水位、目标数据与隔离记录都能恢复。
 */
@Configuration
public class LocalIngestionConfig {

    @Bean
    public SourceRegistry sourceRegistry() {
        return new DefaultSourceRegistry();
    }

    @Bean
    public WatermarkStore watermarkStore(@Value("${ingestion.data-dir:data}") String dataDir) {
        return new FileWatermarkStore(Path.of(dataDir));
    }

    @Bean
    public TargetSink targetSink(@Value("${ingestion.data-dir:data}") String dataDir) {
        return new FileTargetSink(Path.of(dataDir));
    }

    @Bean
    public IsolationStore isolationStore(@Value("${ingestion.data-dir:data}") String dataDir) {
        return new FileIsolationStore(Path.of(dataDir));
    }

    @Bean
    public RetryPolicy retryPolicy() {
        return new DefaultRetryPolicy(3);
    }

    @Bean
    public Normalizer normalizer(SourceRegistry registry) {
        return new Normalizer(registry);
    }

    @Bean
    public IngestionService ingestionService(SourceRegistry registry, Normalizer normalizer,
                                             WatermarkStore watermarkStore, TargetSink targetSink,
                                             IsolationStore isolationStore, RetryPolicy retryPolicy) {
        return new IngestionService(registry, normalizer, watermarkStore, targetSink,
                isolationStore, retryPolicy);
    }
}
