package com.github.h3yow.incrementalsourceingestionjava.watermark;

/**
 * 水位存储。提交使用 CAS：必须带读取时的版本号，
 * 版本不匹配抛 {@link WatermarkConflictException}；位置回退抛 {@link WatermarkRegressionException}。
 */
public interface WatermarkStore {

    /** 读取某来源水位；从未提交过返回初始水位 */
    Watermark load(String sourceName);

    /**
     * CAS 提交水位。
     *
     * @param sourceName     来源实例
     * @param expectedVersion 读取时的版本号
     * @param newPosition    新位置（必须 >= 当前位置）
     */
    Watermark commit(String sourceName, long expectedVersion, long newPosition);
}
