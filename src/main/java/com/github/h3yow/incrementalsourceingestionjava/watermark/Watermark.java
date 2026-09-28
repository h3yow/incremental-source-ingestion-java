package com.github.h3yow.incrementalsourceingestionjava.watermark;

/**
 * 某来源实例的采集水位。
 *
 * @param sourceName    来源实例
 * @param position      已提交到的游标位置（含）
 * @param version       乐观版本号，每次成功提交 +1；用于防止并发拉取互相覆盖
 * @param lastUpdatedAt 最后提交时间（epoch millis）
 */
@com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
public record Watermark(
        String sourceName,
        long position,
        long version,
        long lastUpdatedAt
) {

    public static Watermark initial(String sourceName) {
        return new Watermark(sourceName, Long.MIN_VALUE, 0L, 0L);
    }

    /** 水位尚未推进（首次采集） */
    @com.fasterxml.jackson.annotation.JsonIgnore
    public boolean isEmpty() {
        return version == 0L;
    }
}
