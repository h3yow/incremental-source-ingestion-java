package com.github.h3yow.incrementalsourceingestionjava.watermark;

/**
 * 并发拉取导致版本号对不上（断点互相覆盖），CAS 提交失败。
 */
public class WatermarkConflictException extends RuntimeException {
    public WatermarkConflictException(String message) {
        super(message);
    }
}
