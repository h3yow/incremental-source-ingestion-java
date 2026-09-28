package com.github.h3yow.incrementalsourceingestionjava.watermark;

/**
 * 试图把水位提交到比当前更靠前的位置（水位回退），明确拒绝。
 */
public class WatermarkRegressionException extends RuntimeException {
    public WatermarkRegressionException(String message) {
        super(message);
    }
}
