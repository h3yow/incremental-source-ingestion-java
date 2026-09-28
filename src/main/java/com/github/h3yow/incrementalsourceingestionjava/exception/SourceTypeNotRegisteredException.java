package com.github.h3yow.incrementalsourceingestionjava.exception;

/**
 * 请求的来源类型没有注册时抛出。
 */
public class SourceTypeNotRegisteredException extends RuntimeException {
    public SourceTypeNotRegisteredException(String sourceType) {
        super("来源类型[" + sourceType + "]未注册");
    }
}
