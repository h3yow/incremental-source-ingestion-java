package com.github.h3yow.incrementalsourceingestionjava.model;

/**
 * 请求了未注册的来源类型。
 */
public class UnknownSourceTypeException extends RuntimeException {
    public UnknownSourceTypeException(String message) {
        super(message);
    }
}
