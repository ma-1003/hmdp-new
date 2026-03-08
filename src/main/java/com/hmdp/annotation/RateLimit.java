package com.hmdp.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 令牌桶限流注解，标记在 Controller 方法上
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RateLimit {

    /**
     * 限流 key 前缀，默认使用方法全限定名
     */
    String key() default "";

    /**
     * 桶容量（最大令牌数）
     */
    int capacity() default 10;

    /**
     * 每秒生成的令牌数
     */
    int rate() default 5;

    /**
     * 限流提示信息
     */
    String message() default "请求过于频繁，请稍后再试";
}
