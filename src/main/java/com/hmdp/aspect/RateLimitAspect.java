package com.hmdp.aspect;

import com.hmdp.annotation.RateLimit;
import com.hmdp.dto.Result;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.lang.reflect.Method;
import java.util.Collections;

@Aspect
@Component
@Slf4j
public class RateLimitAspect {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final DefaultRedisScript<Long> RATE_LIMIT_SCRIPT;

    static {
        RATE_LIMIT_SCRIPT = new DefaultRedisScript<>();
        RATE_LIMIT_SCRIPT.setLocation(new ClassPathResource("ratelimit.lua"));
        RATE_LIMIT_SCRIPT.setResultType(Long.class);
    }

    @Around("@annotation(rateLimit)")
    public Object around(ProceedingJoinPoint joinPoint, RateLimit rateLimit) throws Throwable {
        // 构建限流 key：前缀 + 方法标识 + 用户/IP
        String prefix = rateLimit.key();
        if (prefix.isEmpty()) {
            MethodSignature signature = (MethodSignature) joinPoint.getSignature();
            Method method = signature.getMethod();
            prefix = method.getDeclaringClass().getSimpleName() + ":" + method.getName();
        }

        String identity = getIdentity();
        String key = "rate_limit:" + prefix + ":" + identity;

        Long allowed = stringRedisTemplate.execute(
                RATE_LIMIT_SCRIPT,
                Collections.singletonList(key),
                String.valueOf(rateLimit.capacity()),
                String.valueOf(rateLimit.rate()),
                String.valueOf(System.currentTimeMillis()),
                "1"
        );

        if (allowed == null || allowed == 0L) {
            log.warn("请求被限流, key={}, identity={}", prefix, identity);
            return Result.fail(rateLimit.message());
        }

        return joinPoint.proceed();
    }

    private String getIdentity() {
        if (UserHolder.getUser() != null) {
            return "user:" + UserHolder.getUser().getId();
        }
        ServletRequestAttributes attrs =
                (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (attrs != null) {
            HttpServletRequest request = attrs.getRequest();
            return "ip:" + request.getRemoteAddr();
        }
        return "unknown";
    }
}
