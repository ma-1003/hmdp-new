package com.hmdp.listener;

import com.hmdp.config.KafkaConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 缓存删除重试消费者
 * 当 ShopServiceImpl.update() 中删除缓存失败时，消息会发到 Kafka
 * 本消费者负责重试删除，失败3次后进入死信主题，记录日志等待人工处理
 */
@Component
@Slf4j
public class CacheDeleteListener {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 主消费者：重试删除缓存
     * 消费失败会自动重试3次，之后进入死信主题
     */
    @KafkaListener(topics = KafkaConfig.CACHE_DELETE_TOPIC, containerFactory = "kafkaListenerContainerFactory")
    public void retryDeleteCache(ConsumerRecord<String, String> record) {
        String cacheKey = record.value();
        log.info("收到缓存删除重试消息, cacheKey={}", cacheKey);
        Boolean deleted = stringRedisTemplate.delete(cacheKey);
        if (Boolean.TRUE.equals(deleted)) {
            log.info("缓存删除重试成功, cacheKey={}", cacheKey);
        } else {
            log.warn("缓存key不存在或已被删除, cacheKey={}", cacheKey);
        }
    }

    /**
     * 死信消费者：重试多次仍失败的缓存删除消息
     * 记录日志，等待人工介入（实际中可接入告警系统）
     */
    @KafkaListener(topics = KafkaConfig.CACHE_DELETE_DLT_TOPIC, containerFactory = "dltKafkaListenerContainerFactory")
    public void handleDltDeleteCache(ConsumerRecord<String, String> record) {
        String cacheKey = record.value();
        log.error("缓存删除最终失败，需人工介入, cacheKey={}", cacheKey);
    }
}
