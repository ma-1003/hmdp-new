package com.hmdp.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    // 秒杀订单主题
    public static final String SECKILL_ORDER_TOPIC = "seckill-order";
    // 秒杀订单死信主题
    public static final String SECKILL_ORDER_DLT_TOPIC = "seckill-order.DLT";
    // 缓存删除重试主题
    public static final String CACHE_DELETE_TOPIC = "cache-delete-retry";
    // 缓存删除死信主题
    public static final String CACHE_DELETE_DLT_TOPIC = "cache-delete-retry.DLT";

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * 创建秒杀订单主题，3个分区，1个副本
     */
    @Bean
    public NewTopic seckillOrderTopic() {
        return TopicBuilder.name(SECKILL_ORDER_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * 创建死信主题，3个分区，1个副本
     */
    @Bean
    public NewTopic seckillOrderDltTopic() {
        return TopicBuilder.name(SECKILL_ORDER_DLT_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * 创建缓存删除重试主题
     */
    @Bean
    public NewTopic cacheDeleteTopic() {
        return TopicBuilder.name(CACHE_DELETE_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * 创建缓存删除死信主题
     */
    @Bean
    public NewTopic cacheDeleteDltTopic() {
        return TopicBuilder.name(CACHE_DELETE_DLT_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "hmdp-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * 主消费者容器工厂，配置死信发布恢复器
     * 消费失败重试3次后，消息自动发送到死信主题
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            KafkaTemplate<String, String> kafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);

        // 死信发布恢复器：消费失败的消息会被发送到 .DLT 主题
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
        // 重试3次，每次间隔1秒，之后发送到死信主题
        factory.setErrorHandler(new SeekToCurrentErrorHandler(recoverer, new FixedBackOff(1000L, 3L)));

        return factory;
    }

    /**
     * 死信消费者容器工厂，不再配置死信恢复（避免无限循环）
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> dltKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(dltConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        // 死信消费者不再配置 DeadLetterPublishingRecoverer，失败直接记录日志
        return factory;
    }

    @Bean
    public ConsumerFactory<String, String> dltConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "hmdp-dlt-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }
}
