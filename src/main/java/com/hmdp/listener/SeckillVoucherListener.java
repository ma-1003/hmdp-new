package com.hmdp.listener;

import cn.hutool.json.JSONUtil;
import com.hmdp.config.KafkaConfig;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.impl.SeckillVoucherServiceImpl;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Component
@Slf4j
public class SeckillVoucherListener {

    @Resource
    SeckillVoucherServiceImpl seckillVoucherService;
    @Resource
    VoucherOrderServiceImpl voucherOrderService;

    /**
     * 主消费者：消费秒杀订单消息
     * 消费失败重试3次后，消息自动进入死信主题
     */
    @KafkaListener(topics = KafkaConfig.SECKILL_ORDER_TOPIC, containerFactory = "kafkaListenerContainerFactory")
    public void receiveSeckillOrder(ConsumerRecord<String, String> record) {
        String msg = record.value();
        log.info("主题消费 - 收到秒杀订单消息, partition={}, offset={}", record.partition(), record.offset());
        VoucherOrder voucherOrder = JSONUtil.toBean(msg, VoucherOrder.class);
        log.info("订单信息: {}", voucherOrder);
        handleOrder(voucherOrder);
    }

    /**
     * 死信消费者：处理消费失败的消息
     */
    @KafkaListener(topics = KafkaConfig.SECKILL_ORDER_DLT_TOPIC, containerFactory = "dltKafkaListenerContainerFactory")
    public void receiveDltSeckillOrder(ConsumerRecord<String, String> record) {
        String msg = record.value();
        log.warn("死信主题消费 - 收到失败的秒杀订单消息, partition={}, offset={}", record.partition(), record.offset());
        try {
            VoucherOrder voucherOrder = JSONUtil.toBean(msg, VoucherOrder.class);
            log.warn("死信订单信息: {}", voucherOrder);
            handleOrder(voucherOrder);
        } catch (Exception e) {
            log.error("死信消息处理最终失败，需人工介入, message={}", msg, e);
        }
    }

    private void handleOrder(VoucherOrder voucherOrder) {
        voucherOrderService.save(voucherOrder);
        Long voucherId = voucherOrder.getVoucherId();
        seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId).gt("stock", 0)
                .update();
    }
}
