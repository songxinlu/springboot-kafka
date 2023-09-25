package com.example.springbootkafka.web;

import com.example.springbootkafka.ProducerUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.ProcessIdUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ProjectName: springboot-kafka
 * @Author: sxl
 * @Description: 生成者controller
 * @Date: 2023/9/25 10:35
 * @Version: 1.0
 */

/**
 * 带回调的生产者
 * kafkaTemplate提供了一个回调方法addCallback，我们可以在回调方法中监控消息是否发送成功 或 失败时做补偿处理，有两种写法
 */
@RestController("/producer")
@Slf4j
public class ProducerController {
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired
    private ProducerUtils producerUtils;

    @PostMapping("/kafka/send/{topic}")
    public Boolean sendMessage(@PathVariable("topic") String sendTopic, @RequestBody String data) {
        return producerUtils.sendMessage(sendTopic, data);
    }


    @PostMapping("/kafka/callbackOne/{topic}")
    public void sendMessage2(@PathVariable("topic") String sendTopic, @RequestBody String data) {
        kafkaTemplate.send(sendTopic, data).addCallback(success -> {
            // 消息发送到的topic
            String topic = success.getRecordMetadata().topic();
            // 消息发送到的分区
            int partition = success.getRecordMetadata().partition();
            // 消息在分区内的offset
            long offset = success.getRecordMetadata().offset();
            log.info("发送消息成功:" + topic + "-" + partition + "-" + offset);
        }, failure -> {
            log.error("发送消息失败:" + failure.getMessage());
        });
    }

    @PostMapping("/kafka/callbackTwo/{topic}")
    public void sendMessage3(@PathVariable("topic") String sendTopic, @RequestBody String data) {
        kafkaTemplate.send(sendTopic, data).addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
            @Override
            public void onFailure(Throwable ex) {
                log.error("发送消息失败：" + ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, Object> result) {
                log.info("发送消息成功：" + result.getRecordMetadata().topic() + "-"
                        + result.getRecordMetadata().partition() + "-" + result.getRecordMetadata().offset());
            }
        });
    }


    //kafka事务提交
    @PostMapping("/kafka/transaction/{topic}")
    public void sendMessage7(@PathVariable("topic") String sendTopic, @RequestBody String data) {
        // 声明事务：后面报错消息不会发出去
        kafkaTemplate.executeInTransaction(operations -> {
            operations.send(sendTopic, data);
            throw new RuntimeException("fail");
        });

        // 不声明事务：后面报错但前面消息已经发送成功了
        kafkaTemplate.send(sendTopic, data);
        throw new RuntimeException("fail");
    }

}
