package com.rocketMq.demo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.log4j.Logger;
import java.util.List;


public class MqConsumer {
    private static Logger logger = Logger.getLogger(MqConsumer.class);

    public static void main(String[] args) {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("PushConsumer_yll");
        consumer.setNamesrvAddr("192.168.52.72:9876");
        try {
            consumer.subscribe("TopicA", "tagA||tagB");//可订阅多个tag，但是一个消息只能有一个tag
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                    Message msg = list.get(0);
                    logger.info(msg.toString());
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            consumer.start();
            logger.info("consumer启动成功");
        } catch (MQClientException e) {
            logger.error("消费者订阅消息失败，error：" + e);
        }
    }

}
