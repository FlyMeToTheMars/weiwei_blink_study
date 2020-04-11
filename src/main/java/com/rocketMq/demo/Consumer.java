package com.rocketMq.demo;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class Consumer {
    public static void main(String[] args) throws InterruptedException, MQClientException {

        // 实例化 consumer，指定 consumer 组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("flink_test");

        // 指定 name server
        consumer.setNamesrvAddr("192.168.52.72:9876");

//        consumer.setVipChannelEnabled(false);

        // 订阅1个或多个topic
        consumer.subscribe("TopicA", "*");

        // 注册回调，当收到消息时执行
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            public ConsumeConcurrentlyStatus consumeMessage(final List<MessageExt> msgs,
                                                            final ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // 启动 consumer
        consumer.start();

        System.out.printf("Consumer Started.%n");

    }
}
