package com.alibaba.middleware.race.rocketmq;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;


/**
 * Consumer，订阅消息
 */

/**
 * RocketMq消费组信息我们都会再正式提交代码前告知选手
 */
public class ConsumerFactory {

    private volatile static DefaultMQPushConsumer consumer = null;

    public static DefaultMQPushConsumer makeInstance(MessageListenerConcurrently listener) throws InterruptedException, MQClientException {
        if (consumer == null) {
            synchronized (ConsumerFactory.class) {
                if (consumer == null) {
                    consumer = new DefaultMQPushConsumer("please_rename_unique_group_name_4");
                    /**
                     * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
                     * 如果非第一次启动，那么按照上次消费的位置继续消费
                     */
                    consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
                    consumer.setConsumerGroup(RaceConfig.MetaConsumerGroup);
                    consumer.subscribe(RaceConfig.MqPayTopic, "*");
                    consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
                    consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");

                    //在本地搭建好broker后,记得指定nameServer的地址
                    //consumer.setNamesrvAddr("192.168.159.128:9876");

                    consumer.registerMessageListener(listener);
                    consumer.setPullBatchSize(RaceConfig.DEFAULT_BATCH_MSG_NUM);

                    consumer.start();

                    System.out.println("Consumer Started.");
                    return consumer;
                }
            }
        }
        return consumer;
    }
}
