package com.alibaba.middleware.race.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.MetaMessage;
import com.alibaba.middleware.race.model.MetaTuple;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.rocketmq.ConsumerFactory;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

public class RaceSentenceSpout implements IRichSpout , MessageListenerConcurrently{


    private static Logger LOG = LoggerFactory.getLogger(RaceSentenceSpout.class);
    SpoutOutputCollector _collector;
    boolean isStatEnable;
    protected transient LinkedBlockingDeque<MetaMessage> sendingQueue;
    transient DefaultMQPushConsumer consumer;
    //static AtomicInteger count = new AtomicInteger(0);

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        isStatEnable = JStormUtils.parseBoolean(conf.get("is.stat.enable"), false);
        this.sendingQueue = new LinkedBlockingDeque<MetaMessage>();

        /**
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */
        try {
            consumer = ConsumerFactory.makeInstance(this);
        } catch (InterruptedException e) {
            e.printStackTrace();
            LOG.error("Failed to create Meta Consumer ", e);
            throw new RuntimeException("Failed to create MetaConsumer :", e);
        } catch (MQClientException e) {
            e.printStackTrace();
            LOG.error("Failed  to create Meta Consumer ", e);
            throw new RuntimeException("Failed to create MetaConsumer :", e);
        }
        LOG.info("Successfully init spout");
    }

    @Override
    public void nextTuple() {
        MetaMessage metaTuple = null;
        if (sendingQueue != null && sendingQueue.size() > 0) {
            try {
                metaTuple = sendingQueue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
                LOG.error("sending Queue take error:", e);
            }
        }
        if (metaTuple == null) {
            return;
        }
        _collector.emit(new Values(metaTuple.getOrderId(), metaTuple));
    }

    @Override
    public void ack(Object id) {
        // Ignored
    }

    @Override
    public void fail(Object id) {
        _collector.emit(new Values(id), id);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("orderId", "metaTuple"));
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
    }

    @Override
    public void activate() {
        // TODO Auto-generated method stub

    }

    @Override
    public void deactivate() {
        // TODO Auto-generated method stub

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
        try {
            if (msgs != null && msgs.size() > 0) {
                String topic = consumeConcurrentlyContext.getMessageQueue().getTopic();
                for (MessageExt msg : msgs) {
                    byte [] body = msg.getBody();
                    if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                        //Info: 生产者停止生成数据, 并不意味着马上结束
                        System.out.println("Got the end signal");
                        continue;
                    }
                    MetaMessage metaMessage = null;
                    if (RaceConfig.MqTaobaoTradeTopic.equals(topic)) {
                        //System.out.println("------------MqTaobaoTradeTopic----------------");
                        OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
                        metaMessage = new MetaMessage(orderMessage, topic, msg.getMsgId());
                    }
                    if (RaceConfig.MqTmallTradeTopic.equals(topic)) {
                        //System.out.println("------------MqTmallTradeTopic----------------");
                        OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
                        metaMessage = new MetaMessage(orderMessage, topic, msg.getMsgId());
                    }
                    if (RaceConfig.MqPayTopic.equals(topic)) {
                        //System.out.println("------------MqPayTopic----------------");
                        PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
                        metaMessage = new MetaMessage(paymentMessage, topic, msg.getMsgId());
                    }
                    if (metaMessage != null) {
                        sendingQueue.offer(metaMessage);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to emit :" , e);
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}
