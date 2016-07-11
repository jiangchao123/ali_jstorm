package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.MetaMessage;
import com.alibaba.middleware.race.model.MetaTuple;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class TaobaoBolt implements IRichBolt {
    OutputCollector collector;
    //订单和topic的对应关系表
    Map<Long, String> taobaoMap = new HashMap<Long, String>();
    List<MetaMessage> metaMessages = new ArrayList<MetaMessage>();
    long lasttime;

    private synchronized void handleOldMessages() {
        for (int i = 0; i < metaMessages.size(); i++) {
            MetaMessage metaMessage = metaMessages.get(i);
            if (taobaoMap.get(metaMessage.getOrderId()) != null && taobaoMap.get(metaMessage.getOrderId()).equals(RaceConfig.MqTaobaoTradeTopic)) {
                long time = metaMessage.getCreateTime()/60000 * 60;
                metaMessages.remove(i);
                i--;
                collector.emit(new Values(time, metaMessage));
            }
        }
    }

    @Override
    public void execute(Tuple tuple) {
        //System.out.println("==============TaobaoBolt bolt begin==========");
        /*long currenttime = System.currentTimeMillis();
        if (currenttime - lasttime > RaceConfig.TimeInterval && metaMessages.size() > 0) {
            new Thread(){
                @Override
                public void run() {
                    handleOldMessages();
                }
            }.start();
        }*/
        MetaMessage metaTuple = (MetaMessage) tuple.getValue(1);
        if (metaTuple != null ) {
            if (RaceConfig.MqTmallTradeTopic.equals(metaTuple.getTopic())) {
                taobaoMap.put(metaTuple.getOrderId(), RaceConfig.MqTmallTradeTopic);
            } else if (RaceConfig.MqTaobaoTradeTopic.equals(metaTuple.getTopic())) {
                taobaoMap.put(metaTuple.getOrderId(), RaceConfig.MqTaobaoTradeTopic);
            } else if (taobaoMap.get(metaTuple.getOrderId()) == null) {
                metaMessages.add(metaTuple);
            } else if (RaceConfig.MqPayTopic.equals(metaTuple.getTopic())) {
                if (taobaoMap.get(metaTuple.getOrderId()).equals(RaceConfig.MqTaobaoTradeTopic)) {
                    long time = metaTuple.getCreateTime()/60000 * 60;
                    collector.emit(new Values(time, metaTuple));
                }
            }
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time", "taobaoMeta"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.lasttime = System.currentTimeMillis();
        new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(RaceConfig.TimeInterval);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    handleOldMessages();
                }
            }
        }.start();
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}
