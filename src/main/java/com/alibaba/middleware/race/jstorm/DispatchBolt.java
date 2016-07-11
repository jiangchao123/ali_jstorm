package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.MetaMessage;

import java.util.*;

public class DispatchBolt implements IRichBolt {
    OutputCollector collector;

    //天猫订单和topic的对应关系表
    Map<Long, String> tmallMap = new HashMap<Long, String>();

    //淘宝订单号和topic的对应关系表
    Map<Long, String> taobaoMap = new HashMap<Long, String>();

    //存储暂时未解决属于天猫还是淘宝的付款消息
    List<MetaMessage> metaMessages = new ArrayList<MetaMessage>();

    //过滤重复消息
    private Set<String> msgIds = new HashSet<String>();

    private synchronized void handleOldMessages() {
        //System.out.println("==========handle old messages================");
        for (int i = 0; i < metaMessages.size(); i++) {
            MetaMessage metaMessage = metaMessages.get(i);
            if (tmallMap.containsKey(metaMessage.getOrderId())) {
                long time = metaMessage.getCreateTime()/60000 * 60;
                metaMessages.remove(i);
                i--;
                collector.emit(RaceConfig.TM_PAYMENT_STREAMID, new Values(time, metaMessage));
            } else if (taobaoMap.containsKey(metaMessage.getOrderId())) {
                long time = metaMessage.getCreateTime()/60000 * 60;
                metaMessages.remove(i);
                i--;
                collector.emit(RaceConfig.TB_PAYMENT_STREAMID, new Values(time, metaMessage));
            }
        }
    }

    @Override
    public void execute(Tuple tuple) {
        MetaMessage metaTuple = (MetaMessage) tuple.getValue(1);
        if (metaTuple != null ) {
                if (RaceConfig.MqPayTopic.equals(metaTuple.getTopic())) {
                    if (msgIds.add(metaTuple.getMsgId())) {
                        long time = metaTuple.getCreateTime()/60000 * 60;
                        collector.emit(RaceConfig.PAYMENT_STREAMID, new Values(time, metaTuple));
                        if (tmallMap.get(metaTuple.getOrderId()) == null && taobaoMap.get(metaTuple.getOrderId()) == null) {
                            metaMessages.add(metaTuple);
                        } else if (tmallMap.containsKey(metaTuple.getOrderId())) {
                            collector.emit(RaceConfig.TM_PAYMENT_STREAMID, new Values(time, metaTuple));
                        } else {
                            collector.emit(RaceConfig.TB_PAYMENT_STREAMID, new Values(time, metaTuple));
                        }
                    }
                } else if (RaceConfig.MqTmallTradeTopic.equals(metaTuple.getTopic())) {
                    tmallMap.put(metaTuple.getOrderId(), RaceConfig.MqTmallTradeTopic);
                } else {
                    taobaoMap.put(metaTuple.getOrderId(), RaceConfig.MqTaobaoTradeTopic);
                }
        }
        //collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(RaceConfig.PAYMENT_STREAMID, new Fields("time", "meta"));
        declarer.declareStream(RaceConfig.TB_PAYMENT_STREAMID, new Fields("time", "tmMeta"));
        declarer.declareStream(RaceConfig.TM_PAYMENT_STREAMID, new Fields("time", "tmMeta"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
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
