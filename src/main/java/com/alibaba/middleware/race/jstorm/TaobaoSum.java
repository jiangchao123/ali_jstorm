package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.MetaMessage;
import com.alibaba.middleware.race.model.PaymentMessage;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TaobaoSum implements IRichBolt {
    OutputCollector collector;

    //淘宝分钟对应的此分钟的交易额
    Map<Long, Double> timeMap = new HashMap<Long, Double>();

    //缓存已经求得淘宝的分钟以及此分钟的交易额
    Map<Long, Double> tmpMap = new HashMap<Long, Double>();
    TairOperatorImpl tairOperator;

    @Override
    public void execute(Tuple tuple) {
        Long time = tuple.getLong(0);
        MetaMessage paymentMessage = (MetaMessage) tuple.getValue(1);
        Double money = timeMap.get(time);
        if (money == null) {
            money = 0.00;
        }
        //System.out.println("===time:"+RaceConfig.prex_taobao + time + "    ========now money:" + (money+paymentMessage.getPayAmount()) + " = premoney:" + money + "+" + paymentMessage.getPayAmount());
        money = money + paymentMessage.getPayAmount();
        //System.out.println("=========TaobaoSum current total money is==============:" + money);
        timeMap.put(time, money);
        //collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
        //每隔一段时间写入tair
        new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(RaceConfig.TimeInterval);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    Iterator iter = timeMap.entrySet().iterator();
                    while (iter.hasNext()) {
                        Map.Entry entry = (Map.Entry) iter.next();
                        Long minuteTime = (Long) entry.getKey();
                        Double minuteMoney = (Double) entry.getValue();
                        if (tmpMap.get(minuteTime) != null && tmpMap.get(minuteTime) == minuteMoney) {
                            continue;
                        }
                        boolean isSuccess = tairOperator.write(RaceConfig.prex_taobao + minuteTime, minuteMoney);
                        if (isSuccess) {
                            tmpMap.put(minuteTime, minuteMoney);
                        }
                    }
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
