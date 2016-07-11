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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class PWRatio implements IRichBolt {
    OutputCollector collector;
    TairOperatorImpl tairOperator;
    static ConcurrentHashMap<Long, Double> pcMap;
    static ConcurrentHashMap<Long, Double> wireMap;
    Map<Long, Double> tmpMap = new HashMap<Long, Double>();

    /*
    计算历史pc和wire的交易总额，然后计算ratio，若和之前的值不一样，则写入tair更新结果。
     */
    private synchronized  void  writeTair(Map<Long, Double> map1, Map<Long, Double> map2) {
        List<Long> times = new ArrayList<Long>();
        Double pc = 0.00;
        Double wire = 0.00;
        Iterator iter = map1.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            Long minuteTime = (Long) entry.getKey();
            times.add(minuteTime);
        }
        Collections.sort(times);
        for (Long time : times) {
            pc += map1.get(time);
            if (map2.get(time) != null) {
                wire += map2.get(time);
            }
            if (pc != 0) {
                double ratio = wire/pc;
                if (tmpMap.get(time) != null && tmpMap.get(time) == ratio) {
                    continue;
                }
                boolean isSuccess = tairOperator.write(RaceConfig.prex_ratio + time, ratio);
                if (isSuccess) {
                    tmpMap.put(time, ratio);
                }
            }
        }
    }

    @Override
    public void execute(Tuple tuple) {
        Long time = tuple.getLong(0);
        MetaMessage paymentMessage = (MetaMessage) tuple.getValue(1);
        Double money = null;
        if (paymentMessage.getPayPlatform() == 0) { //PC
            money = pcMap.get(time);
            if (money == null) {
                money = 0.00;
            }
            //System.out.println("==========ratio :pc   ====premoney:" + money );
            money += paymentMessage.getPayAmount();
            //System.out.println("==========ratio :pc   ====this message money is:" + paymentMessage.getPayAmount() + " curretn money is :" + money);
            pcMap.put(time, money);
        } else if (paymentMessage.getPayPlatform() == 1) { //Wire
            money = wireMap.get(time);
            if (money == null) {
                money = 0.00;
            }
            //System.out.println("==========ratio :wire   ====premoney:" + money );
            money += paymentMessage.getPayAmount();
            //System.out.println("==========ratio :wire   ====this message money is:" + paymentMessage.getPayAmount() + " curretn money is :" + money);
            wireMap.put(time, money);
        } else {
            return;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
        this.pcMap = new ConcurrentHashMap<Long, Double>();
        this.wireMap = new ConcurrentHashMap<Long, Double>();

        //每隔一段时间计算ratio值写入tair
        new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(RaceConfig.TimeInterval);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    writeTair(pcMap, wireMap);
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
