package com.alibaba.middleware.race.jstorm;

import backtype.storm.StormSubmitter;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.middleware.race.RaceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology；
 * 所以这个主类路径一定要正确
 */
public class RaceTopology {

    private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);


    public static void main(String[] args) throws Exception {

        Config conf = new Config();
        conf.setNumWorkers(4);
        conf.setNumAckers(0);
        int spout_Parallelism_hint = 4;
        int split_Parallelism_hint = 8;
        int count_Parallelism_hint = 4;

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RaceSentenceSpout(), spout_Parallelism_hint);
        //builder.setBolt("tmall", new TMallBolt(), split_Parallelism_hint).fieldsGrouping("spout", new Fields("orderId"));
        //builder.setBolt("taobao", new TaobaoBolt(), split_Parallelism_hint).fieldsGrouping("spout", new Fields("orderId"));
        //builder.setBolt("platform", new PayPlatformBolt(), split_Parallelism_hint).shuffleGrouping("spout");
        builder.setBolt("dispatch", new DispatchBolt(), split_Parallelism_hint).fieldsGrouping("spout", new Fields("orderId"));
        builder.setBolt("tmallsum", new TMallSum(), count_Parallelism_hint).fieldsGrouping("dispatch", RaceConfig.TM_PAYMENT_STREAMID, new Fields("time"));
        builder.setBolt("taobaosum", new TaobaoSum(), count_Parallelism_hint).fieldsGrouping("dispatch", RaceConfig.TB_PAYMENT_STREAMID, new Fields("time"));
        builder.setBolt("ratio", new PWRatio(), count_Parallelism_hint).fieldsGrouping("dispatch", RaceConfig.PAYMENT_STREAMID, new Fields("time"));
        String topologyName = RaceConfig.JstormTopologyName;

        /*LocalCluster cluster = new LocalCluster();
        System.out.println("=====================begin============");
        cluster.submitTopology(topologyName, conf, builder.createTopology());
        Thread.sleep(600000);
        System.out.println("====================end===============");
        cluster.killTopology(topologyName);
        cluster.shutdown();*/

        try {
            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
