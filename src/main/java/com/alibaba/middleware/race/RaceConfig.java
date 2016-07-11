package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {

    //这些是写tair key的前缀
    public static String prex_tmall = "platformTmall_423559h636_";
    public static String prex_taobao = "platformTaobao_423559h636_";
    public static String prex_ratio = "ratio_423559h636_";

    //TeamCode
    public static String TeamCode = "423559h636_";

    //时间间隔
    public static long TimeInterval = 30000;

    //这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
    public static String JstormTopologyName = "423559h636";
    public static String MetaConsumerGroup = "423559h636";
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";

    /*public static String TairConfigServer = "192.168.159.128:5198";
    public static String TairSalveConfigServer = "xxx";
    public static String TairGroup = "group_1";
    public static Integer TairNamespace = 0;*/

    public static String TairConfigServer = "10.101.72.127:5198";
    public static String TairSalveConfigServer = "10.101.72.128:5198";
    public static String TairGroup = "group_tianchi";
    public static Integer TairNamespace = 47511;

    public static final int DEFAULT_BATCH_MSG_NUM = 32;

    public static String TMALL_STREAMID = "tmallOrderStream";

    public static String TAOBAO_STREAMID = "taobaoOrderStream";

    public static String PAYMENT_STREAMID = "paymentStream";

    public static String TM_PAYMENT_STREAMID = "tmallPaymentStream";

    public static String TB_PAYMENT_STREAMID = "taobaoPaymentStream";
}
