package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * 读写tair所需要的集群信息，如masterConfigServer/slaveConfigServer地址/
 * group 、namespace我们都会在正式提交代码前告知选手
 */
public class TairOperatorImpl {

    private String masterConfigServer;
    private String slaveConfigServer;
    private String groupName;
    private int namespace;
    // 创建config server列表
    List<String> confServers = new ArrayList<String>();
    // 创建客户端实例
    private DefaultTairManager tairManager = new DefaultTairManager();

    public TairOperatorImpl(String masterConfigServer,
                            String slaveConfigServer,
                            String groupName,
                            int namespace) {
        this.masterConfigServer = masterConfigServer;
        this.slaveConfigServer = slaveConfigServer;
        this.groupName = groupName;
        this.namespace = namespace;
        confServers.add(masterConfigServer);
        confServers.add(slaveConfigServer);
        tairManager.setConfigServerList(confServers);
        // 设置组名
        tairManager.setGroupName(groupName);
        // 初始化客户端
        tairManager.init();
    }

    public boolean write(Serializable key, Serializable value) {
        ResultCode rc = tairManager.put(this.namespace, key, value);
        if (rc.isSuccess()){
            return true;
        } else {
            return false;
        }
    }

    public Object get(Serializable key) {
        return tairManager.get(this.namespace, key);
    }

    public boolean remove(Serializable key) {
        return false;
    }

    public void close(){
    }

    //天猫的分钟交易额写入tair
    public static void main(String [] args) throws Exception {
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
        //假设这是付款时间
        Long millisTime = System.currentTimeMillis();
        //由于整分时间戳是10位数，所以需要转换成整分时间戳
        Long minuteTime = (millisTime / 1000 / 60) * 60;
        //假设这一分钟的交易额是100;
        Double money = 100.0;
        //写入tair
        tairOperator.write(RaceConfig.prex_tmall + minuteTime, money);
    }
}
