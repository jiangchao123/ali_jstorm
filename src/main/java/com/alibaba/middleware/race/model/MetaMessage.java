package com.alibaba.middleware.race.model;

import java.io.Serializable;

/**
 * Created by hasee on 2016/6/30.
 */
public class MetaMessage implements Serializable {
    private static final long serialVersionUID = 2277714452693486955L;

    private String msgId; //消息ID

    private long orderId; //订单ID

    private double payAmount; //金额

    /**
     * 支付平台
     * 0，pC
     * 1，无线
     * 2, 订单信息，无
     */
    private short payPlatform; //支付平台

    /**
     * 付款记录创建时间
     */
    private long createTime; //13位数，毫秒级时间戳，初赛要求的时间都是指该时间

    private String topic; //消息种类

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public double getPayAmount() {
        return payAmount;
    }

    public void setPayAmount(double payAmount) {
        this.payAmount = payAmount;
    }

    public short getPayPlatform() {
        return payPlatform;
    }

    public void setPayPlatform(short payPlatform) {
        this.payPlatform = payPlatform;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    //Kryo默认需要无参数构造函数
    public MetaMessage() {
    }

    public MetaMessage(PaymentMessage paymentMessage, String topic, String msgId) {
        this.orderId = paymentMessage.getOrderId();
        this.payPlatform = paymentMessage.getPayPlatform();
        this.createTime = paymentMessage.getCreateTime();
        this.payAmount = paymentMessage.getPayAmount();
        this.topic = topic;
        this.msgId = msgId;
    }

    public MetaMessage(OrderMessage orderMessage, String topic, String msgId) {
        this.orderId = orderMessage.getOrderId();
        this.payAmount = orderMessage.getTotalPrice();
        this.createTime = orderMessage.getCreateTime();
        this.payPlatform = 2;
        this.topic = topic;
        this.msgId = msgId;
    }

    public MetaMessage(long orderId, double payAmount, short payPlatform, long createTime, String topic, String msgId) {
        this.orderId = orderId;
        this.payAmount = payAmount;
        this.payPlatform = payPlatform;
        this.createTime = createTime;
        this.topic = topic;
        this.msgId = msgId;
    }

    @Override
    public String toString() {
        return "MetaMessage{" +
                "orderId=" + orderId +
                ", payAmount=" + payAmount +
                ", payPlatform=" + payPlatform +
                ", createTime=" + createTime +
                ", topic='" + topic + '\'' +
                '}';
    }


}
