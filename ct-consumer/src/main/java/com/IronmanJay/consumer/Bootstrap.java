package com.IronmanJay.consumer;

import com.IronmanJay.consumer.bean.CalllogConsumer;
import com.IronmanJay.ct.common.bean.Consumer;

/**
 * 启动消费者
 * 使用kafka的消费者获取flume采集的数据
 * 将数据存储搭配hbase中去
 */
public class Bootstrap {

    public static void main(String[] args) throws Exception {
        // 创建消费者
        Consumer consumer = new CalllogConsumer();
        // 消费数据
        consumer.consume();
        // 关闭资源
        consumer.close();
    }

}
