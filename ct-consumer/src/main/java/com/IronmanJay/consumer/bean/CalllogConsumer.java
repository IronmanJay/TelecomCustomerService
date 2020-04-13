package com.IronmanJay.consumer.bean;

import com.IronmanJay.consumer.dao.HBaseDao;
import com.IronmanJay.ct.common.bean.Consumer;
import com.IronmanJay.ct.common.constant.Names;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * 通话日志的消费者对象
 */
public class CalllogConsumer implements Consumer {
    /**
     * 消费数据
     */
    public void consume() {
        // 创建配置对象
        Properties prop = new Properties();
        try {
            prop.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("consumer.properties"));
            // 获取flume采集的数据
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
            // 关注主题
            consumer.subscribe(Arrays.asList(Names.TOPIC.getValue()));
            // HBase数据访问对象
            HBaseDao dao = new HBaseDao();
            // 初始化
            dao.init();
            // 消费数据
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.out.println(consumerRecord.value());
                    // 插入数据
                    dao.insertData(consumerRecord.value());
//                    Calllog log = new Calllog(consumerRecord.value());
//                    dao.insertData(log);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭资源
     *
     * @throws IOException
     */
    public void close() throws IOException {

    }
}
