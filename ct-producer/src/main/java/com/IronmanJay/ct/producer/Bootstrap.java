package com.IronmanJay.ct.producer;

import com.IronmanJay.ct.common.bean.Producer;
import com.IronmanJay.ct.producer.bean.LocalFileProducer;
import com.IronmanJay.ct.producer.io.LocalFileDataIn;
import com.IronmanJay.ct.producer.io.LocalFileDataOut;

/**
 * 启动对象
 */
public class Bootstrap {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("系统参数不正确，请按照指定格式传递:java -jar Produce.jar path1 path2");
            System.exit(1);
        }
        // 构建生产者对象
        Producer producer = new LocalFileProducer();
//        producer.setIn(new LocalFileDataIn("D:\\Software\\TelecomCustomerService\\ct-producer\\src\\main\\java\\com\\IronmanJay\\ct\\producer\\data\\contact.log"));
//        producer.setOut(new LocalFileDataOut("D:\\Software\\TelecomCustomerService\\ct-producer\\src\\main\\java\\com\\IronmanJay\\ct\\producer\\data\\call.log"));
        producer.setIn(new LocalFileDataIn(args[0]));
        producer.setOut(new LocalFileDataOut(args[1]));
        // 生产数据
        producer.produce();
        // 关闭生产者对象
        producer.close();
    }

}
