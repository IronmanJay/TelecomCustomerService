package com.IronmanJay.ct.consumer.coprocessor;

import com.IronmanJay.ct.common.bean.BaseDao;
import com.IronmanJay.ct.common.constant.Names;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 使用协处理器保存被叫用户的数据
 * <p>
 * 协处理器的使用
 * 1. 创建类
 * 2. 让表找到协处理类（和表有关联）
 * 3. 将项目打成jar包发布到hbase中（关联的jar包也需要发布），并且需要分发
 */
public class InsertCalleeCoprocessor extends BaseRegionObserver {

    // 方法的命名规则
    // login
    // logout
    // prePut
    // doPut ：模板方法设计模式
    //    存在父子类：
    //    父类搭建算法的骨架
    //    1. tel取用户代码，2时间取年月，3，异或运算，4 hash散列
    //    子类重写算法的细节
    //    do1. tel取后4位，do2，201810， do3 ^, 4, % &
    // postPut

    /**
     * 保存主叫用户数据之后，由Hbase自动保存被叫用户数据
     *
     * @param e
     * @param put
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        // 获取表
        Table table = e.getEnvironment().getTable(TableName.valueOf(Names.TABLE.getValue()));
        // 主叫用户的rowkey
        String rowkey = Bytes.toString(put.getRow());
        // 1_133_2019_144_1010_1
        String[] values = rowkey.split("_");
        CoprocessorDao dao = new CoprocessorDao();
        String call1 = values[1];
        String call2 = values[3];
        String calltime = values[2];
        String duration = values[4];
        String flg = values[5];

        if ("1".equals(flg)) {
            // 只有主叫用户保存后才需要触发被叫用户的保存
            String calleeRowkey = dao.getRegionNum(call2, calltime) + "_" + call2 + "_" + calltime + "_" + call1 + "_" + duration + "_0";
            // 保存数据
            Put calleePut = new Put(Bytes.toBytes(calleeRowkey));
            byte[] calleeFamily = Bytes.toBytes(Names.CF_CALLEE.getValue());
            calleePut.addColumn(calleeFamily, Bytes.toBytes("call1"), Bytes.toBytes(call2));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("call2"), Bytes.toBytes(call1));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("calltime"), Bytes.toBytes(calltime));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("duration"), Bytes.toBytes(duration));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("flg"), Bytes.toBytes("0"));
            table.put(calleePut);
            // 关闭表
            table.close();
        }

    }

    private class CoprocessorDao extends BaseDao {
        public int getRegionNum(String tel, String time) {
            return genRegionNum(tel, time);
        }
    }
}
