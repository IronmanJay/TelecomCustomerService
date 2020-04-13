package com.IronmanJay.ct.analysis.kv;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义分析数据key
 */
public class AnalysisKey implements WritableComparable<AnalysisKey> {

    public AnalysisKey() {

    }

    public AnalysisKey(String tel, String date) {
        this.tel = tel;
        this.date = date;
    }

    private String tel;
    private String date;

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    /**
     * 比较:tel, date
     *
     * @param key
     * @return
     */
    public int compareTo(AnalysisKey key) {
        int result = tel.compareTo(key.getTel());
        if (result == 0) {
            result = date.compareTo(key.getDate());
        }
        return result;
    }

    /**
     * 写数据（序列化）
     *
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeUTF(tel);
        out.writeUTF(date);
    }

    /**
     * 读数据（反序列化）
     *
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        tel = in.readUTF();
        date = in.readUTF();
    }

}
