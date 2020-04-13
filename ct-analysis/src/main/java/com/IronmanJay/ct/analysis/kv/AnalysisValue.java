package com.IronmanJay.ct.analysis.kv;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义分析数据value
 */
public class AnalysisValue implements Writable {

    public AnalysisValue() {

    }

    public AnalysisValue(String sumCall, String sumDuration) {
        this.sumCall = sumCall;
        this.sumDuration = sumDuration;
    }

    private String sumCall;
    private String sumDuration;

    public String getSumCall() {
        return sumCall;
    }

    public void setSumCall(String sumCall) {
        this.sumCall = sumCall;
    }

    public String getSumDuration() {
        return sumDuration;
    }

    public void setSumDuration(String sumDuration) {
        this.sumDuration = sumDuration;
    }

    /**
     * 写数据（序列化）
     *
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeUTF(sumCall);
        out.writeUTF(sumDuration);
    }

    /**
     * 读数据（反序列化）
     *
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        sumCall = in.readUTF();
        sumDuration = in.readUTF();
    }
}
