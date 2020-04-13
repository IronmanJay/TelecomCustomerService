package com.IronmanJay.ct.producer.io;

import com.IronmanJay.ct.common.bean.DataOut;

import java.io.*;

/**
 * 本地文件数据输出
 */
public class LocalFileDataOut implements DataOut {

    private PrintWriter writer = null;

    public LocalFileDataOut(String path) {
        setPath(path);
    }

    public void setPath(String path) {
        try {
            writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void write(Object data) throws Exception {
        write(data.toString());
    }

    /**
     * 将数据字符串生成到文件中
     *
     * @param data
     * @throws Exception
     */
    public void write(String data) throws Exception {
        writer.println(data);
        writer.flush();
    }

    /**
     * 释放资源
     *
     * @throws IOException
     */
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }
}
