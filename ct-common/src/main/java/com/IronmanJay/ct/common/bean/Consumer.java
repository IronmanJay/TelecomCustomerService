package com.IronmanJay.ct.common.bean;

import java.io.Closeable;

/**
 * 消费者接口
 */
public interface Consumer extends Closeable {
    // 消费数据
    public void consume();
}
