package com.IronmanJay.ct.common.constant;

import com.IronmanJay.ct.common.bean.Val;

// 名称常量枚举类
public enum Names implements Val {
    NAMESPACE("ct"),
    TOPIC("ct"),
    TABLE("ct:calllog"),
    CF_CALLER("caller"),
    CF_INFO("info"),
    CF_CALLEE("callee");

    private String name;

    private Names(String name) {
        this.name = name;
    }


    public void setValue(Object val) {
        this.name = (String) val;
    }

    public String getValue() {
        return name;
    }
}
