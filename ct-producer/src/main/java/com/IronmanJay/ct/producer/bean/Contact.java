package com.IronmanJay.ct.producer.bean;

import com.IronmanJay.ct.common.bean.Data;

// 联系人
public class Contact extends Data {
    private String tel;
    private String name;

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setValue(Object val) {
        content = (String) val;
        String[] values = content.split("\t");
        setName(values[1]);
        setTel(values[0]);
    }

    public String toString() {
        return "Contact[" + tel + "，" + name + "]";
    }

}
