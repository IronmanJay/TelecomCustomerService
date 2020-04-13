package com.IronmanJay.ct.web.service;

import com.IronmanJay.ct.web.bean.Calllog;

import java.util.List;

public interface CalllogService {
    List<Calllog> queryMonthDatas(String tel, String calltime);
}
