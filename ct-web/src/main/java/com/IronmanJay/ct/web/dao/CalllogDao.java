package com.IronmanJay.ct.web.dao;

import com.IronmanJay.ct.web.bean.Calllog;

import java.util.List;
import java.util.Map;

/*
* 通话日志数据访问对象
 */
public interface CalllogDao {
    List<Calllog> queryMonthDatas(Map<String, Object> paramMap);
}
