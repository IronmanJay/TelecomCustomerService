package com.IronmanJay.ct.web.controller;

import com.IronmanJay.ct.web.bean.Calllog;
import com.IronmanJay.ct.web.service.CalllogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.List;

/**
 * 通话日志控制器对象
 */
@Controller
public class CalllogController {
    @Autowired
    private CalllogService calllogService;

    @RequestMapping("/query")
    public String query() {
        return "query";
    }

    // Object ==> json ==> String
    //@ResponseBody
    @RequestMapping("/view")
    public String view( String tel, String calltime, Model model ) {
        // 查询统计结果 ： Mysql
        List<Calllog> logs = calllogService.queryMonthDatas(tel, calltime);
        System.out.println(logs.size());
        model.addAttribute("calllogs", logs);
        return "view";
    }
}
