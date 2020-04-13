package com.IronmanJay.ct.web.bean;

public class Calllog {
    private Integer id;
    private Integer telid;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getTelid() {
        return telid;
    }

    public void setTelid(Integer telid) {
        this.telid = telid;
    }

    public Integer getDateid() {
        return dateid;
    }

    public void setDateid(Integer dateid) {
        this.dateid = dateid;
    }

    public Integer getSumcall() {
        return sumcall;
    }

    public void setSumcall(Integer sumcall) {
        this.sumcall = sumcall;
    }

    public Integer getSumduration() {
        return sumduration;
    }

    public void setSumduration(Integer sumduration) {
        this.sumduration = sumduration;
    }

    private Integer dateid;
    private Integer sumcall;
    private Integer sumduration;
}
