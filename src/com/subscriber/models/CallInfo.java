package com.subscriber.models;

public class CallInfo {
    private String tn;
    private String ip;
    private int id;
    private String fn;

    public void setTn(String tn) {
        this.tn = tn;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setFn(String fn) {
        this.fn = fn;
    }

    public String getTn() {
        return tn;
    }

    public String getIp() {
        return ip;
    }

    public int getId() {
        return id;
    }

    public String getFn() {
        return fn;
    }
}
