package org.apache.kyuubi.web.model;

public class KResult {

    private String msg;
    private Integer code;

    public KResult(String msg, Integer code) {
        this.msg = msg;
        this.code = code;
    }

    public static KResult success(String msg) {
        return new KResult(msg, 0);
    }

    public static KResult failure(String msg) {
        return new KResult(msg, 1);
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }
}
