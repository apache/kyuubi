package org.apache.kyuubi.web.model.entity;

import org.apache.kyuubi.common.utils.KyuubiUtils;
import org.apache.kyuubi.web.model.DeleteStatus;

import java.util.Date;

public class KServer {
    private String server_id = KyuubiUtils.loadServerId();
    private String ip;
    private Integer thrift_port;
    private Integer http_port;
    private Date start_time;
    private Integer delete_status = DeleteStatus.UNDELETED;

    public String getServer_id() {
        return server_id;
    }

    public void setServer_id(String server_id) {
        this.server_id = server_id;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getThrift_port() {
        return thrift_port;
    }

    public void setThrift_port(Integer thrift_port) {
        this.thrift_port = thrift_port;
    }

    public Integer getHttp_port() {
        return http_port;
    }

    public void setHttp_port(Integer http_port) {
        this.http_port = http_port;
    }

    public Date getStart_time() {
        return start_time;
    }

    public void setStart_time(Date start_time) {
        this.start_time = start_time;
    }

    public Integer getDelete_status() {
        return delete_status;
    }

    public void setDelete_status(Integer delete_status) {
        this.delete_status = delete_status;
    }
}
