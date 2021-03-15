package org.apache.kyuubi.web.model.entity;

import org.apache.kyuubi.common.utils.KyuubiUtils;
import org.apache.kyuubi.web.model.DeleteStatus;

import java.util.Date;

public class KMetric {

    private String server_id = KyuubiUtils.loadServerId();
    private String name;
    private Double metric_value;
    private Date update_time;
    private Integer delete_status = DeleteStatus.UNDELETED;

    public KMetric() {
    }

    public KMetric(String server_id, String name, Double metric_value, Date update_time) {
        this.server_id = server_id;
        this.name = name;
        this.metric_value = metric_value;
        this.update_time = update_time;
    }

    public String getServer_id() {
        return server_id;
    }

    public void setServer_id(String server_id) {
        this.server_id = server_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getMetric_value() {
        return metric_value;
    }

    public void setMetric_value(Double metric_value) {
        this.metric_value = metric_value;
    }

    public Date getUpdate_time() {
        return update_time;
    }

    public void setUpdate_time(Date update_time) {
        this.update_time = update_time;
    }

    public Integer getDelete_status() {
        return delete_status;
    }

    public void setDelete_status(Integer delete_status) {
        this.delete_status = delete_status;
    }
}
