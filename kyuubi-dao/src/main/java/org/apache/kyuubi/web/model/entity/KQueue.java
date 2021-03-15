package org.apache.kyuubi.web.model.entity;

import org.apache.kyuubi.web.model.DeleteStatus;

public class KQueue {
    private String username;
    private String queue;
    private Integer delete_status = DeleteStatus.UNDELETED;


    public KQueue() {
    }

    public KQueue(String username, String queue) {
        this.username = username;
        this.queue = queue;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public Integer getDelete_status() {
        return delete_status;
    }

    public void setDelete_status(Integer delete_status) {
        this.delete_status = delete_status;
    }
}
