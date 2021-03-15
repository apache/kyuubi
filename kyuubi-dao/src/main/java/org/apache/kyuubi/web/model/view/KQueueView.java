package org.apache.kyuubi.web.model.view;

public class KQueueView {
    private String username;
    private String queue;
    private String used;
    private String available;

    public KQueueView() {
    }

    public KQueueView(String username, String queue) {
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

    public String getUsed() {
        return used;
    }

    public void setUsed(String used) {
        this.used = used;
    }

    public String getAvailable() {
        return available;
    }

    public void setAvailable(String available) {
        this.available = available;
    }
}
