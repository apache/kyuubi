package org.apache.kyuubi.web.model.view;

import org.apache.kyuubi.common.utils.KyuubiUtils;
import org.apache.kyuubi.web.model.entity.KContext;

public class KContextView {
private   String id;
private   String name;
private   String serverLocation;
private   String owner;
private   String state;
private   String queue;
private   Integer executors;
private   Integer executor_vcores;
private   String executor_memory;
private   String start_time;
private   String finish_time;
private   String server_id;

    public KContextView() {
    }

    public KContextView(KContext context) {
        this.id = context.getContext_id();
        this.name = context.getName();
        this.serverLocation = context.getServer_location();
        this.owner = context.getOwner();
        this.state = context.getStatus();
        this.queue = context.getQueue();
        this.executors = context.getExecutors();
        this.executor_vcores = context.getExecutor_vcores();
        this.executor_memory = context.getExecutor_memory();
        this.start_time = KyuubiUtils.dateToString(context.getStart_time());
        this.finish_time = KyuubiUtils.dateToString(context.getFinish_time());
        this.server_id = context.getServer_id();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getServerLocation() {
        return serverLocation;
    }

    public void setServerLocation(String serverLocation) {
        this.serverLocation = serverLocation;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public Integer getExecutors() {
        return executors;
    }

    public void setExecutors(Integer executors) {
        this.executors = executors;
    }

    public Integer getExecutor_vcores() {
        return executor_vcores;
    }

    public void setExecutor_vcores(Integer executor_vcores) {
        this.executor_vcores = executor_vcores;
    }

    public String getExecutor_memory() {
        return executor_memory;
    }

    public void setExecutor_memory(String executor_memory) {
        this.executor_memory = executor_memory;
    }

    public String getStart_time() {
        return start_time;
    }

    public void setStart_time(String start_time) {
        this.start_time = start_time;
    }

    public String getFinish_time() {
        return finish_time;
    }

    public void setFinish_time(String finish_time) {
        this.finish_time = finish_time;
    }

    public String getServer_id() {
        return server_id;
    }

    public void setServer_id(String server_id) {
        this.server_id = server_id;
    }
}
