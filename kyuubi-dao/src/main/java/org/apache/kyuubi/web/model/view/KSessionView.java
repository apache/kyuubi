package org.apache.kyuubi.web.model.view;

import org.apache.kyuubi.common.utils.KyuubiUtils;
import org.apache.kyuubi.web.model.entity.KSession;

public class KSessionView {

    private String id;
    private String owner;
    private String contextId;
    private String state;
    private String start_time;
    private String finish_time;
    private String server_id;
    private String serverLocation;

    private Long running_sql = 0L;
    private Long error_sql = 0L;
    private Long total_sql = 0L;


    public KSessionView() {
    }

    public KSessionView(KSession session) {
        this.id = session.getSession_id();
        this.owner = session.getOwner();
        this.contextId = session.getContext_id();
        this.state =  session.getStatus();
        this.start_time = KyuubiUtils.dateToString(session.getStart_time());
        this.finish_time = KyuubiUtils.dateToString(session.getFinish_time());
        this.server_id = session.getServer_id();
        this.serverLocation = session.getServer_location();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getContextId() {
        return contextId;
    }

    public void setContextId(String contextId) {
        this.contextId = contextId;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
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

    public String getServerLocation() {
        return serverLocation;
    }

    public void setServerLocation(String serverLocation) {
        this.serverLocation = serverLocation;
    }

    public Long getRunning_sql() {
        return running_sql;
    }

    public void setRunning_sql(Long running_sql) {
        this.running_sql = running_sql;
    }

    public Long getError_sql() {
        return error_sql;
    }

    public void setError_sql(Long error_sql) {
        this.error_sql = error_sql;
    }

    public Long getTotal_sql() {
        return total_sql;
    }

    public void setTotal_sql(Long total_sql) {
        this.total_sql = total_sql;
    }

    public void incRunningSql(long num) {
        this.running_sql += num;
    }

    public void incTotalSql(long num) {
        this.total_sql += num;
    }

    public void incErrorSql(long num) {
        this.error_sql += num;
    }
}
