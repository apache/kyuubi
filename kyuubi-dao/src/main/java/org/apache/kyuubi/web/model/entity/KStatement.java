package org.apache.kyuubi.web.model.entity;

import org.apache.kyuubi.common.utils.KyuubiUtils;
import org.apache.kyuubi.web.model.DeleteStatus;

import java.util.Date;

public class KStatement {

    private String statement_id;
    private String session_id;
    private String context_id;
    private String owner;
    private String queue;
    private String code;
    private String status;
    private Long row_count;
    private Date start_time;
    private Date finish_time;
    private String error = null;
    private Long runtime = 0L;
    private Integer delete_status = DeleteStatus.UNDELETED;
    private String server_id = KyuubiUtils.loadServerId();
    private String server_location = KyuubiUtils.loadServerIp();

    public String getStatement_id() {
        return statement_id;
    }

    public void setStatement_id(String statement_id) {
        this.statement_id = statement_id;
    }

    public String getSession_id() {
        return session_id;
    }

    public void setSession_id(String session_id) {
        this.session_id = session_id;
    }

    public String getContext_id() {
        return context_id;
    }

    public void setContext_id(String context_id) {
        this.context_id = context_id;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Long getRow_count() {
        return row_count;
    }

    public void setRow_count(Long row_count) {
        this.row_count = row_count;
    }

    public Date getStart_time() {
        return start_time;
    }

    public void setStart_time(Date start_time) {
        this.start_time = start_time;
    }

    public Date getFinish_time() {
        return finish_time;
    }

    public void setFinish_time(Date finish_time) {
        this.finish_time = finish_time;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public Long getRuntime() {
        return runtime;
    }

    public void setRuntime(Long runtime) {
        this.runtime = runtime;
    }

    public Integer getDelete_status() {
        return delete_status;
    }

    public void setDelete_status(Integer delete_status) {
        this.delete_status = delete_status;
    }

    public String getServer_id() {
        return server_id;
    }

    public void setServer_id(String server_id) {
        this.server_id = server_id;
    }

    public String getServer_location() {
        return server_location;
    }

    public void setServer_location(String server_location) {
        this.server_location = server_location;
    }
}
