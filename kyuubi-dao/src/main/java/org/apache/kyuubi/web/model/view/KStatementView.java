package org.apache.kyuubi.web.model.view;
import org.apache.kyuubi.common.utils.KyuubiUtils;
import org.apache.kyuubi.web.model.entity.KStatement;

import java.util.Optional;

public class KStatementView {
    private String id;
    private String sessionId;
    private String contextId;
    private String owner;
    private String queue;
    private String code;
    private String state;

    private Long row_count;
    private String exception;
    private String start_time;
    private String finish_time;
    private String runtime;
    private String server_id;
    private Boolean permit_cancel = false;

    public KStatementView() {
    }

    public KStatementView(KStatement statement) {
        this.id = statement.getStatement_id();
        this.sessionId = statement.getSession_id();
        this.contextId = statement.getContext_id();
        this.owner = statement.getOwner();
        this.queue = statement.getQueue();
        this.code = Optional.ofNullable(statement.getCode()).orElse("");
        this.state = statement.getStatus();
        this.row_count = statement.getRow_count();
        this.exception = statement.getError();
        this.start_time = KyuubiUtils.dateToString(statement.getStart_time());
        this.finish_time = KyuubiUtils.dateToString(statement.getFinish_time());
        this.runtime = KyuubiUtils.durationToString(statement.getStart_time(), statement.getFinish_time());
        this.server_id = statement.getServer_id();
        this.permit_cancel = false;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getContextId() {
        return contextId;
    }

    public void setContextId(String contextId) {
        this.contextId = contextId;
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

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Long getRow_count() {
        return row_count;
    }

    public void setRow_count(Long row_count) {
        this.row_count = row_count;
    }

    public String getException() {
        return exception;
    }

    public void setException(String exception) {
        this.exception = exception;
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

    public String getRuntime() {
        return runtime;
    }

    public void setRuntime(String runtime) {
        this.runtime = runtime;
    }

    public String getServer_id() {
        return server_id;
    }

    public void setServer_id(String server_id) {
        this.server_id = server_id;
    }

    public Boolean getPermit_cancel() {
        return permit_cancel;
    }

    public void setPermit_cancel(Boolean permit_cancel) {
        this.permit_cancel = permit_cancel;
    }
}
