package org.apache.kyuubi.monitor.entity;

public class KStatement {

    private String sessionId;
    private String operationId;
    private String statement;
    private Long startTime;
    private String state;
    private Long stateTime;

    public KStatement(String sessionId, String operationId, String statement, Long startTime, String state) {
        this.sessionId = sessionId;
        this.operationId = operationId;
        this.statement = statement;
        this.startTime = startTime;
        this.state = state;
    }

    public KStatement() {
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getOperationId() {
        return operationId;
    }

    public void setOperationId(String operationId) {
        this.operationId = operationId;
    }

    public String getStatement() {
        return statement;
    }

    public void setStatement(String statement) {
        this.statement = statement;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Long getStateTime() {
        return stateTime;
    }

    public void setStateTime(Long stateTime) {
        this.stateTime = stateTime;
    }
}
