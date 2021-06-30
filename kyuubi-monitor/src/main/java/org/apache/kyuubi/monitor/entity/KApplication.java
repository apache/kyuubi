package org.apache.kyuubi.monitor.entity;

public class KApplication {

    private String appId;
    private String appAttemptId;
    private String appName;
    private String sparkUser;

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getAppAttemptId() {
        return appAttemptId;
    }

    public void setAppAttemptId(String appAttemptId) {
        this.appAttemptId = appAttemptId;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getSparkUser() {
        return sparkUser;
    }

    public void setSparkUser(String sparkUser) {
        this.sparkUser = sparkUser;
    }
}
