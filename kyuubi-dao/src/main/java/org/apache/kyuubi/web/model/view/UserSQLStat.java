package org.apache.kyuubi.web.model.view;

public class UserSQLStat {

    private String owner;
    private long total = 0;
    private long success = 0;
    private long failure = 0;

    public UserSQLStat() {
    }

    public UserSQLStat(String owner) {
        this.owner = owner;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public long getSuccess() {
        return success;
    }

    public void setSuccess(long success) {
        this.success = success;
    }

    public long getFailure() {
        return failure;
    }

    public void setFailure(long failure) {
        this.failure = failure;
    }

    public void incSuccess(long count) {
        this.success += count;
    }

    public void incFailure(long count) {
        this.failure += count;
    }

    public void incTotal(long count) {
        this.total += count;
    }
}
