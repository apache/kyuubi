package org.apache.kyuubi.web.model;

public enum StatementState {
    waiting, running, available, closed, error, cancelling, cancelled;

    public static boolean isSuccess(String state) {
        StatementState statementState = valueOf(state);
        return statementState == closed;
    }

    public static boolean isFailure(String state) {
        StatementState statementState = valueOf(state);
        return statementState == error;
    }

    public static boolean isFinished(String state) {
        StatementState statementState = valueOf(state);
        return statementState == error || statementState == cancelled || statementState == closed;
    }
}
