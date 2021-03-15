package org.apache.kyuubi.web.model;

public enum KState {
    not_started, starting, started, shutting_down, error, killed, stopped;

    public static boolean isFinished(String status) {
        KState kState = valueOf(status);
        return kState == error || kState == killed || kState == stopped;

    }
}
