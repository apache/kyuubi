package org.apache.kyuubi.web.utils;

public class KyuubiWebException extends RuntimeException {
    public KyuubiWebException() {
    }

    public KyuubiWebException(String message) {
        super(message);
    }

    public KyuubiWebException(String message, Throwable cause) {
        super(message, cause);
    }

    public KyuubiWebException(Throwable cause) {
        super(cause);
    }

    public KyuubiWebException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
