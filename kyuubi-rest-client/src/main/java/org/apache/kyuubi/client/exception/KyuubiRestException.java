package org.apache.kyuubi.client.exception;

public class KyuubiRestException extends Exception {

  public KyuubiRestException(String message, Throwable cause) {
    super(message, cause);
  }
}
