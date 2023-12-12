/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.jdbc.hive;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TStatus;
import org.apache.kyuubi.util.reflect.DynConstructors;

public class KyuubiSQLException extends SQLException {

  /** Constructor. */
  public KyuubiSQLException() {
    super();
  }

  /**
   * Constructs a SQLException object with a given reason. The SQLState is initialized to null and
   * the vendor code is initialized to 0. The cause is not initialized, and may subsequently be
   * initialized by a call to the Throwable.initCause(java.lang.Throwable) method.
   *
   * @param reason a description of the exception
   */
  public KyuubiSQLException(String reason) {
    super(reason);
  }

  /**
   * Constructs a SQLException object with a given cause. The SQLState is initialized to null and
   * the vendor code is initialized to 0. The reason is initialized to null if cause==null or to
   * cause.toString() if cause!=null.
   *
   * @param cause the underlying reason for this SQLException - may be null indicating the cause is
   *     non-existent or unknown
   */
  public KyuubiSQLException(Throwable cause) {
    super(cause);
  }

  public KyuubiSQLException(String reason, String sqlState) {
    super(reason, sqlState);
  }

  public KyuubiSQLException(String reason, Throwable cause) {
    super(reason, cause);
  }

  public KyuubiSQLException(String reason, String sqlState, int vendorCode) {
    super(reason, sqlState, vendorCode);
  }

  public KyuubiSQLException(String reason, String sqlState, Throwable cause) {
    super(reason, sqlState, cause);
  }

  public KyuubiSQLException(String reason, String sqlState, int vendorCode, Throwable cause) {
    super(reason, sqlState, vendorCode, cause);
  }

  public KyuubiSQLException(TStatus status) {
    // TODO: set correct vendorCode field
    super(status.getErrorMessage(), status.getSqlState(), status.getErrorCode());
    if (status.getInfoMessages() != null) {
      initCause(toCause(status.getInfoMessages()));
    }
  }

  /**
   * Converts a {@link Throwable} object into a flattened list of texts including its stack trace
   * and the stack traces of the nested causes.
   *
   * @param ex a {@link Throwable} object
   * @return a flattened list of texts including the {@link Throwable} object's stack trace and the
   *     stack traces of the nested causes.
   */
  public static List<String> toString(Throwable ex) {
    return toString(ex, null);
  }

  private static List<String> toString(Throwable cause, StackTraceElement[] parent) {
    StackTraceElement[] trace = cause.getStackTrace();
    int m = trace.length - 1;
    if (parent != null) {
      int n = parent.length - 1;
      while (m >= 0 && n >= 0 && trace[m].equals(parent[n])) {
        m--;
        n--;
      }
    }
    List<String> detail = enroll(cause, trace, m);
    cause = cause.getCause();
    if (cause != null) {
      detail.addAll(toString(cause, trace));
    }
    return detail;
  }

  private static List<String> enroll(Throwable ex, StackTraceElement[] trace, int max) {
    List<String> details = new ArrayList<>();
    StringBuilder builder = new StringBuilder();
    builder.append('*').append(ex.getClass().getName()).append(':');
    builder.append(ex.getMessage()).append(':');
    builder.append(trace.length).append(':').append(max);
    details.add(builder.toString());
    for (int i = 0; i <= max; i++) {
      builder.setLength(0);
      builder.append(trace[i].getClassName()).append(':');
      builder.append(trace[i].getMethodName()).append(':');
      String fileName = trace[i].getFileName();
      builder.append(fileName == null ? "" : fileName).append(':');
      builder.append(trace[i].getLineNumber());
      details.add(builder.toString());
    }
    return details;
  }

  /**
   * Converts a flattened list of texts including the stack trace and the stack traces of the nested
   * causes into a {@link Throwable} object.
   *
   * @param details a flattened list of texts including the stack trace and the stack traces of the
   *     nested causes
   * @return a {@link Throwable} object
   */
  public static Throwable toCause(List<String> details) {
    return toStackTrace(details, null, 0);
  }

  private static Throwable toStackTrace(
      List<String> details, StackTraceElement[] parent, int index) {
    String detail = details.get(index++);
    if (!detail.startsWith("*")) {
      return null; // should not be happened. ignore remaining
    }
    int i1 = detail.indexOf(':');
    int i3 = detail.lastIndexOf(':');
    int i2 = detail.substring(0, i3).lastIndexOf(':');
    String exceptionClass = detail.substring(1, i1);
    String exceptionMessage = detail.substring(i1 + 1, i2);
    Throwable ex = newInstance(exceptionClass, exceptionMessage);

    int length = Integer.parseInt(detail.substring(i2 + 1, i3));
    int unique = Integer.parseInt(detail.substring(i3 + 1));

    int i = 0;
    StackTraceElement[] trace = new StackTraceElement[length];
    for (; i <= unique; i++) {
      detail = details.get(index++);
      int j1 = detail.indexOf(':');
      int j3 = detail.lastIndexOf(':');
      int j2 = detail.substring(0, j3).lastIndexOf(':');
      String className = detail.substring(0, j1);
      String methodName = detail.substring(j1 + 1, j2);
      String fileName = detail.substring(j2 + 1, j3);
      if (fileName.isEmpty()) {
        fileName = null;
      }
      int lineNumber = Integer.parseInt(detail.substring(j3 + 1));
      trace[i] = new StackTraceElement(className, methodName, fileName, lineNumber);
    }
    int common = trace.length - i;
    if (common > 0) {
      System.arraycopy(parent, parent.length - common, trace, trace.length - common, common);
    }
    if (details.size() > index) {
      ex.initCause(toStackTrace(details, trace, index));
    }
    ex.setStackTrace(trace);
    return ex;
  }

  private static Throwable newInstance(String className, String message) {
    try {
      return DynConstructors.builder()
          .impl(className, String.class)
          .<Throwable>buildChecked()
          .newInstance(message);
    } catch (Exception e) {
      return new RuntimeException(className + ":" + message);
    }
  }
}
