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

package org.apache.kyuubi.plugin;

/**
 * The decision returned by a {@link StatementInterceptor} for a single statement: proceed with the
 * current statement, rewrite it for the next interceptor and the engine, or reject it.
 */
public final class StatementInterceptResult {

  public enum Action {
    PROCEED,
    REWRITE,
    REJECT
  }

  private final Action action;
  private final String statement; // the rewritten statement when action is REWRITE
  private final String message; // the rejection reason when action is REJECT

  private StatementInterceptResult(Action action, String statement, String message) {
    this.action = action;
    this.statement = statement;
    this.message = message;
  }

  /** Keep the current statement and pass it to the next interceptor. */
  public static StatementInterceptResult proceed() {
    return new StatementInterceptResult(Action.PROCEED, null, null);
  }

  /**
   * Replace the current statement with {@code statement}. It is passed to the next interceptor and
   * ultimately to the engine. {@code statement} must not be null or blank.
   */
  public static StatementInterceptResult rewrite(String statement) {
    return new StatementInterceptResult(
        Action.REWRITE, requireNonBlank(statement, "statement"), null);
  }

  /**
   * Reject the statement immediately; the chain stops and the client receives an error. {@code
   * message} must not be null or blank.
   */
  public static StatementInterceptResult reject(String message) {
    return new StatementInterceptResult(Action.REJECT, null, requireNonBlank(message, "message"));
  }

  public Action action() {
    return action;
  }

  public String statement() {
    return statement;
  }

  public String message() {
    return message;
  }

  private static String requireNonBlank(String value, String name) {
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException(name + " must not be null or blank");
    }
    return value;
  }
}
