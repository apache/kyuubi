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

import java.util.Map;

/**
 * Stable, gateway-level context passed to a {@link StatementInterceptor}. All fields are JDK types.
 * It intentionally does not expose the session configuration (which carries connection parameters
 * and engine credentials) nor any mutable internal session object.
 */
public interface StatementInterceptContext {

  /** The session identifier the statement belongs to. */
  String sessionId();

  /** The user submitting the statement. */
  String user();

  /** The client IP address; an empty string when unknown, never {@code null}. */
  String ipAddress();

  /** The statement to be executed. With a chain of interceptors, this is the current statement. */
  String statement();

  /** The per-statement configuration overlay (statement-level, read-only). */
  Map<String, String> confOverlay();

  /** Whether the statement is executed asynchronously. */
  boolean runAsync();

  /** The client-requested query timeout in seconds; {@code 0} means no timeout. */
  long queryTimeout();

  /** The engine type (spark / flink / trino / ...) resolved from kyuubi.engine.type. */
  String engineType();
}
