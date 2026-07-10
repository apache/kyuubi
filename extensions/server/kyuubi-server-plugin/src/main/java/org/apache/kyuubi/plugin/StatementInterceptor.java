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
 * A server-side extension point invoked before an interactive statement is routed to the engine.
 * Implementations can inspect, reject, or rewrite each statement.
 *
 * <p>Each interceptor is instantiated once via its zero-arg constructor and held as a single global
 * instance per Kyuubi server. {@link #beforeExecuteStatement} is called concurrently by many
 * sessions/threads, so implementations MUST be thread-safe and MUST NOT keep per-request mutable
 * state in instance fields. Resources prepared in {@link #initialize} should be shared read-only.
 */
public interface StatementInterceptor {

  /**
   * Called once when the Kyuubi server starts. The passed map is an immutable read-only snapshot of
   * the full server configuration taken at startup; it does not reflect later changes and must not
   * be modified. Implementations read their own private keys from it.
   */
  default void initialize(Map<String, String> conf) {}

  /** Invoked for each statement before operation creation and engine routing. */
  StatementInterceptResult beforeExecuteStatement(StatementInterceptContext context);

  /**
   * Called at most once after initialization is attempted, either when server startup fails or when
   * the server stops. Implementations must also release resources allocated before {@link
   * #initialize} throws.
   */
  default void close() {}
}
