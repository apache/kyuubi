/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.engine.flink.context;

import java.util.Objects;
import org.apache.kyuubi.engine.flink.config.EngineEnvironment;

/** Context describing current session properties, original properties, ExecutionContext, etc. */
public class SessionContext {
  private final EngineEnvironment originalSessionEnv;
  private final EngineContext engineContext;
  private ExecutionContext<?> executionContext;

  public SessionContext(EngineEnvironment originalSessionEnv, EngineContext engineContext) {
    this.originalSessionEnv = originalSessionEnv;
    this.engineContext = engineContext;
    this.executionContext = createExecutionContextBuilder(originalSessionEnv).build();
  }

  public EngineEnvironment getOriginalSessionEnv() {
    return this.originalSessionEnv;
  }

  public ExecutionContext<?> getExecutionContext() {
    return executionContext;
  }

  public void setExecutionContext(ExecutionContext<?> executionContext) {
    this.executionContext = executionContext;
  }

  /** Returns ExecutionContext.Builder with given {@link SessionContext} session context. */
  public ExecutionContext.Builder createExecutionContextBuilder(EngineEnvironment sessionEnv) {
    return ExecutionContext.builder(
        engineContext.getEngineEnv(),
        sessionEnv,
        engineContext.getDependencies(),
        engineContext.getFlinkConfig(),
        engineContext.getClusterClientServiceLoader(),
        engineContext.getCommandLineOptions(),
        engineContext.getCommandLines());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SessionContext)) {
      return false;
    }
    SessionContext context = (SessionContext) o;
    return Objects.equals(originalSessionEnv, context.originalSessionEnv)
        && Objects.equals(executionContext, context.executionContext);
  }

  @Override
  public int hashCode() {
    return Objects.hash(originalSessionEnv, executionContext);
  }
}
