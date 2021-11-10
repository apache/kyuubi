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

package org.apache.kyuubi.engine.flink.deployment;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.yarn.executors.YarnJobClusterExecutor;
import org.apache.kyuubi.engine.flink.context.ExecutionContext;

/** The factory to create {@link ClusterDescriptorAdapter} based on execution.target. */
public class ClusterDescriptorAdapterFactory {

  public static <ClusterID> ClusterDescriptorAdapter<ClusterID> create(
      ExecutionContext<ClusterID> executionContext,
      Configuration configuration,
      String sessionId,
      JobID jobId) {
    String executionTarget = executionContext.getFlinkConfig().getString(DeploymentOptions.TARGET);
    if (executionTarget == null) {
      throw new RuntimeException("No execution.target specified in your configuration file.");
    }

    ClusterDescriptorAdapter<ClusterID> clusterDescriptorAdapter;

    if (YarnJobClusterExecutor.NAME.equals(executionTarget)) {
      clusterDescriptorAdapter =
          new YarnPerJobClusterDescriptorAdapter<>(
              executionContext, configuration, sessionId, jobId);
    } else {
      clusterDescriptorAdapter =
          new SessionClusterDescriptorAdapter<>(executionContext, configuration, sessionId, jobId);
    }

    return clusterDescriptorAdapter;
  }
}
