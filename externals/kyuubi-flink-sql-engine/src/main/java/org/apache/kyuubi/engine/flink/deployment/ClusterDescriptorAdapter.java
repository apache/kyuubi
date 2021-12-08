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
import org.apache.kyuubi.engine.flink.context.ExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adapter to handle kinds of job actions (eg. get job status or cancel job) based on
 * execution.target
 */
public abstract class ClusterDescriptorAdapter<ClusterID> {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterDescriptorAdapter.class);

  protected final ExecutionContext<ClusterID> executionContext;
  // Only used for logging
  private final String sessionId;
  // jobId is not null only after job is submitted
  protected final JobID jobId;
  protected final Configuration configuration;
  protected final ClusterID clusterID;

  public ClusterDescriptorAdapter(
      ExecutionContext<ClusterID> executionContext,
      Configuration configuration,
      String sessionId,
      JobID jobId) {
    this.executionContext = executionContext;
    this.sessionId = sessionId;
    this.jobId = jobId;
    this.configuration = configuration;
    this.clusterID = executionContext.getClusterClientFactory().getClusterId(configuration);
  }

  @Override
  public String toString() {
    return "ClusterDescriptorAdapter{"
        + "sessionId='"
        + sessionId
        + '\''
        + ", jobId="
        + jobId
        + ", configuration="
        + configuration
        + ", clusterID="
        + clusterID
        + '}';
  }
}
