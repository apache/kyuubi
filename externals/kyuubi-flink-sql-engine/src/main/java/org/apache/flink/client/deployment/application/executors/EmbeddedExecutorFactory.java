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

package org.apache.flink.client.deployment.application.executors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.cli.ClientOptions;
import org.apache.flink.client.deployment.application.EmbeddedJobClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Copied from Apache Flink to exposed the DispatcherGateway for Kyuubi statements. */
@Internal
public class EmbeddedExecutorFactory implements PipelineExecutorFactory {

  private static Collection<JobID> bootstrapJobIds;

  private static Collection<JobID> submittedJobIds;

  private static DispatcherGateway dispatcherGateway;

  private static ScheduledExecutor retryExecutor;

  private static final Object bootstrapLock = new Object();

  private static final long BOOTSTRAP_WAIT_INTERVAL = 10_000L;

  private static final int BOOTSTRAP_WAIT_RETRIES = 3;

  private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedExecutorFactory.class);

  public EmbeddedExecutorFactory() {
    LOGGER.debug(
        "{} loaded in thread {} with classloader {}.",
        this.getClass().getCanonicalName(),
        Thread.currentThread().getName(),
        this.getClass().getClassLoader().toString());
  }

  /**
   * Creates an {@link EmbeddedExecutorFactory}.
   *
   * @param submittedJobIds a list that is going to be filled with the job ids of the new jobs that
   *     will be submitted. This is essentially used to return the submitted job ids to the caller.
   * @param dispatcherGateway the dispatcher of the cluster which is going to be used to submit
   *     jobs.
   */
  public EmbeddedExecutorFactory(
      final Collection<JobID> submittedJobIds,
      final DispatcherGateway dispatcherGateway,
      final ScheduledExecutor retryExecutor) {
    // there should be only one instance of EmbeddedExecutorFactory
    LOGGER.debug(
        "{} initiated in thread {} with classloader {}.",
        this.getClass().getCanonicalName(),
        Thread.currentThread().getName(),
        this.getClass().getClassLoader().toString());
    checkState(EmbeddedExecutorFactory.submittedJobIds == null);
    checkState(EmbeddedExecutorFactory.dispatcherGateway == null);
    checkState(EmbeddedExecutorFactory.retryExecutor == null);
    synchronized (bootstrapLock) {
      // submittedJobIds would be always 1, because we create a new list to avoid concurrent access
      // issues
      LOGGER.debug("Bootstrapping EmbeddedExecutorFactory.");
      EmbeddedExecutorFactory.submittedJobIds =
          new ConcurrentLinkedQueue<>(checkNotNull(submittedJobIds));
      EmbeddedExecutorFactory.bootstrapJobIds = submittedJobIds;
      EmbeddedExecutorFactory.dispatcherGateway = checkNotNull(dispatcherGateway);
      EmbeddedExecutorFactory.retryExecutor = checkNotNull(retryExecutor);
      bootstrapLock.notifyAll();
    }
  }

  @Override
  public String getName() {
    return EmbeddedExecutor.NAME;
  }

  @Override
  public boolean isCompatibleWith(final Configuration configuration) {
    // override Flink's implementation to allow usage in Kyuubi
    LOGGER.debug("Matching execution target: {}", configuration.get(DeploymentOptions.TARGET));
    return configuration.get(DeploymentOptions.TARGET).equalsIgnoreCase("yarn-application")
        && configuration.toMap().getOrDefault("yarn.tags", "").toLowerCase().contains("kyuubi");
  }

  @Override
  public PipelineExecutor getExecutor(final Configuration configuration) {
    checkNotNull(configuration);
    Collection<JobID> executorJobIDs;
    synchronized (bootstrapLock) {
      // wait in a loop to avoid spurious wakeups
      int retry = 0;
      while (bootstrapJobIds == null && retry < BOOTSTRAP_WAIT_RETRIES) {
        try {
          LOGGER.debug("Waiting for bootstrap to complete. Wait retries: {}.", retry);
          bootstrapLock.wait(BOOTSTRAP_WAIT_INTERVAL);
          retry++;
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted while waiting for bootstrap.", e);
        }
      }
      if (bootstrapJobIds == null) {
        throw new RuntimeException(
            "Bootstrap of Flink SQL engine timed out after "
                + BOOTSTRAP_WAIT_INTERVAL * BOOTSTRAP_WAIT_RETRIES
                + " ms. Please check the engine log for more details.");
      }
    }
    if (bootstrapJobIds.size() > 0) {
      LOGGER.info("Submitting new Kyuubi job. Job submitted: {}.", submittedJobIds.size());
      executorJobIDs = submittedJobIds;
    } else {
      LOGGER.info("Bootstrapping Flink SQL engine with the initial SQL.");
      executorJobIDs = bootstrapJobIds;
    }
    return new EmbeddedExecutor(
        executorJobIDs,
        dispatcherGateway,
        (jobId, userCodeClassloader) -> {
          final Time timeout =
              Time.milliseconds(configuration.get(ClientOptions.CLIENT_TIMEOUT).toMillis());
          return new EmbeddedJobClient(
              jobId, dispatcherGateway, retryExecutor, timeout, userCodeClassloader);
        });
  }
}
