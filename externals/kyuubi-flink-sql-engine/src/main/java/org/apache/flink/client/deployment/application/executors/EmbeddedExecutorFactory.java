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

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.cli.ClientOptions;
import org.apache.flink.client.deployment.application.EmbeddedJobClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.kyuubi.util.reflect.DynClasses;
import org.apache.kyuubi.util.reflect.DynConstructors;
import org.apache.kyuubi.util.reflect.DynMethods;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copied from Apache Flink to expose the DispatcherGateway for Kyuubi statements and stamp the
 * application id on submitted StreamGraphs, which is required since FLINK-38974 (2.3.0).
 */
@Internal
public class EmbeddedExecutorFactory implements PipelineExecutorFactory {

  /** FLINK-14068 (2.0.0) removed {@code Time} in favor of {@link Duration}. */
  private static final Class<?> LEGACY_TIME_CLASS =
      DynClasses.builder().impl("org.apache.flink.api.common.time.Time").orNull().build();

  /** FLINK-38974 (2.3.0) introduced {@code ApplicationID}, absent in older Flink versions. */
  private static final Class<?> APPLICATION_ID_CLASS =
      DynClasses.builder().impl("org.apache.flink.api.common.ApplicationID").orNull().build();

  private static final boolean IS_FLINK_1 = LEGACY_TIME_CLASS != null;

  /**
   * EmbeddedExecutor's constructor changed twice, FLINK-33212 (2.0.0) added the {@link
   * Configuration} parameter, FLINK-38974 (2.3.0) split the job ids into the application, suspended
   * and terminal ones. Bind all variants reflectively so that a single engine jar runs on all
   * supported Flink versions.
   */
  private static final DynConstructors.Ctor<EmbeddedExecutor> EMBEDDED_EXECUTOR_CTOR =
      DynConstructors.builder()
          .impl(
              EmbeddedExecutor.class,
              Collection.class,
              Collection.class,
              Collection.class,
              DispatcherGateway.class,
              Configuration.class,
              EmbeddedJobClientCreator.class)
          .impl(
              EmbeddedExecutor.class,
              Collection.class,
              DispatcherGateway.class,
              Configuration.class,
              EmbeddedJobClientCreator.class)
          .impl(
              EmbeddedExecutor.class,
              Collection.class,
              DispatcherGateway.class,
              EmbeddedJobClientCreator.class)
          .build();

  /** FLINK-14068 (2.0.0) replaced the {@code Time} timeout parameter with {@link Duration}. */
  private static final DynConstructors.Ctor<EmbeddedJobClient> EMBEDDED_JOB_CLIENT_CTOR =
      DynConstructors.builder()
          .impl(
              EmbeddedJobClient.class,
              JobID.class,
              DispatcherGateway.class,
              ScheduledExecutor.class,
              Duration.class,
              ClassLoader.class)
          .impl(
              EmbeddedJobClient.class,
              JobID.class,
              DispatcherGateway.class,
              ScheduledExecutor.class,
              LEGACY_TIME_CLASS,
              ClassLoader.class)
          .build();

  private static final DynMethods.StaticMethod LEGACY_TIME_OF_MILLIS =
      IS_FLINK_1
          ? DynMethods.builder("milliseconds").impl(LEGACY_TIME_CLASS, long.class).buildStatic()
          : null;

  private static Collection<JobID> bootstrapJobIds;

  private static boolean bootstrapJobIdsClaimed;

  private static Collection<JobID> submittedJobIds;

  /**
   * Both are null before FLINK-38974 (2.3.0), which is also how the Flink version is told apart.
   */
  private static Collection<JobID> suspendedJobIds;

  private static Collection<JobID> terminalJobIds;

  /**
   * FLINK-38974 (2.3.0): the dispatcher rejects jobs that carry no application registered in it.
   * Kyuubi statements run through the SQL gateway, which builds plain StreamExecutionEnvironments,
   * so capture the id from the first StreamGraph that carries it and stamp it on the rest.
   */
  private static volatile Object applicationId;

  private static volatile DynMethods.UnboundMethod streamGraphGetApplicationId;

  private static volatile DynMethods.UnboundMethod streamGraphSetApplicationId;

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
   * Creates an {@link EmbeddedExecutorFactory}, invoked by Flink before FLINK-38974 (2.3.0).
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
    this(submittedJobIds, null, null, dispatcherGateway, retryExecutor);
  }

  /**
   * Creates an {@link EmbeddedExecutorFactory}, invoked by Flink since FLINK-38974 (2.3.0). Flink
   * resolves this constructor at compile time, so it has to exist even though Kyuubi never calls it
   * directly.
   *
   * @param applicationJobIds a list that is going to be filled with the job ids of the new jobs
   *     that will be submitted. This is essentially used to return the submitted job ids to the
   *     caller.
   * @param suspendedJobIds ids of jobs suspended by a previous application execution.
   * @param terminalJobIds ids of jobs already terminated by a previous application execution.
   * @param dispatcherGateway the dispatcher of the cluster which is going to be used to submit
   *     jobs.
   */
  public EmbeddedExecutorFactory(
      final Collection<JobID> applicationJobIds,
      final Collection<JobID> suspendedJobIds,
      final Collection<JobID> terminalJobIds,
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
      // Keep Flink's collection for the application bootstrap job. Later Kyuubi jobs use the
      // thread-safe copy to avoid concurrent access to Flink's ArrayList.
      LOGGER.debug("Bootstrapping EmbeddedExecutorFactory.");
      EmbeddedExecutorFactory.submittedJobIds =
          new ConcurrentLinkedQueue<>(checkNotNull(applicationJobIds));
      EmbeddedExecutorFactory.bootstrapJobIds = applicationJobIds;
      EmbeddedExecutorFactory.bootstrapJobIdsClaimed = !applicationJobIds.isEmpty();
      EmbeddedExecutorFactory.suspendedJobIds = suspendedJobIds;
      EmbeddedExecutorFactory.terminalJobIds = terminalJobIds;
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
    final Collection<JobID> executorJobIDs = claimJobIdsForExecutor();
    final EmbeddedJobClientCreator jobClientCreator =
        (jobId, userCodeClassloader) ->
            newEmbeddedJobClient(
                jobId, configuration.get(ClientOptions.CLIENT_TIMEOUT), userCodeClassloader);
    return stampApplicationId(newEmbeddedExecutor(executorJobIDs, configuration, jobClientCreator));
  }

  @VisibleForTesting
  static Collection<JobID> claimJobIdsForExecutor() {
    if (bootstrapJobIdsClaimed) {
      LOGGER.info("Submitting new Kyuubi job. Job submitted: {}.", submittedJobIds.size());
      return submittedJobIds;
    }
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
      if (!bootstrapJobIdsClaimed) {
        // Flink owns this collection and expects the application bootstrap job in it. Claim it
        // before returning the executor so another submission cannot observe the list as empty and
        // concurrently add to Flink's non-thread-safe ArrayList.
        bootstrapJobIdsClaimed = true;
        LOGGER.info("Bootstrapping Flink SQL engine with the initial SQL.");
        return bootstrapJobIds;
      }
      LOGGER.info("Submitting new Kyuubi job. Job submitted: {}.", submittedJobIds.size());
      return submittedJobIds;
    }
  }

  private static PipelineExecutor newEmbeddedExecutor(
      final Collection<JobID> jobIds,
      final Configuration configuration,
      final EmbeddedJobClientCreator jobClientCreator) {
    if (suspendedJobIds != null) {
      return EMBEDDED_EXECUTOR_CTOR.newInstance(
          jobIds,
          suspendedJobIds,
          terminalJobIds,
          dispatcherGateway,
          configuration,
          jobClientCreator);
    }
    return IS_FLINK_1
        ? EMBEDDED_EXECUTOR_CTOR.newInstance(jobIds, dispatcherGateway, jobClientCreator)
        : EMBEDDED_EXECUTOR_CTOR.newInstance(
            jobIds, dispatcherGateway, configuration, jobClientCreator);
  }

  /**
   * FLINK-38974 (2.3.0) requires submitted jobs to carry the id of the application registered in
   * the dispatcher. The bootstrap SQL goes through StreamContextEnvironment which stamps the id,
   * while Kyuubi statements go through plain environments, so cache the id and stamp it on every
   * StreamGraph before delegating to the {@link EmbeddedExecutor}.
   */
  private static PipelineExecutor stampApplicationId(final PipelineExecutor executor) {
    if (suspendedJobIds == null) {
      return executor;
    }
    return (pipeline, configuration, userCodeClassloader) -> {
      if (pipeline instanceof StreamGraph) {
        final StreamGraph streamGraph = (StreamGraph) pipeline;
        final Object appId = captureApplicationId(streamGraph);
        if (appId != null) {
          streamGraphSetApplicationId(streamGraph, appId);
        }
      }
      return executor.execute(pipeline, configuration, userCodeClassloader);
    };
  }

  private static Object captureApplicationId(final StreamGraph streamGraph) {
    Object appId = applicationId;
    if (appId == null) {
      synchronized (bootstrapLock) {
        appId = applicationId;
        if (appId == null) {
          initStreamGraphApplicationIdMethods();
          applicationId = appId = streamGraphGetApplicationId(streamGraph);
        }
      }
    }
    return appId;
  }

  @SuppressWarnings("unchecked")
  private static Object streamGraphGetApplicationId(final StreamGraph streamGraph) {
    return ((Optional<Object>) streamGraphGetApplicationId.invoke(streamGraph)).orElse(null);
  }

  private static void streamGraphSetApplicationId(
      final StreamGraph streamGraph, final Object applicationId) {
    streamGraphSetApplicationId.invoke(streamGraph, applicationId);
  }

  private static void initStreamGraphApplicationIdMethods() {
    if (streamGraphGetApplicationId == null) {
      streamGraphGetApplicationId =
          DynMethods.builder("getApplicationId").impl(StreamGraph.class).build();
      streamGraphSetApplicationId =
          DynMethods.builder("setApplicationId")
              .impl(StreamGraph.class, APPLICATION_ID_CLASS)
              .build();
    }
  }

  private static JobClient newEmbeddedJobClient(
      final JobID jobId, final Duration timeout, final ClassLoader userCodeClassloader) {
    final Object rpcTimeout =
        IS_FLINK_1 ? LEGACY_TIME_OF_MILLIS.invoke(timeout.toMillis()) : timeout;
    return EMBEDDED_JOB_CLIENT_CTOR.newInstance(
        jobId, dispatcherGateway, retryExecutor, rpcTimeout, userCodeClassloader);
  }
}
