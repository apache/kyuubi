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

package org.apache.kyuubi.engine

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.google.common.cache.{Cache, CacheBuilder, RemovalNotification}
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.informers.ResourceEventHandler
import io.fabric8.kubernetes.client.informers.SharedIndexInformer

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.ApplicationState.{isTerminated, ApplicationState, FAILED, FINISHED, NOT_FOUND, PENDING, RUNNING, UNKNOWN}
import org.apache.kyuubi.engine.KubernetesApplicationOperation.{toApplicationState, LABEL_KYUUBI_UNIQUE_KEY, SPARK_APP_ID_LABEL}
import org.apache.kyuubi.util.KubernetesUtils

class KubernetesApplicationOperation extends ApplicationOperation with Logging {

  @volatile
  private var kubernetesClient: KubernetesClient = _
  private var enginePodInformer: SharedIndexInformer[Pod] = _
  private var submitTimeout: Long = _

  private val appInfoStore: ConcurrentHashMap[String, ApplicationInfo] =
    new ConcurrentHashMap[String, ApplicationInfo]
  private var deletedAppInfoCache: Cache[String, ApplicationState] = _

  override def initialize(conf: KyuubiConf): Unit = {
    info("Start initializing Kubernetes Client.")
    kubernetesClient = KubernetesUtils.buildKubernetesClient(conf) match {
      case Some(client) =>
        info(s"Initialized Kubernetes Client connect to: ${client.getMasterUrl}")
        submitTimeout = conf.get(KyuubiConf.ENGINE_SUBMIT_TIMEOUT)
        // Using Kubernetes Informer to update application state
        // Set 0 for no resync, see more details in
        // https://github.com/fabric8io/kubernetes-client/discussions/5015
        enginePodInformer =
          client.pods().withLabel(LABEL_KYUUBI_UNIQUE_KEY).runnableInformer(0)
        enginePodInformer.addEventHandler(new SparkEnginePodEventHandler()).start()
        info("Start Kubernetes Client Informer.")
        // Using Cache help clean delete app info
        val cachePeriod = conf.get(KyuubiConf.KUBERNETES_TERMINATED_APPLICATION_RETAIN_PERIOD)
        deletedAppInfoCache = CacheBuilder
          .newBuilder()
          .expireAfterWrite(cachePeriod, TimeUnit.MILLISECONDS)
          .removalListener((notification: RemovalNotification[String, ApplicationState]) => {
            debug(s"Remove cached appInfo[tag: ${notification.getKey}], " +
              s"due to app state: ${notification.getValue}.")
            appInfoStore.remove(notification.getKey)
          })
          .build()
        client
      case None =>
        warn("Fail to init Kubernetes Client for Kubernetes Application Operation")
        null
    }
  }

  override def isSupported(clusterManager: Option[String]): Boolean = {
    // TODO add deploy mode to check whether is supported
    kubernetesClient != null && clusterManager.nonEmpty &&
    clusterManager.get.toLowerCase.startsWith("k8s")
  }

  override def killApplicationByTag(tag: String): KillResponse = {
    if (kubernetesClient == null) {
      throw new IllegalStateException("Methods initialize and isSupported must be called ahead")
    }
    debug(s"Deleting application info from Kubernetes cluster by $tag tag")
    try {
      val info = appInfoStore.getOrDefault(tag, ApplicationInfo.notFound)
      debug(s"Application info[tag: ${tag}] is in ${info.state}")
      info.state match {
        case NOT_FOUND | FAILED | UNKNOWN =>
          (
            false,
            s"Target application[tag: $tag] is in ${info.state} status")
        case _ =>
          (
            !kubernetesClient.pods.withName(info.name).delete().isEmpty,
            s"Operation of deleted application[appId: ${info.id} ,tag: $tag] is completed")
      }
    } catch {
      case e: Exception =>
        (false, s"Failed to terminate application with $tag, due to ${e.getMessage}")
    }
  }

  override def getApplicationInfoByTag(tag: String, submitTime: Option[Long]): ApplicationInfo = {
    if (kubernetesClient == null) {
      throw new IllegalStateException("Methods initialize and isSupported must be called ahead")
    }
    debug(s"Getting application info from Kubernetes cluster by $tag tag")
    try {
      val info = appInfoStore.getOrDefault(tag, ApplicationInfo.notFound)
      info.state match {
        // Kyuubi should wait second if pod is not be created
        case NOT_FOUND if submitTime.nonEmpty =>
          val elapsedTime = System.currentTimeMillis() - submitTime.get
          if (elapsedTime > submitTimeout) {
            error(s"Can't find target driver pod by tag: $tag, " +
              s"elapsed time: ${elapsedTime}ms exceeds ${submitTimeout}ms.")
            ApplicationInfo(id = null, name = null, ApplicationState.NOT_FOUND)
          } else {
            warn("Wait for driver pod to be created, " +
              s"elapsed time: ${elapsedTime}ms, return UNKNOWN status")
            ApplicationInfo(id = null, name = null, ApplicationState.UNKNOWN)
          }
        case NOT_FOUND =>
          ApplicationInfo(id = null, name = null, ApplicationState.NOT_FOUND)
        case _ =>
          debug(s"Successfully got application info by $tag: $info")
          info
      }
    } catch {
      case e: Exception =>
        error(s"Failed to get application with $tag, due to ${e.getMessage}")
        ApplicationInfo(id = null, name = null, ApplicationState.NOT_FOUND)
    }
  }

  override def stop(): Unit = {
    try {
      if (enginePodInformer != null) {
        enginePodInformer.stop()
      }
      if (kubernetesClient != null) {
        kubernetesClient.close()
      }
      if (deletedAppInfoCache != null) {
        deletedAppInfoCache.cleanUp()
      }
    } catch {
      case e: Exception => error(e.getMessage)
    }
  }

  private class SparkEnginePodEventHandler extends ResourceEventHandler[Pod] {

    override def onAdd(pod: Pod): Unit = {
      if (isSparkEnginePod(pod)) {
        updateApplicationState(pod)
      }
    }

    override def onUpdate(oldPod: Pod, newPod: Pod): Unit = {
      if (isSparkEnginePod(newPod)) {
        updateApplicationState(newPod)
        toApplicationState(newPod.getStatus.getPhase) match {
          case state if isTerminated(state) =>
            markTerminated(newPod)
          case _ =>
          // do nothing
        }
      }
    }

    override def onDelete(pod: Pod, deletedFinalStateUnknown: Boolean): Unit = {
      if (isSparkEnginePod(pod)) {
        updateApplicationState(pod)
        markTerminated(pod)
      }
    }
  }

  private def isSparkEnginePod(pod: Pod): Boolean = {
    pod.getMetadata.getLabels.containsKey(LABEL_KYUUBI_UNIQUE_KEY)
  }

  private def updateApplicationState(pod: Pod): Unit = {
    val metaData = pod.getMetadata
    val state = toApplicationState(pod.getStatus.getPhase)
    debug(s"Driver Informer change pod: ${metaData.getName} state: $state")
    appInfoStore.put(
      metaData.getLabels.get(LABEL_KYUUBI_UNIQUE_KEY),
      ApplicationInfo(
        id = metaData.getLabels.get(SPARK_APP_ID_LABEL),
        name = metaData.getName,
        state = state,
        error = Option(pod.getStatus.getReason)))
  }

  private def markTerminated(pod: Pod): Unit = {
    deletedAppInfoCache.put(
      pod.getMetadata.getLabels.get(LABEL_KYUUBI_UNIQUE_KEY),
      toApplicationState(pod.getStatus.getPhase))
  }
}

object KubernetesApplicationOperation extends Logging {
  val LABEL_KYUUBI_UNIQUE_KEY = "kyuubi-unique-tag"
  val SPARK_APP_ID_LABEL = "spark-app-selector"
  val KUBERNETES_SERVICE_HOST = "KUBERNETES_SERVICE_HOST"
  val KUBERNETES_SERVICE_PORT = "KUBERNETES_SERVICE_PORT"

  def toApplicationState(state: String): ApplicationState = state match {
    // https://github.com/kubernetes/kubernetes/blob/master/pkg/apis/core/types.go#L2396
    // https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/
    case "Pending" => PENDING
    case "Running" => RUNNING
    case "Succeeded" => FINISHED
    case "Failed" | "Error" => FAILED
    case "Unknown" => ApplicationState.UNKNOWN
    case _ =>
      warn(s"The kubernetes driver pod state: $state is not supported, " +
        "mark the application state as UNKNOWN.")
      ApplicationState.UNKNOWN
  }
}
