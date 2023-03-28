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
import org.apache.kyuubi.engine.ApplicationState.{ApplicationState, FAILED, FINISHED, NOT_FOUND, PENDING, RUNNING, UNKNOWN}
import org.apache.kyuubi.engine.KubernetesApplicationOperation.{toApplicationState, LABEL_KYUUBI_UNIQUE_KEY, SPARK_APP_ID_LABEL}
import org.apache.kyuubi.util.KubernetesUtils

class KubernetesApplicationOperation extends ApplicationOperation with Logging {

  @volatile
  private var kubernetesClient: KubernetesClient = _
  private var driverInformer: SharedIndexInformer[Pod] = _
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
        val informerPeriod = conf.get(KyuubiConf.KUBERNETES_INFORMER_PERIOD)
        driverInformer = client.informers().sharedIndexInformerFor(
          classOf[Pod],
          informerPeriod)
        driverInformer.addEventHandler(new DriverPodEventHandler()).start()
        info("Start Kubernetes Client Informer.")
        // Using Cache help clean delete app info
        val cachePeriod = conf.get(KyuubiConf.KUBERNETES_INFORMER_CACHE_PERIOD)
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
      if (kubernetesClient != null) {
        kubernetesClient.close()
      }
      if (driverInformer != null) {
        driverInformer.stop()
      }
      if (deletedAppInfoCache != null) {
        deletedAppInfoCache.cleanUp()
      }
    } catch {
      case e: Exception => error(e.getMessage)
    }
  }

  class DriverPodEventHandler extends ResourceEventHandler[Pod] {
    private def filter(pod: Pod): Boolean = {
      pod.getMetadata.getLabels.containsKey(LABEL_KYUUBI_UNIQUE_KEY)
    }

    private def updateState(pod: Pod): Unit = {
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

    private def markDeleted(pod: Pod): Unit = {
      deletedAppInfoCache.put(
        pod.getMetadata.getLabels.get(LABEL_KYUUBI_UNIQUE_KEY),
        toApplicationState(pod.getStatus.getPhase))
    }

    override def onAdd(pod: Pod): Unit = {
      if (filter(pod)) {
        updateState(pod)
      }
    }

    override def onUpdate(oldPod: Pod, newPod: Pod): Unit = {
      if (filter(newPod)) {
        updateState(newPod)
        toApplicationState(newPod.getStatus.getPhase) match {
          case FINISHED | FAILED | UNKNOWN =>
            markDeleted(newPod)
        }
      }
    }

    override def onDelete(pod: Pod, deletedFinalStateUnknown: Boolean): Unit = {
      if (filter(pod)) {
        updateState(pod)
        markDeleted(pod)
      }
    }
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
