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

import java.util.Locale
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.JavaConverters._

import com.google.common.cache.{Cache, CacheBuilder, RemovalNotification}
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.informers.{ResourceEventHandler, SharedIndexInformer}

import org.apache.kyuubi.{KyuubiException, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.ApplicationState.{isTerminated, ApplicationState, FAILED, FINISHED, NOT_FOUND, PENDING, RUNNING, UNKNOWN}
import org.apache.kyuubi.engine.KubernetesApplicationOperation.{toApplicationState, LABEL_KYUUBI_UNIQUE_KEY, SPARK_APP_ID_LABEL}
import org.apache.kyuubi.util.KubernetesUtils

class KubernetesApplicationOperation extends ApplicationOperation with Logging {

  private val kubernetesClients: ConcurrentHashMap[KubernetesInfo, KubernetesClient] =
    new ConcurrentHashMap[KubernetesInfo, KubernetesClient]
  private val enginePodInformers: ConcurrentHashMap[KubernetesInfo, SharedIndexInformer[Pod]] =
    new ConcurrentHashMap[KubernetesInfo, SharedIndexInformer[Pod]]

  private var submitTimeout: Long = _
  private var kyuubiConf: KyuubiConf = _

  private def allowedContexts: Seq[String] =
    kyuubiConf.get(KyuubiConf.KUBERNETES_CONTEXT_ALLOW_LIST)
  private def allowedNamespaces: Seq[String] =
    kyuubiConf.get(KyuubiConf.KUBERNETES_NAMESPACE_ALLOW_LIST)

  // key is kyuubi_unique_key
  private val appInfoStore: ConcurrentHashMap[String, ApplicationInfo] =
    new ConcurrentHashMap[String, ApplicationInfo]
  // key is kyuubi_unique_key
  private var cleanupTerminatedAppInfoTrigger: Cache[String, ApplicationState] = _

  private def getOrCreateKubernetesClient(kubernetesInfo: KubernetesInfo): KubernetesClient = {
    val context = kubernetesInfo.context
    val namespace = kubernetesInfo.namespace

    if (allowedContexts.nonEmpty && !allowedContexts.contains(context)) {
      throw new KyuubiException(
        s"Kubernetes context $context is not in the allowed list[$allowedContexts]")
    }

    if (allowedNamespaces.nonEmpty && !allowedNamespaces.contains(namespace)) {
      throw new KyuubiException(
        s"Kubernetes namespace $namespace is not in the allowed list[$allowedNamespaces]")
    }

    kubernetesClients.computeIfAbsent(kubernetesInfo, kInfo => buildKubernetesClient(kInfo))
  }

  private def buildKubernetesClient(kubernetesInfo: KubernetesInfo): KubernetesClient = {
    val kubernetesConf =
      kyuubiConf.getKubernetesConf(kubernetesInfo.context, kubernetesInfo.namespace)
    KubernetesUtils.buildKubernetesClient(kubernetesConf) match {
      case Some(client) =>
        info(s"[$kubernetesInfo] Initialized Kubernetes Client connect to: ${client.getMasterUrl}")
        val enginePodInformer = client.pods()
          .withLabel(LABEL_KYUUBI_UNIQUE_KEY)
          .inform(new SparkEnginePodEventHandler)
        info(s"[$kubernetesInfo] Start Kubernetes Client Informer.")
        enginePodInformers.put(kubernetesInfo, enginePodInformer)
        client

      case None => throw new KyuubiException(s"Fail to build Kubernetes client for $kubernetesInfo")
    }
  }

  override def initialize(conf: KyuubiConf): Unit = {
    kyuubiConf = conf
    info("Start initializing Kubernetes application operation.")
    submitTimeout = conf.get(KyuubiConf.ENGINE_KUBERNETES_SUBMIT_TIMEOUT)
    // Defer cleaning terminated application information
    val retainPeriod = conf.get(KyuubiConf.KUBERNETES_TERMINATED_APPLICATION_RETAIN_PERIOD)
    cleanupTerminatedAppInfoTrigger = CacheBuilder.newBuilder()
      .expireAfterWrite(retainPeriod, TimeUnit.MILLISECONDS)
      .removalListener((notification: RemovalNotification[String, ApplicationState]) => {
        Option(appInfoStore.remove(notification.getKey)).foreach { removed =>
          info(s"Remove terminated application ${removed.id} with " +
            s"tag ${notification.getKey} and state ${removed.state}")
        }
      })
      .build()
  }

  override def isSupported(appMgrInfo: ApplicationManagerInfo): Boolean = {
    // TODO add deploy mode to check whether is supported
    kyuubiConf != null &&
    appMgrInfo.resourceManager.exists(_.toLowerCase(Locale.ROOT).startsWith("k8s"))
  }

  override def killApplicationByTag(
      appMgrInfo: ApplicationManagerInfo,
      tag: String): KillResponse = {
    if (kyuubiConf == null) {
      throw new IllegalStateException("Methods initialize and isSupported must be called ahead")
    }
    val kubernetesInfo = appMgrInfo.kubernetesInfo
    val kubernetesClient = getOrCreateKubernetesClient(kubernetesInfo)
    debug(s"[$kubernetesInfo] Deleting application info from Kubernetes cluster by $tag tag")
    try {
      val info = appInfoStore.getOrDefault(tag, ApplicationInfo.NOT_FOUND)
      debug(s"Application info[tag: $tag] is in ${info.state}")
      info.state match {
        case NOT_FOUND | FAILED | UNKNOWN =>
          (
            false,
            s"[$kubernetesInfo] Target application[tag: $tag] is in ${info.state} status")
        case _ =>
          (
            !kubernetesClient.pods.withName(info.name).delete().isEmpty,
            s"[$kubernetesInfo] Operation of deleted" +
              s" application[appId: ${info.id} ,tag: $tag] is completed")
      }
    } catch {
      case e: Exception =>
        (
          false,
          s"[$kubernetesInfo] Failed to terminate application with $tag, due to ${e.getMessage}")
    }
  }

  override def getApplicationInfoByTag(
      appMgrInfo: ApplicationManagerInfo,
      tag: String,
      submitTime: Option[Long]): ApplicationInfo = {
    if (kyuubiConf == null) {
      throw new IllegalStateException("Methods initialize and isSupported must be called ahead")
    }
    debug(s"Getting application info from Kubernetes cluster by $tag tag")
    try {
      // need to initialize the kubernetes client if not exists
      getOrCreateKubernetesClient(appMgrInfo.kubernetesInfo)
      val appInfo = appInfoStore.getOrDefault(tag, ApplicationInfo.NOT_FOUND)
      (appInfo.state, submitTime) match {
        // Kyuubi should wait second if pod is not be created
        case (NOT_FOUND, Some(_submitTime)) =>
          val elapsedTime = System.currentTimeMillis - _submitTime
          if (elapsedTime > submitTimeout) {
            error(s"Can't find target driver pod by tag: $tag, " +
              s"elapsed time: ${elapsedTime}ms exceeds ${submitTimeout}ms.")
            ApplicationInfo.NOT_FOUND
          } else {
            warn("Wait for driver pod to be created, " +
              s"elapsed time: ${elapsedTime}ms, return UNKNOWN status")
            ApplicationInfo.UNKNOWN
          }
        case (NOT_FOUND, None) =>
          ApplicationInfo.NOT_FOUND
        case _ =>
          debug(s"Successfully got application info by $tag: $appInfo")
          appInfo
      }
    } catch {
      case e: Exception =>
        error(s"Failed to get application with $tag, due to ${e.getMessage}")
        ApplicationInfo.NOT_FOUND
    }
  }

  override def stop(): Unit = {
    enginePodInformers.asScala.foreach { case (_, informer) =>
      Utils.tryLogNonFatalError(informer.stop())
    }
    enginePodInformers.clear()

    kubernetesClients.asScala.foreach { case (_, client) =>
      Utils.tryLogNonFatalError(client.close())
    }
    kubernetesClients.clear()

    if (cleanupTerminatedAppInfoTrigger != null) {
      cleanupTerminatedAppInfoTrigger.cleanUp()
      cleanupTerminatedAppInfoTrigger = null
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
        val appState = toApplicationState(newPod.getStatus.getPhase)
        if (isTerminated(appState)) {
          markApplicationTerminated(newPod)
        }
      }
    }

    override def onDelete(pod: Pod, deletedFinalStateUnknown: Boolean): Unit = {
      if (isSparkEnginePod(pod)) {
        updateApplicationState(pod)
        markApplicationTerminated(pod)
      }
    }
  }

  private def isSparkEnginePod(pod: Pod): Boolean = {
    val labels = pod.getMetadata.getLabels
    labels.containsKey(LABEL_KYUUBI_UNIQUE_KEY) && labels.containsKey(SPARK_APP_ID_LABEL)
  }

  private def updateApplicationState(pod: Pod): Unit = {
    val appState = toApplicationState(pod.getStatus.getPhase)
    debug(s"Driver Informer changes pod: ${pod.getMetadata.getName} to state: $appState")
    appInfoStore.put(
      pod.getMetadata.getLabels.get(LABEL_KYUUBI_UNIQUE_KEY),
      ApplicationInfo(
        id = pod.getMetadata.getLabels.get(SPARK_APP_ID_LABEL),
        name = pod.getMetadata.getName,
        state = appState,
        error = Option(pod.getStatus.getReason)))
  }

  private def markApplicationTerminated(pod: Pod): Unit = synchronized {
    val key = pod.getMetadata.getLabels.get(LABEL_KYUUBI_UNIQUE_KEY)
    if (cleanupTerminatedAppInfoTrigger.getIfPresent(key) == null) {
      cleanupTerminatedAppInfoTrigger.put(key, toApplicationState(pod.getStatus.getPhase))
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
    case "Unknown" => UNKNOWN
    case _ =>
      warn(s"The kubernetes driver pod state: $state is not supported, " +
        "mark the application state as UNKNOWN.")
      UNKNOWN
  }
}
