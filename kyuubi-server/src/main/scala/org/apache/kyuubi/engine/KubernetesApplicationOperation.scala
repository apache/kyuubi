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
import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService, ThreadPoolExecutor, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.google.common.cache.{Cache, CacheBuilder, RemovalNotification}
import io.fabric8.kubernetes.api.model.{ContainerState, Pod, Service}
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.informers.{ResourceEventHandler, SharedIndexInformer}

import org.apache.kyuubi.{KyuubiException, Logging, Utils}
import org.apache.kyuubi.client.util.JsonUtils
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{KubernetesApplicationStateSource, KubernetesCleanupDriverPodStrategy}
import org.apache.kyuubi.config.KyuubiConf.KubernetesApplicationStateSource.KubernetesApplicationStateSource
import org.apache.kyuubi.config.KyuubiConf.KubernetesCleanupDriverPodStrategy.{ALL, COMPLETED, NONE}
import org.apache.kyuubi.engine.ApplicationState.{isTerminated, ApplicationState, FAILED, FINISHED, KILLED, NOT_FOUND, PENDING, RUNNING, UNKNOWN}
import org.apache.kyuubi.operation.OperationState
import org.apache.kyuubi.server.KyuubiServer
import org.apache.kyuubi.session.KyuubiSessionManager
import org.apache.kyuubi.util.{KubernetesUtils, ThreadUtils}

class KubernetesApplicationOperation extends ApplicationOperation with Logging {
  import KubernetesApplicationOperation._

  private val kubernetesClients: ConcurrentHashMap[KubernetesInfo, KubernetesClient] =
    new ConcurrentHashMap[KubernetesInfo, KubernetesClient]
  private val enginePodInformers: ConcurrentHashMap[KubernetesInfo, SharedIndexInformer[Pod]] =
    new ConcurrentHashMap[KubernetesInfo, SharedIndexInformer[Pod]]
  private val engineSvcInformers: ConcurrentHashMap[KubernetesInfo, SharedIndexInformer[Service]] =
    new ConcurrentHashMap[KubernetesInfo, SharedIndexInformer[Service]]

  private var submitTimeout: Long = _
  private var kyuubiConf: KyuubiConf = _

  private def allowedContexts: Set[String] =
    kyuubiConf.get(KyuubiConf.KUBERNETES_CONTEXT_ALLOW_LIST)
  private def allowedNamespaces: Set[String] =
    kyuubiConf.get(KyuubiConf.KUBERNETES_NAMESPACE_ALLOW_LIST)

  private def appStateSource: KubernetesApplicationStateSource =
    KubernetesApplicationStateSource.withName(
      kyuubiConf.get(KyuubiConf.KUBERNETES_APPLICATION_STATE_SOURCE))
  private def appStateContainer: String =
    kyuubiConf.get(KyuubiConf.KUBERNETES_APPLICATION_STATE_CONTAINER)

  // key is kyuubi_unique_key
  private val appInfoStore: ConcurrentHashMap[String, (KubernetesInfo, ApplicationInfo)] =
    new ConcurrentHashMap[String, (KubernetesInfo, ApplicationInfo)]
  // key is kyuubi_unique_key
  private var cleanupTerminatedAppInfoTrigger: Cache[String, ApplicationState] = _

  private var expireCleanUpTriggerCacheExecutor: ScheduledExecutorService = _

  private var cleanupCanceledAppPodExecutor: ThreadPoolExecutor = _

  private def getOrCreateKubernetesClient(kubernetesInfo: KubernetesInfo): KubernetesClient = {
    checkKubernetesInfo(kubernetesInfo)
    kubernetesClients.computeIfAbsent(kubernetesInfo, kInfo => buildKubernetesClient(kInfo))
  }

  private def metadataManager = KyuubiServer.kyuubiServer.backendService
    .sessionManager.asInstanceOf[KyuubiSessionManager].metadataManager

  // Visible for testing
  private[engine] def checkKubernetesInfo(kubernetesInfo: KubernetesInfo): Unit = {
    val context = kubernetesInfo.context
    val namespace = kubernetesInfo.namespace

    if (allowedContexts.nonEmpty && context.exists(!allowedContexts.contains(_))) {
      throw new KyuubiException(
        s"Kubernetes context $context is not in the allowed list[$allowedContexts]")
    }

    if (allowedNamespaces.nonEmpty && namespace.exists(!allowedNamespaces.contains(_))) {
      throw new KyuubiException(
        s"Kubernetes namespace $namespace is not in the allowed list[$allowedNamespaces]")
    }
  }

  private def buildKubernetesClient(kubernetesInfo: KubernetesInfo): KubernetesClient = {
    val kubernetesConf =
      kyuubiConf.getKubernetesConf(kubernetesInfo.context, kubernetesInfo.namespace)
    KubernetesUtils.buildKubernetesClient(kubernetesConf) match {
      case Some(client) =>
        info(s"[$kubernetesInfo] Initialized Kubernetes Client connect to: ${client.getMasterUrl}")
        val enginePodInformer = client.pods()
          .withLabel(LABEL_KYUUBI_UNIQUE_KEY)
          .inform(new SparkEnginePodEventHandler(kubernetesInfo))
        info(s"[$kubernetesInfo] Start Kubernetes Client Informer.")
        val engineSvcInformer = client.services()
          .inform(new SparkEngineSvcEventHandler(kubernetesInfo))
        enginePodInformers.put(kubernetesInfo, enginePodInformer)
        engineSvcInformers.put(kubernetesInfo, engineSvcInformer)
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
    val cleanupDriverPodStrategy = KubernetesCleanupDriverPodStrategy.withName(
      conf.get(KyuubiConf.KUBERNETES_SPARK_CLEANUP_TERMINATED_DRIVER_POD_KIND))
    val cleanupDriverPodCheckInterval = conf.get(
      KyuubiConf.KUBERNETES_SPARK_CLEANUP_TERMINATED_DRIVER_POD_KIND_CHECK_INTERVAL)
    cleanupTerminatedAppInfoTrigger = CacheBuilder.newBuilder()
      .expireAfterWrite(retainPeriod, TimeUnit.MILLISECONDS)
      .removalListener((notification: RemovalNotification[String, ApplicationState]) => {
        Option(appInfoStore.remove(notification.getKey)).foreach { case (kubernetesInfo, removed) =>
          val appLabel = notification.getKey
          val shouldDelete = cleanupDriverPodStrategy match {
            case NONE => false
            case ALL => true
            case COMPLETED => !ApplicationState.isFailed(notification.getValue)
          }
          if (shouldDelete) {
            deletePod(kubernetesInfo, removed.name, appLabel)
          }
          info(s"Remove terminated application $removed with ${toLabel(appLabel)}")
        }
      })
      .build()
    expireCleanUpTriggerCacheExecutor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "pod-cleanup-trigger-thread")
    ThreadUtils.scheduleTolerableRunnableWithFixedDelay(
      expireCleanUpTriggerCacheExecutor,
      () => {
        try {
          if (cleanupTerminatedAppInfoTrigger == null) return
          cleanupTerminatedAppInfoTrigger.asMap().asScala.foreach {
            case (key, _) =>
              // do get to trigger cache eviction
              cleanupTerminatedAppInfoTrigger.getIfPresent(key)
          }
        } catch {
          case NonFatal(e) => error("Failed to evict clean up terminated app cache", e)
        }
      },
      cleanupDriverPodCheckInterval,
      cleanupDriverPodCheckInterval,
      TimeUnit.MILLISECONDS)
    cleanupCanceledAppPodExecutor = ThreadUtils.newDaemonCachedThreadPool(
      "cleanup-canceled-app-pod-thread")
  }

  override def isSupported(appMgrInfo: ApplicationManagerInfo): Boolean = {
    // TODO add deploy mode to check whether is supported
    kyuubiConf != null &&
    appMgrInfo.resourceManager.exists(_.toLowerCase(Locale.ROOT).startsWith("k8s"))
  }

  override def killApplicationByTag(
      appMgrInfo: ApplicationManagerInfo,
      tag: String,
      proxyUser: Option[String] = None): KillResponse = {
    if (kyuubiConf == null) {
      throw new IllegalStateException("Methods initialize and isSupported must be called ahead")
    }
    val kubernetesInfo = appMgrInfo.kubernetesInfo
    val kubernetesClient = getOrCreateKubernetesClient(kubernetesInfo)
    debug(s"[$kubernetesInfo] Deleting application[${toLabel(tag)}]'s info from Kubernetes cluster")
    try {
      Option(appInfoStore.get(tag)) match {
        case Some((_, info)) =>
          debug(s"Application[${toLabel(tag)}] is in ${info.state} state")
          info.state match {
            case NOT_FOUND | FAILED | UNKNOWN =>
              (
                false,
                s"[$kubernetesInfo] Target application[${toLabel(tag)}] is in ${info.state} state")
            case _ =>
              (
                !kubernetesClient.pods.withName(info.name).delete().isEmpty,
                s"[$kubernetesInfo] Operation of deleted" +
                  s" application[appId: ${info.id}, ${toLabel(tag)}] is completed")
          }
        case None =>
          warn(s"No application info found, trying to delete pod with ${toLabel(tag)}")
          (
            !kubernetesClient.pods.withLabel(LABEL_KYUUBI_UNIQUE_KEY, tag).delete().isEmpty,
            s"[$kubernetesInfo] Operation of deleted pod with ${toLabel(tag)} is completed")
      }
    } catch {
      case e: Exception =>
        (
          false,
          s"[$kubernetesInfo] Failed to terminate application[${toLabel(tag)}], " +
            s"due to ${e.getMessage}")
    }
  }

  override def getApplicationInfoByTag(
      appMgrInfo: ApplicationManagerInfo,
      tag: String,
      proxyUser: Option[String] = None,
      submitTime: Option[Long] = None): ApplicationInfo = {
    if (kyuubiConf == null) {
      throw new IllegalStateException("Methods initialize and isSupported must be called ahead")
    }
    debug(s"Getting application[${toLabel(tag)}]'s info from Kubernetes cluster")
    try {
      // need to initialize the kubernetes client if not exists
      getOrCreateKubernetesClient(appMgrInfo.kubernetesInfo)
      val (_, appInfo) =
        appInfoStore.getOrDefault(tag, appMgrInfo.kubernetesInfo -> ApplicationInfo.NOT_FOUND)
      (appInfo.state, submitTime) match {
        // Kyuubi should wait second if pod is not be created
        case (NOT_FOUND, Some(_submitTime)) =>
          val elapsedTime = System.currentTimeMillis - _submitTime
          if (elapsedTime > submitTimeout) {
            error(s"Can't find target driver pod by ${toLabel(tag)}, " +
              s"elapsed time: ${elapsedTime}ms exceeds ${submitTimeout}ms.")
            ApplicationInfo.NOT_FOUND
          } else {
            warn(s"Waiting for driver pod with ${toLabel(tag)} to be created, " +
              s"elapsed time: ${elapsedTime}ms, return UNKNOWN status")
            ApplicationInfo.UNKNOWN
          }
        case (NOT_FOUND, None) =>
          ApplicationInfo.NOT_FOUND
        case _ =>
          debug(s"Successfully got application[${toLabel(tag)}]'s info: $appInfo")
          appInfo
      }
    } catch {
      case e: Exception =>
        error(s"Failed to get application by ${toLabel(tag)}, due to ${e.getMessage}")
        ApplicationInfo.NOT_FOUND
    }
  }

  override def stop(): Unit = {
    enginePodInformers.asScala.foreach { case (_, informer) =>
      Utils.tryLogNonFatalError(informer.stop())
    }
    enginePodInformers.clear()

    engineSvcInformers.asScala.foreach { case (_, informer) =>
      Utils.tryLogNonFatalError(informer.stop())
    }
    engineSvcInformers.clear()

    if (cleanupTerminatedAppInfoTrigger != null) {
      cleanupTerminatedAppInfoTrigger.invalidateAll()
      cleanupTerminatedAppInfoTrigger = null
    }

    kubernetesClients.asScala.foreach { case (_, client) =>
      Utils.tryLogNonFatalError(client.close())
    }
    kubernetesClients.clear()

    if (expireCleanUpTriggerCacheExecutor != null) {
      ThreadUtils.shutdown(expireCleanUpTriggerCacheExecutor)
      expireCleanUpTriggerCacheExecutor = null
    }

    if (cleanupCanceledAppPodExecutor != null) {
      ThreadUtils.shutdown(cleanupCanceledAppPodExecutor)
      cleanupCanceledAppPodExecutor = null
    }
  }

  private class SparkEnginePodEventHandler(kubernetesInfo: KubernetesInfo)
    extends ResourceEventHandler[Pod] {

    override def onAdd(pod: Pod): Unit = {
      if (isSparkEnginePod(pod)) {
        updateApplicationState(kubernetesInfo, pod)
        KubernetesApplicationAuditLogger.audit(
          kubernetesInfo,
          pod,
          appStateSource,
          appStateContainer)
        checkPodAppCanceled(kubernetesInfo, pod)
      }
    }

    override def onUpdate(oldPod: Pod, newPod: Pod): Unit = {
      if (isSparkEnginePod(newPod)) {
        val kyuubiUniqueKey = newPod.getMetadata.getLabels.get(LABEL_KYUUBI_UNIQUE_KEY)
        val firstUpdate = appInfoStore.get(kyuubiUniqueKey) == null
        updateApplicationState(kubernetesInfo, newPod)
        val appState = toApplicationState(newPod, appStateSource, appStateContainer)
        if (isTerminated(appState)) {
          markApplicationTerminated(newPod)
        }
        KubernetesApplicationAuditLogger.audit(
          kubernetesInfo,
          newPod,
          appStateSource,
          appStateContainer)
        if (firstUpdate) {
          checkPodAppCanceled(kubernetesInfo, newPod)
        }
      }
    }

    override def onDelete(pod: Pod, deletedFinalStateUnknown: Boolean): Unit = {
      if (isSparkEnginePod(pod)) {
        updateApplicationState(kubernetesInfo, pod)
        markApplicationTerminated(pod)
        KubernetesApplicationAuditLogger.audit(
          kubernetesInfo,
          pod,
          appStateSource,
          appStateContainer)
      }
    }
  }

  private class SparkEngineSvcEventHandler(kubernetesInfo: KubernetesInfo)
    extends ResourceEventHandler[Service] {

    override def onAdd(svc: Service): Unit = {
      if (isSparkEngineSvc(svc)) {
        updateApplicationUrl(kubernetesInfo, svc)
      }
    }

    override def onUpdate(oldSvc: Service, newSvc: Service): Unit = {
      if (isSparkEngineSvc(newSvc)) {
        updateApplicationUrl(kubernetesInfo, newSvc)
      }
    }

    override def onDelete(svc: Service, deletedFinalStateUnknown: Boolean): Unit = {
      // do nothing
    }
  }

  private def isSparkEnginePod(pod: Pod): Boolean = {
    val labels = pod.getMetadata.getLabels
    labels.containsKey(LABEL_KYUUBI_UNIQUE_KEY) && labels.containsKey(SPARK_APP_ID_LABEL)
  }

  private def isSparkEngineSvc(svc: Service): Boolean = {
    val selectors = svc.getSpec.getSelector
    selectors.containsKey(LABEL_KYUUBI_UNIQUE_KEY) && selectors.containsKey(SPARK_APP_ID_LABEL)
  }

  private def updateApplicationState(kubernetesInfo: KubernetesInfo, pod: Pod): Unit = {
    val (appState, appError) =
      toApplicationStateAndError(pod, appStateSource, appStateContainer)
    debug(s"Driver Informer changes pod: ${pod.getMetadata.getName} to state: $appState")
    val kyuubiUniqueKey = pod.getMetadata.getLabels.get(LABEL_KYUUBI_UNIQUE_KEY)
    appInfoStore.synchronized {
      Option(appInfoStore.get(kyuubiUniqueKey)).map { case (_, appInfo) =>
        appInfoStore.put(
          kyuubiUniqueKey,
          kubernetesInfo -> appInfo.copy(
            id = pod.getMetadata.getLabels.get(SPARK_APP_ID_LABEL),
            name = pod.getMetadata.getName,
            state = appState,
            error = appError))
      }.getOrElse {
        appInfoStore.put(
          kyuubiUniqueKey,
          kubernetesInfo -> ApplicationInfo(
            id = pod.getMetadata.getLabels.get(SPARK_APP_ID_LABEL),
            name = pod.getMetadata.getName,
            state = appState,
            error = appError))
      }
    }
  }

  private def updateApplicationUrl(kubernetesInfo: KubernetesInfo, svc: Service): Unit = {
    svc.getSpec.getPorts.asScala.find(_.getName == SPARK_UI_PORT_NAME).map(_.getPort).map {
      sparkUiPort =>
        val appUrlPattern = kyuubiConf.get(KyuubiConf.KUBERNETES_SPARK_APP_URL_PATTERN)
        val sparkAppId = svc.getSpec.getSelector.get(SPARK_APP_ID_LABEL)
        val sparkDriverSvc = svc.getMetadata.getName
        val kubernetesNamespace = kubernetesInfo.namespace.getOrElse("")
        val kubernetesContext = kubernetesInfo.context.getOrElse("")
        val appUrl = buildSparkAppUrl(
          appUrlPattern,
          sparkAppId,
          sparkDriverSvc,
          kubernetesContext,
          kubernetesNamespace,
          sparkUiPort)
        debug(s"Driver Informer svc: ${svc.getMetadata.getName} app url: $appUrl")
        val kyuubiUniqueKey = svc.getSpec.getSelector.get(LABEL_KYUUBI_UNIQUE_KEY)
        appInfoStore.synchronized {
          Option(appInfoStore.get(kyuubiUniqueKey)).foreach { case (_, appInfo) =>
            appInfoStore.put(kyuubiUniqueKey, kubernetesInfo -> appInfo.copy(url = Some(appUrl)))
          }
        }
    }.getOrElse(warn(s"Spark UI port not found in service ${svc.getMetadata.getName}"))
  }

  private def markApplicationTerminated(pod: Pod): Unit = synchronized {
    val key = pod.getMetadata.getLabels.get(LABEL_KYUUBI_UNIQUE_KEY)
    if (cleanupTerminatedAppInfoTrigger.getIfPresent(key) == null) {
      cleanupTerminatedAppInfoTrigger.put(
        key,
        toApplicationState(pod, appStateSource, appStateContainer))
    }
  }

  private def deletePod(
      kubernetesInfo: KubernetesInfo,
      podName: String,
      podLabelUniqueKey: String): Unit = {
    try {
      val kubernetesClient = getOrCreateKubernetesClient(kubernetesInfo)
      val deleted = if (podName == null) {
        !kubernetesClient.pods()
          .withLabel(LABEL_KYUUBI_UNIQUE_KEY, podLabelUniqueKey)
          .delete().isEmpty
      } else {
        !kubernetesClient.pods().withName(podName).delete().isEmpty
      }
      if (deleted) {
        info(s"[$kubernetesInfo] Operation of delete pod $podName with" +
          s" ${toLabel(podLabelUniqueKey)} is completed.")
      } else {
        warn(s"[$kubernetesInfo] Failed to delete pod $podName with ${toLabel(podLabelUniqueKey)}.")
      }
    } catch {
      case NonFatal(e) => error(
          s"[$kubernetesInfo] Failed to delete pod $podName with ${toLabel(podLabelUniqueKey)}",
          e)
    }
  }

  private def checkPodAppCanceled(kubernetesInfo: KubernetesInfo, pod: Pod): Unit = {
    if (kyuubiConf.isRESTEnabled) {
      cleanupCanceledAppPodExecutor.submit(new Runnable {
        override def run(): Unit = Utils.tryLogNonFatalError {
          val kyuubiUniqueKey = pod.getMetadata.getLabels.get(LABEL_KYUUBI_UNIQUE_KEY)
          val batch = metadataManager.flatMap(_.getBatchSessionMetadata(kyuubiUniqueKey))
          if (batch.map(_.state).map(OperationState.withName)
              .exists(_ == OperationState.CANCELED)) {
            warn(s"[$kubernetesInfo] Batch[$kyuubiUniqueKey] is canceled, " +
              s"try to delete the pod ${pod.getMetadata.getName}")
            deletePod(kubernetesInfo, pod.getMetadata.getName, kyuubiUniqueKey)
          }
        }
      })
    }
  }
}

object KubernetesApplicationOperation extends Logging {
  val LABEL_KYUUBI_UNIQUE_KEY = "kyuubi-unique-tag"
  val SPARK_APP_ID_LABEL = "spark-app-selector"
  val KUBERNETES_SERVICE_HOST = "KUBERNETES_SERVICE_HOST"
  val KUBERNETES_SERVICE_PORT = "KUBERNETES_SERVICE_PORT"
  val SPARK_UI_PORT_NAME = "spark-ui"

  def toLabel(tag: String): String = s"label: $LABEL_KYUUBI_UNIQUE_KEY=$tag"

  def toApplicationState(
      pod: Pod,
      appStateSource: KubernetesApplicationStateSource,
      appStateContainer: String): ApplicationState = {
    toApplicationStateAndError(pod, appStateSource, appStateContainer)._1
  }

  def toApplicationStateAndError(
      pod: Pod,
      appStateSource: KubernetesApplicationStateSource,
      appStateContainer: String): (ApplicationState, Option[String]) = {
    val podName = pod.getMetadata.getName
    val containerStatusToBuildAppState = appStateSource match {
      case KubernetesApplicationStateSource.CONTAINER =>
        pod.getStatus.getContainerStatuses.asScala
          .find(cs => appStateContainer.equalsIgnoreCase(cs.getName))
      case KubernetesApplicationStateSource.POD => None
    }

    val podAppState = podStateToApplicationState(pod.getStatus.getPhase)
    val containerAppState = containerStatusToBuildAppState
      .map(_.getState)
      .map(containerStateToApplicationState)

    // When the pod app state is terminated, the container app state will be ignored
    val applicationState = if (ApplicationState.isTerminated(podAppState)) {
      podAppState
    } else {
      containerAppState.getOrElse(podAppState)
    }
    val applicationError =
      if (ApplicationState.isFailed(applicationState)) {
        val errorMap = containerStatusToBuildAppState.map { cs =>
          Map(
            "Pod" -> podName,
            "PodStatus" -> pod.getStatus,
            "Container" -> appStateContainer,
            "ContainerStatus" -> cs)
        }.getOrElse {
          Map("Pod" -> podName, "PodStatus" -> pod.getStatus)
        }
        Some(JsonUtils.toPrettyJson(errorMap.asJava))
      } else {
        None
      }
    applicationState -> applicationError
  }

  def containerStateToApplicationState(containerState: ContainerState): ApplicationState = {
    // https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#container-states
    if (containerState.getWaiting != null) {
      PENDING
    } else if (containerState.getRunning != null) {
      RUNNING
    } else if (containerState.getTerminated == null) {
      UNKNOWN
    } else if (containerState.getTerminated.getExitCode == 0) {
      FINISHED
    } else {
      FAILED
    }
  }

  def podStateToApplicationState(podState: String): ApplicationState = podState match {
    // https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase
    case "Pending" => PENDING
    case "Running" => RUNNING
    case "Succeeded" => FINISHED
    case "Failed" | "Error" => FAILED
    case "OOMKilled" => KILLED
    case "Unknown" => UNKNOWN
    case _ =>
      warn(s"The spark driver pod state: $podState is not supported, " +
        "mark the application state as UNKNOWN.")
      UNKNOWN
  }

  /**
   * Replaces all the {{SPARK_APP_ID}} occurrences with the Spark App Id,
   * {{SPARK_DRIVER_SVC}} occurrences with the Spark Driver Service name,
   * {{KUBERNETES_CONTEXT}} occurrences with the Kubernetes Context,
   * {{KUBERNETES_NAMESPACE}} occurrences with the Kubernetes Namespace,
   * and {{SPARK_UI_PORT}} occurrences with the Spark UI Port.
   */
  private[kyuubi] def buildSparkAppUrl(
      sparkAppUrlPattern: String,
      sparkAppId: String,
      sparkDriverSvc: String,
      kubernetesContext: String,
      kubernetesNamespace: String,
      sparkUiPort: Int): String = {
    sparkAppUrlPattern
      .replace("{{SPARK_APP_ID}}", sparkAppId)
      .replace("{{SPARK_DRIVER_SVC}}", sparkDriverSvc)
      .replace("{{KUBERNETES_CONTEXT}}", kubernetesContext)
      .replace("{{KUBERNETES_NAMESPACE}}", kubernetesNamespace)
      .replace("{{SPARK_UI_PORT}}", sparkUiPort.toString)
  }
}
