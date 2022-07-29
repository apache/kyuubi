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

import io.fabric8.kubernetes.api.model.{Pod, PodList}
import io.fabric8.kubernetes.client.{Config, DefaultKubernetesClient, KubernetesClient, KubernetesClientException}
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.KUBERNETES_CONTEXT
import org.apache.kyuubi.engine.ApplicationState.{ApplicationState, FAILED, FINISHED, PENDING, RUNNING}

class KubernetesApplicationOperation extends ApplicationOperation with Logging {

  @volatile
  private var kubernetesClient: KubernetesClient = _
  private var jpsOperation: JpsApplicationOperation = _

  override def initialize(conf: KyuubiConf): Unit = {
    info("Start Initialize Kubernetes Client.")
    val contextOpt = conf.get(KUBERNETES_CONTEXT)
    if (contextOpt.isEmpty) {
      warn("Skip Initialize Kubernetes Client, because of Context not set.")
      return
    }
    jpsOperation = new JpsApplicationOperation
    jpsOperation.initialize(conf)
    kubernetesClient =
      try {
        val client = new DefaultKubernetesClient(Config.autoConfigure(contextOpt.get))
        info(s"Initialized Kubernetes Client connect to: ${client.getMasterUrl}")
        client
      } catch {
        case e: KubernetesClientException =>
          error("Fail to init KubernetesClient for KubernetesApplicationOperation", e)
          null
      }
  }

  override def isSupported(clusterManager: Option[String]): Boolean = {
    kubernetesClient != null && clusterManager.nonEmpty &&
    clusterManager.get.toLowerCase.startsWith("k8s")
  }

  override def killApplicationByTag(tag: String): KillResponse = {
    if (kubernetesClient != null) {
      debug(s"Deleting application info from Kubernetes cluster by $tag tag")
      try {
        // Need driver only
        val operation = findDriverPodByTag(tag)
        val podList = operation.list().getItems
        if (podList.size() != 0) {
          (
            operation.delete(),
            s"Operation of deleted appId: " + s"${podList.get(0).getMetadata.getName} is completed")
        } else {
          // client mode
          return jpsOperation.killApplicationByTag(tag)
        }
      } catch {
        case e: Exception =>
          (false, s"Failed to terminate application with $tag, due to ${e.getMessage}")
      }
    } else {
      throw new IllegalStateException("Methods initialize and isSupported must be called ahead")
    }
  }

  override def getApplicationInfoByTag(tag: String): ApplicationInfo = {
    if (kubernetesClient != null) {
      debug(s"Getting application info from Kubernetes cluster by $tag tag")
      try {
        val operation = findDriverPodByTag(tag)
        val podList = operation.list().getItems
        if (podList.size() != 0) {
          val pod = podList.get(0)
          val info = ApplicationInfo(
            // Can't get appId, get Pod UID instead.
            id = pod.getMetadata.getUid,
            name = pod.getMetadata.getName,
            state = KubernetesApplicationOperation.toApplicationState(pod.getStatus.getPhase),
            error = Option(pod.getStatus.getReason))
          debug(s"Successfully got application info by $tag: $info")
          info
        } else {
          // client mode
          jpsOperation.getApplicationInfoByTag(tag)
        }
      } catch {
        case e: Exception =>
          error(s"Failed to get application with $tag, due to ${e.getMessage}")
          ApplicationInfo(id = null, name = null, ApplicationState.NOT_FOUND)
      }
    } else {
      throw new IllegalStateException("Methods initialize and isSupported must be called ahead")
    }
  }

  private def findDriverPodByTag(tag: String): FilterWatchListDeletable[Pod, PodList] = {
    val operation = kubernetesClient.pods()
      .withLabel(KubernetesApplicationOperation.LABEL_KYUUBI_UNIQUE_KEY, tag)
    val size = operation.list().getItems.size()
    if (size != 1) {
      warn(s"Get Tag: ${tag} Driver Pod In Kubernetes size: ${size}, we expect 1")
    }
    operation
  }

  override def stop(): Unit = {
    if (kubernetesClient != null) {
      try {
        kubernetesClient.close()
      } catch {
        case e: Exception => error(e.getMessage)
      }
    }
  }
}

object KubernetesApplicationOperation extends Logging {
  val LABEL_KYUUBI_UNIQUE_KEY = "kyuubi-unique-tag"

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
