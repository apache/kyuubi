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

import java.rmi.UnexpectedException

import io.fabric8.kubernetes.api.model.{Pod, PodList}
import io.fabric8.kubernetes.client.{Config, DefaultKubernetesClient, KubernetesClient, KubernetesClientException}
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.KUBERNETES_CONTEXT
import org.apache.kyuubi.engine.ApplicationOperation.{APP_ERROR_KEY, APP_ID_KEY, APP_STATE_KEY}
import org.apache.kyuubi.engine.KubernetesApplicationOperation._

class KubernetesApplicationOperation extends ApplicationOperation with Logging {

  @volatile
  private var kubernetesClient: KubernetesClient = _

  override def initialize(conf: KyuubiConf): Unit = {
    info("Start Initialize Kubernetes Client.")
    val contextOpt = conf.get(KUBERNETES_CONTEXT)
    if (contextOpt.isEmpty) {
      warn("Skip Initialize Kubernetes Client, because of Context not set.")
      return
    }
    try {
      kubernetesClient = new DefaultKubernetesClient(Config.autoConfigure(contextOpt.get))
      info(s"Initialized Kubernetes Client connect to: ${kubernetesClient.getMasterUrl}")
    } catch {
      case e: KubernetesClientException =>
        error("Fail to init KubernetesClient for KubernetesApplicationOperation", e)
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
        val pod = operation.list().getItems.get(0)
        (
          operation.delete(),
          s"Operation of deleted appId: " +
            s"${pod.getMetadata.getLabels.get(LABEL_SPARK_APP_SELECTOR)} is complete")
      } catch {
        case e: Exception =>
          (false, s"Failed to terminate application with $tag, due to ${e.getMessage}")
      }
    } else {
      throw new IllegalStateException("Methods initialize and isSupported must be called ahead")
    }
  }

  override def getApplicationInfoByTag(tag: String): Map[String, String] = {
    if (kubernetesClient != null) {
      debug(s"Getting application info from Kubernetes cluster by $tag tag")
      try {
        val operation = findDriverPodByTag(tag)
        val pod = operation.list().getItems.get(0)
        val res = Map(
          APP_ID_KEY -> pod.getMetadata.getLabels.get(LABEL_SPARK_APP_SELECTOR),
          APP_DRIVER_NAME -> pod.getMetadata.getName,
          APP_DRIVER_NAMESPACE -> pod.getMetadata.getNamespace,
          APP_STATE_KEY -> pod.getStatus.getMessage,
          APP_ERROR_KEY -> pod.getStatus.getReason)
        debug(s"Successfully got application info by $tag: " + res.mkString(", "))
        res
      } catch {
        case e: Exception =>
          error(s"Failed to get application with $tag, due to ${e.getMessage}")
          null
      }
    } else {
      throw new IllegalStateException("Methods initialize and isSupported must be called ahead")
    }
  }

  private def findDriverPodByTag(tag: String): FilterWatchListDeletable[Pod, PodList] = {
    val operation = kubernetesClient.pods()
      .withLabel(LABEL_KYUUBI_UNIQUE_KEY, tag)
      .withLabel(LABEL_SPARK_ROLE, LABEL_SPARK_ROLE_DRIVER)
    val size = operation.list().getItems.size()
    if (size == 1) {
      operation
    } else {
      throw new UnexpectedException(s"Get Tag: ${tag} Driver Pod size: ${size}, we expect 1")
    }
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

object KubernetesApplicationOperation {
  val LABEL_SPARK_ROLE = "spark-role"
  val LABEL_SPARK_APP_SELECTOR = "spark-app-selector"
  val LABEL_SPARK_ROLE_DRIVER = "driver"
  val LABEL_KYUUBI_UNIQUE_KEY = "kyuubi-unique-tag"

  val APP_DRIVER_NAME = "driverName"
  val APP_DRIVER_NAMESPACE = "namespace"
}