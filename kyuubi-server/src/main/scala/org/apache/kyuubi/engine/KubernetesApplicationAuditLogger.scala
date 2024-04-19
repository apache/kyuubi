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

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.{Pod, Service}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf.KubernetesApplicationStateSource.KubernetesApplicationStateSource
import org.apache.kyuubi.engine.KubernetesApplicationOperation.{toApplicationStateAndError, LABEL_KYUUBI_UNIQUE_KEY, SPARK_APP_ID_LABEL}

object KubernetesApplicationAuditLogger extends Logging {
  final private val AUDIT_BUFFER = new ThreadLocal[StringBuilder]() {
    override protected def initialValue: StringBuilder = new StringBuilder()
  }

  def audit(
      kubernetesInfo: KubernetesInfo,
      pod: Pod,
      applicationInfo: Option[ApplicationInfo]): Unit = {
    val sb = AUDIT_BUFFER.get()
    sb.setLength(0)
    sb.append(s"label=${pod.getMetadata.getLabels.get(LABEL_KYUUBI_UNIQUE_KEY)}").append("\t")
    sb.append(s"context=${kubernetesInfo.context.orNull}").append("\t")
    sb.append(s"namespace=${kubernetesInfo.namespace.orNull}").append("\t")
    sb.append(s"pod=${pod.getMetadata.getName}").append("\t")
    sb.append(s"podState=${pod.getStatus.getPhase}").append("\t")
    val containerStatuses = pod.getStatus.getContainerStatuses.asScala.map { containerState =>
      s"${containerState.getName}->${containerState.getState}"
    }.mkString("[", ",", "]")
    sb.append(s"containers=$containerStatuses").append("\t")
    sb.append(s"appId=${applicationInfo.map(_.id).getOrElse("")}").append("\t")
    sb.append(s"appName=${applicationInfo.map(_.name).getOrElse("")}").append("\t")
    sb.append(s"appState=${applicationInfo.map(_.state).getOrElse("")}").append("\t")
    sb.append(s"appUrl=${applicationInfo.map(_.url).getOrElse("")}").append("\t")
    sb.append(s"appError=${applicationInfo.map(_.error).getOrElse("")}")
    info(sb.toString())
  }
}
