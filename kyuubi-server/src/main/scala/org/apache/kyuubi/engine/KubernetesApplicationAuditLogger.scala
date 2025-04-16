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

import io.fabric8.kubernetes.api.model.Pod

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf.KubernetesApplicationStateSource.KubernetesApplicationStateSource
import org.apache.kyuubi.engine.KubernetesApplicationOperation.{toApplicationStateAndError, LABEL_KYUUBI_UNIQUE_KEY, SPARK_APP_ID_LABEL}
import org.apache.kyuubi.engine.KubernetesResourceEventTypes.KubernetesResourceEventType

object KubernetesApplicationAuditLogger extends Logging {
  final private val AUDIT_BUFFER = new ThreadLocal[StringBuilder]() {
    override protected def initialValue: StringBuilder = new StringBuilder()
  }

  def audit(
      eventType: KubernetesResourceEventType,
      kubernetesInfo: KubernetesInfo,
      pod: Pod,
      appStateSource: KubernetesApplicationStateSource,
      appStateContainer: String): Unit = {
    val sb = AUDIT_BUFFER.get()
    sb.setLength(0)
    sb.append("eventType=").append(eventType).append("\t")
    sb.append(s"label=${pod.getMetadata.getLabels.get(LABEL_KYUUBI_UNIQUE_KEY)}").append("\t")
    sb.append(s"context=${kubernetesInfo.context.orNull}").append("\t")
    sb.append(s"namespace=${kubernetesInfo.namespace.orNull}").append("\t")
    sb.append(s"pod=${pod.getMetadata.getName}").append("\t")
    sb.append(s"podState=${pod.getStatus.getPhase}").append("\t")
    val containerStatuses = pod.getStatus.getContainerStatuses.asScala.map { containerState =>
      s"${containerState.getName}->${containerState.getState}"
    }.mkString("[", ",", "]")
    sb.append(s"containers=$containerStatuses").append("\t")
    sb.append(s"appId=${pod.getMetadata.getLabels.get(SPARK_APP_ID_LABEL)}").append("\t")
    val (appState, appError) =
      toApplicationStateAndError(pod, appStateSource, appStateContainer, eventType)
    sb.append(s"appState=$appState").append("\t")
    sb.append(s"appError='${appError.getOrElse("")}'")
    info(sb.toString())
  }
}
