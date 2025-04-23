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

import java.nio.charset.StandardCharsets
import java.util.Base64

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.ApplicationState.ApplicationState
import org.apache.kyuubi.server.metadata.MetadataManager

trait ApplicationOperation {

  /**
   * Step for initializing the instance.
   */
  def initialize(conf: KyuubiConf, metadataManager: Option[MetadataManager]): Unit

  /**
   * Step to clean up the instance
   */
  def stop(): Unit

  /**
   * Called before other method to do a quick skip
   *
   * @param appMgrInfo the application manager information
   */
  def isSupported(appMgrInfo: ApplicationManagerInfo): Boolean

  /**
   * Kill the app/engine by the unique application tag
   *
   * @param appMgrInfo the application manager information
   * @param tag the unique application tag for engine instance.
   *            For example,
   *            if the Hadoop Yarn is used, for spark applications,
   *            the tag will be preset via spark.yarn.tags
   * @param proxyUser the proxy user to use for executing kill commands.
   *                  For secured YARN cluster, the Kyuubi Server's user typically
   *                  has no permission to kill the application. Admin user or
   *                  application owner should be used instead.
   * @return a message contains response describing how the kill process.
   *
   * @note For implementations, please suppress exceptions and always return KillResponse
   */
  def killApplicationByTag(
      appMgrInfo: ApplicationManagerInfo,
      tag: String,
      proxyUser: Option[String] = None): KillResponse

  /**
   * Get the engine/application status by the unique application tag
   *
   * @param appMgrInfo the application manager information
   * @param tag the unique application tag for engine instance.
   * @param submitTime engine submit to resourceManager time
   * @param proxyUser  the proxy user to use for creating YARN client
   *                   For secured YARN cluster, the Kyuubi Server's user may have no permission
   *                   to operate the application. Admin user or application owner could be used
   *                   instead.
   * @return [[ApplicationInfo]]
   */
  def getApplicationInfoByTag(
      appMgrInfo: ApplicationManagerInfo,
      tag: String,
      proxyUser: Option[String] = None,
      submitTime: Option[Long] = None): ApplicationInfo
}

object ApplicationState extends Enumeration {
  type ApplicationState = Value
  val PENDING, RUNNING, FINISHED, KILLED, FAILED, ZOMBIE, NOT_FOUND, UNKNOWN = Value

  def isFailed(state: ApplicationState): Boolean = state match {
    case FAILED => true
    case KILLED => true
    case _ => false
  }

  def isTerminated(state: ApplicationState): Boolean = {
    state match {
      case FAILED => true
      case KILLED => true
      case FINISHED => true
      case NOT_FOUND => true
      case _ => false
    }
  }
}

case class ApplicationInfo(
    id: String,
    name: String,
    state: ApplicationState,
    url: Option[String] = None,
    error: Option[String] = None) {

  def toMap: Map[String, String] = {
    Map(
      "id" -> id,
      "name" -> name,
      "state" -> state.toString,
      "url" -> url.orNull,
      "error" -> error.orNull)
  }
}

object ApplicationInfo {
  val NOT_FOUND: ApplicationInfo = ApplicationInfo(null, null, ApplicationState.NOT_FOUND)
  val UNKNOWN: ApplicationInfo = ApplicationInfo(null, null, ApplicationState.UNKNOWN)
}

object ApplicationOperation {
  val NOT_FOUND = "APPLICATION_NOT_FOUND"
}

case class KubernetesInfo(context: Option[String] = None, namespace: Option[String] = None)

case class ApplicationManagerInfo(
    resourceManager: Option[String],
    kubernetesInfo: KubernetesInfo = KubernetesInfo())

object ApplicationManagerInfo extends Logging {
  final val DEFAULT_KUBERNETES_NAMESPACE = "default"
  val mapper: ObjectMapper = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .registerModule(DefaultScalaModule)

  def apply(
      resourceManager: Option[String],
      kubernetesContext: Option[String],
      kubernetesNamespace: Option[String]): ApplicationManagerInfo = {
    new ApplicationManagerInfo(
      resourceManager,
      KubernetesInfo(kubernetesContext, kubernetesNamespace))
  }

  def serialize(appMgrInfo: ApplicationManagerInfo): String = {
    Base64.getEncoder.encodeToString(
      mapper.writeValueAsString(appMgrInfo).getBytes(StandardCharsets.UTF_8))
  }

  def deserialize(encodedStr: String): ApplicationManagerInfo = {
    try {
      val json = new String(
        Base64.getDecoder.decode(encodedStr.getBytes),
        StandardCharsets.UTF_8)
      mapper.readValue(json, classOf[ApplicationManagerInfo])
    } catch {
      case _: Throwable =>
        error(s"Fail to deserialize the encoded string: $encodedStr")
        ApplicationManagerInfo(None)
    }
  }
}
