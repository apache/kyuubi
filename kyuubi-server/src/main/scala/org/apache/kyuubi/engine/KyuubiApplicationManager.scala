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

import java.io.File
import java.net.{URI, URISyntaxException}
import java.nio.file.{Files, Path}
import java.util.Locale

import scala.util.control.NonFatal

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.{KyuubiException, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.KubernetesApplicationOperation.LABEL_KYUUBI_UNIQUE_KEY
import org.apache.kyuubi.engine.flink.FlinkProcessBuilder
import org.apache.kyuubi.engine.spark.SparkProcessBuilder
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.util.reflect.ReflectUtils._

class KyuubiApplicationManager extends AbstractService("KyuubiApplicationManager") {

  // TODO: maybe add a configuration is better
  private val operations =
    loadFromServiceLoader[ApplicationOperation](Utils.getContextOrKyuubiClassLoader).toSeq

  override def initialize(conf: KyuubiConf): Unit = {
    operations.foreach { op =>
      try {
        op.initialize(conf)
      } catch {
        case NonFatal(e) => warn(s"Error starting ${op.getClass.getSimpleName}: ${e.getMessage}")
      }
    }
    super.initialize(conf)
  }

  override def stop(): Unit = {
    operations.foreach { op =>
      try {
        op.stop()
      } catch {
        case NonFatal(e) => warn(s"Error stopping ${op.getClass.getSimpleName}: ${e.getMessage}")
      }
    }
    super.stop()
  }

  def killApplication(
      appMgrInfo: ApplicationManagerInfo,
      tag: String,
      proxyUser: Option[String] = None): KillResponse = {
    var (killed, lastMessage): KillResponse = (false, null)
    for (operation <- operations if !killed) {
      if (operation.isSupported(appMgrInfo)) {
        val (k, m) = operation.killApplicationByTag(appMgrInfo, tag, proxyUser)
        killed = k
        lastMessage = m
      }
    }

    val finalMessage =
      if (lastMessage == null) {
        s"No ${classOf[ApplicationOperation]} Service found in ServiceLoader" +
          s" for $appMgrInfo"
      } else {
        lastMessage
      }
    (killed, finalMessage)
  }

  def getApplicationInfo(
      appMgrInfo: ApplicationManagerInfo,
      tag: String,
      proxyUser: Option[String] = None,
      submitTime: Option[Long] = None): Option[ApplicationInfo] = {
    val operation = operations.find(_.isSupported(appMgrInfo))
    operation match {
      case Some(op) => Some(op.getApplicationInfoByTag(appMgrInfo, tag, proxyUser, submitTime))
      case None => None
    }
  }
}

object KyuubiApplicationManager {
  private def setupSparkYarnTag(tag: String, conf: KyuubiConf): Unit = {
    val originalTag = conf.getOption(SparkProcessBuilder.TAG_KEY).map(_ + ",").getOrElse("")
    val newTag = s"${originalTag}KYUUBI" + Some(tag).filterNot(_.isEmpty).map("," + _).getOrElse("")
    conf.set(SparkProcessBuilder.TAG_KEY, newTag)
  }

  private def setupEngineYarnModeTag(tag: String, conf: KyuubiConf): Unit = {
    val originalTag =
      conf.getOption(KyuubiConf.ENGINE_DEPLOY_YARN_MODE_TAGS.key).map(_ + ",").getOrElse("")
    val newTag = s"${originalTag}KYUUBI" + Some(tag).filterNot(_.isEmpty).map("," + _).getOrElse("")
    conf.set(KyuubiConf.ENGINE_DEPLOY_YARN_MODE_TAGS.key, newTag)
  }

  private def setupSparkK8sTag(tag: String, conf: KyuubiConf): Unit = {
    conf.set("spark.kubernetes.driver.label." + LABEL_KYUUBI_UNIQUE_KEY, tag)
  }

  private def setupFlinkYarnTag(tag: String, conf: KyuubiConf): Unit = {
    val originalTag = conf
      .getOption(s"${FlinkProcessBuilder.FLINK_CONF_PREFIX}.${FlinkProcessBuilder.YARN_TAG_KEY}")
      .orElse(conf.getOption(FlinkProcessBuilder.YARN_TAG_KEY))
      .map(_ + ",").getOrElse("")
    val newTag = s"${originalTag}KYUUBI" + Some(tag).filterNot(_.isEmpty).map("," + _).getOrElse("")
    conf.set(FlinkProcessBuilder.YARN_TAG_KEY, newTag)
  }

  val uploadWorkDir: Path = {
    val path = Utils.getAbsolutePathFromWork("upload")
    val pathFile = path.toFile
    if (!pathFile.exists()) {
      Files.createDirectories(path)
    }
    path
  }

  private[kyuubi] def checkApplicationAccessPath(path: String, conf: KyuubiConf): Unit = {
    var localDirAllowList: Set[String] = conf.get(KyuubiConf.SESSION_LOCAL_DIR_ALLOW_LIST)
    if (localDirAllowList.nonEmpty) {
      localDirAllowList ++= Set(uploadWorkDir.toUri.getPath)
      val uri =
        try {
          new URI(path)
        } catch {
          case e: URISyntaxException => throw new IllegalArgumentException(e)
        }

      if (uri.getScheme == null || uri.getScheme == "file") {
        if (!uri.getPath.startsWith(File.separator)) {
          throw new KyuubiException(
            s"Relative path ${uri.getPath} is not allowed, please use absolute path.")
        }

        if (!localDirAllowList.exists(uri.getPath.startsWith(_))) {
          throw new KyuubiException(
            s"The file ${uri.getPath} to access is not in the local dir allow list" +
              s" [${localDirAllowList.mkString(",")}].")
        }
      }
    }
  }

  private def checkSparkAccessPaths(
      appConf: Map[String, String],
      kyuubiConf: KyuubiConf): Unit = {
    if (kyuubiConf.get(KyuubiConf.SESSION_LOCAL_DIR_ALLOW_LIST).nonEmpty) {
      SparkProcessBuilder.PATH_CONFIGS.flatMap { key =>
        appConf.get(key).map(_.split(",")).getOrElse(Array.empty)
      }.filter(_.nonEmpty).foreach { path =>
        checkApplicationAccessPath(path, kyuubiConf)
      }
    }
  }

  /**
   * Add a unique tag on the application
   * @param applicationTag a unique tag to identify application
   * @param applicationType application short type, e.g. SPARK_SQL SPARK_BATCH is SPARK
   * @param resourceManager yarn, kubernetes(k8s) etc.
   * @param conf  kyuubi conf instance in session layer
   */
  def tagApplication(
      applicationTag: String,
      applicationType: String,
      resourceManager: Option[String],
      conf: KyuubiConf): Unit = {
    (applicationType.toUpperCase, resourceManager.map(_.toUpperCase())) match {
      case ("SPARK", Some("YARN")) => setupSparkYarnTag(applicationTag, conf)
      case ("SPARK", Some(rm)) if rm.startsWith("K8S") => setupSparkK8sTag(applicationTag, conf)
      case ("SPARK", _) =>
        // if the master is not identified ahead, add all tags
        setupSparkYarnTag(applicationTag, conf)
        setupSparkK8sTag(applicationTag, conf)
      case ("FLINK", Some("YARN")) =>
        // running flink on other platforms is not yet supported
        setupFlinkYarnTag(applicationTag, conf)
      case ("HIVE", Some("YARN")) =>
        setupEngineYarnModeTag(applicationTag, conf)
      case ("JDBC", Some("YARN")) =>
        setupEngineYarnModeTag(applicationTag, conf)
      // other engine types are running locally yet
      case _ =>
    }
  }

  def checkApplicationAccessPaths(
      applicationType: String,
      appConf: Map[String, String],
      kyuubiConf: KyuubiConf): Unit = {
    applicationType.toUpperCase(Locale.ROOT) match {
      case appType if appType.contains("SPARK") => checkSparkAccessPaths(appConf, kyuubiConf)
      case appType if appType.startsWith("FLINK") => // TODO: check flink app access local paths
      case _ =>
    }
  }

  def sessionUploadFolderPath(sessionId: String): Path = {
    require(StringUtils.isNotBlank(sessionId))
    uploadWorkDir.resolve(sessionId)
  }
}
