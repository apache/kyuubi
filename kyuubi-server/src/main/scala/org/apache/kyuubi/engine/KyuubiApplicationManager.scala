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
import java.nio.file.{Files, Path, Paths}
import java.util.Locale

import scala.util.control.NonFatal

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{Path => HadoopPath}

import org.apache.kyuubi.{KyuubiException, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.KubernetesApplicationOperation.LABEL_KYUUBI_UNIQUE_KEY
import org.apache.kyuubi.engine.flink.FlinkProcessBuilder
import org.apache.kyuubi.engine.spark.SparkProcessBuilder
import org.apache.kyuubi.server.metadata.MetadataManager
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.util.reflect.ReflectUtils._

class KyuubiApplicationManager(metadataManager: Option[MetadataManager])
  extends AbstractService("KyuubiApplicationManager") {

  // TODO: maybe add a configuration is better
  private val operations =
    loadFromServiceLoader[ApplicationOperation](Utils.getContextOrKyuubiClassLoader).toSeq

  override def initialize(serverConf: KyuubiConf): Unit = {
    operations.foreach { op =>
      try {
        op.initialize(serverConf, metadataManager)
      } catch {
        case NonFatal(e) => warn(s"Error starting ${op.getClass.getSimpleName}: ${e.getMessage}")
      }
    }
    super.initialize(serverConf)
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

  private[kyuubi] def getKubernetesApplicationOperation: Option[KubernetesApplicationOperation] = {
    operations.find(_.isInstanceOf[KubernetesApplicationOperation])
      .map(_.asInstanceOf[KubernetesApplicationOperation])
  }

  private[kyuubi] def getApplicationOperation(appMgrInfo: ApplicationManagerInfo)
      : Option[ApplicationOperation] = {
    operations.find(_.isSupported(appMgrInfo))
  }
}

object KyuubiApplicationManager {
  private def setupSparkYarnTag(tag: String, sessionConf: KyuubiConf): Unit = {
    val originalTag = sessionConf.getOption(SparkProcessBuilder.TAG_KEY).map(_ + ",").getOrElse("")
    val newTag = s"${originalTag}KYUUBI" + Some(tag).filterNot(_.isEmpty).map("," + _).getOrElse("")
    sessionConf.set(SparkProcessBuilder.TAG_KEY, newTag)
  }

  private def setupEngineYarnModeTag(tag: String, sessionConf: KyuubiConf): Unit = {
    val originalTag =
      sessionConf.getOption(KyuubiConf.ENGINE_DEPLOY_YARN_MODE_TAGS.key).map(_ + ",").getOrElse("")
    val newTag = s"${originalTag}KYUUBI" + Some(tag).filterNot(_.isEmpty).map("," + _).getOrElse("")
    sessionConf.set(KyuubiConf.ENGINE_DEPLOY_YARN_MODE_TAGS.key, newTag)
  }

  private def setupSparkK8sTag(tag: String, sessionConf: KyuubiConf): Unit = {
    sessionConf.set("spark.kubernetes.driver.label." + LABEL_KYUUBI_UNIQUE_KEY, tag)
  }

  private def setupFlinkYarnTag(tag: String, sessionConf: KyuubiConf): Unit = {
    val originalTag = sessionConf
      .getOption(s"${FlinkProcessBuilder.FLINK_CONF_PREFIX}.${FlinkProcessBuilder.YARN_TAG_KEY}")
      .orElse(sessionConf.getOption(FlinkProcessBuilder.YARN_TAG_KEY))
      .map(_ + ",").getOrElse("")
    val newTag = s"${originalTag}KYUUBI" + Some(tag).filterNot(_.isEmpty).map("," + _).getOrElse("")
    sessionConf.set(FlinkProcessBuilder.YARN_TAG_KEY, newTag)
  }

  val uploadWorkDir: Path = {
    val path = Utils.getAbsolutePathFromWork("upload")
    val pathFile = path.toFile
    if (!pathFile.exists()) {
      Files.createDirectories(path)
    }
    path
  }

  private[kyuubi] def checkApplicationAccessPath(path: String, serverConf: KyuubiConf): Unit = {
    var localDirAllowList: Set[String] = serverConf.get(KyuubiConf.SESSION_LOCAL_DIR_ALLOW_LIST)
    if (localDirAllowList.nonEmpty) {
      localDirAllowList ++= Set(uploadWorkDir.toUri.getPath)
      val uri =
        try {
          new URI(path)
        } catch {
          case e: URISyntaxException => throw new IllegalArgumentException(e)
        }

      if (uri.getScheme == null || uri.getScheme.equalsIgnoreCase("file")) {
        if (!uri.getPath.startsWith(File.separator)) {
          throw new KyuubiException(
            s"Relative path ${uri.getPath} is not allowed, please use absolute path.")
        }

        // Hadoop treats '?' and '#' as path characters instead of URI delimiters, so
        // validate both interpretations to prevent traversal through query or fragment.
        val normalizedPaths = Seq(uri.getPath, new HadoopPath(path).toUri.getPath)
          .map(Paths.get(_).normalize)
        if (!normalizedPaths.forall(p => localDirAllowList.exists(p.startsWith(_)))) {
          throw new KyuubiException(
            s"The file $path to access is not in the local dir allow list" +
              s" [${localDirAllowList.mkString(",")}].")
        }
      }
    }
  }

  private def checkSparkAccessPaths(
      appConf: Map[String, String],
      serverConf: KyuubiConf): Unit = {
    if (serverConf.get(KyuubiConf.SESSION_LOCAL_DIR_ALLOW_LIST).nonEmpty) {
      val policedKeys = SparkProcessBuilder.PATH_CONFIGS.toSet ++
        serverConf.get(KyuubiConf.SERVER_SPARK_FILE_CONFIG_LIST)
          .map(SparkProcessBuilder.convertConfigKey)
      appConf.foreach { case (key, value) =>
        if (policedKeys.contains(SparkProcessBuilder.convertConfigKey(key))) {
          value.split(",").filter(_.nonEmpty).foreach { path =>
            checkApplicationAccessPath(path, serverConf)
          }
        }
      }
      // the main resource is not a spark.* conf but spark-submit uploads it from the server
      // host as the primary resource of the engine application
      appConf.get(KyuubiConf.ENGINE_SPARK_MAIN_RESOURCE.key).filter(_.nonEmpty).foreach { path =>
        checkApplicationAccessPath(path, serverConf)
      }
    }
  }

  /**
   * Add a unique tag on the application
   * @param applicationTag a unique tag to identify application
   * @param applicationType application short type, e.g. SPARK_SQL SPARK_BATCH is SPARK
   * @param resourceManager yarn, kubernetes(k8s) etc.
   * @param sessionConf kyuubi conf instance in session layer
   */
  def tagApplication(
      applicationTag: String,
      applicationType: String,
      resourceManager: Option[String],
      sessionConf: KyuubiConf): Unit = {
    (applicationType.toUpperCase, resourceManager.map(_.toUpperCase())) match {
      case ("SPARK", Some("YARN")) => setupSparkYarnTag(applicationTag, sessionConf)
      case ("SPARK", Some(rm)) if rm.startsWith("K8S") =>
        setupSparkK8sTag(applicationTag, sessionConf)
      case ("SPARK", _) =>
        // if the master is not identified ahead, add all tags
        setupSparkYarnTag(applicationTag, sessionConf)
        setupSparkK8sTag(applicationTag, sessionConf)
      case ("FLINK", Some("YARN")) =>
        // running flink on other platforms is not yet supported
        setupFlinkYarnTag(applicationTag, sessionConf)
      case ("HIVE", Some("YARN")) =>
        setupEngineYarnModeTag(applicationTag, sessionConf)
      case ("JDBC", Some("YARN")) =>
        setupEngineYarnModeTag(applicationTag, sessionConf)
      // other engine types are running locally yet
      case _ =>
    }
  }

  def checkApplicationAccessPaths(
      applicationType: String,
      appConf: Map[String, String],
      serverConf: KyuubiConf): Unit = {
    applicationType.toUpperCase(Locale.ROOT) match {
      case appType if appType.contains("SPARK") => checkSparkAccessPaths(appConf, serverConf)
      case appType if appType.startsWith("FLINK") => // TODO: check flink app access local paths
      case _ =>
    }
  }

  def sessionUploadFolderPath(sessionId: String): Path = {
    require(StringUtils.isNotBlank(sessionId))
    uploadWorkDir.resolve(sessionId)
  }
}
