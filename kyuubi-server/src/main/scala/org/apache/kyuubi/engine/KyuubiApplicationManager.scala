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
import java.util.{Locale, ServiceLoader}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.kyuubi.{KyuubiException, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.KubernetesApplicationOperation.LABEL_KYUUBI_UNIQUE_KEY
import org.apache.kyuubi.engine.flink.FlinkProcessBuilder
import org.apache.kyuubi.engine.spark.SparkProcessBuilder
import org.apache.kyuubi.service.AbstractService

class KyuubiApplicationManager extends AbstractService("KyuubiApplicationManager") {

  // TODO: maybe add a configuration is better
  private val operations = {
    ServiceLoader.load(classOf[ApplicationOperation], Utils.getContextOrKyuubiClassLoader)
      .iterator().asScala.toSeq
  }

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

  def killApplication(resourceManager: Option[String], tag: String): KillResponse = {
    var (killed, lastMessage): KillResponse = (false, null)
    for (operation <- operations if !killed) {
      if (operation.isSupported(resourceManager)) {
        val (k, m) = operation.killApplicationByTag(tag)
        killed = k
        lastMessage = m
      }
    }

    val finalMessage =
      if (lastMessage == null) {
        s"No ${classOf[ApplicationOperation]} Service found in ServiceLoader" +
          s" for $resourceManager"
      } else {
        lastMessage
      }
    (killed, finalMessage)
  }

  def getApplicationInfo(
      clusterManager: Option[String],
      tag: String): Option[ApplicationInfo] = {
    val operation = operations.find(_.isSupported(clusterManager))
    operation match {
      case Some(op) => Some(op.getApplicationInfoByTag(tag))
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

  private def setupSparkK8sTag(tag: String, conf: KyuubiConf): Unit = {
    conf.set("spark.kubernetes.driver.label." + LABEL_KYUUBI_UNIQUE_KEY, tag)
  }

  private def setupFlinkK8sTag(tag: String, conf: KyuubiConf): Unit = {
    val originalTag = conf.getOption(FlinkProcessBuilder.TAG_KEY).map(_ + ",").getOrElse("")
    val newTag = s"${originalTag}KYUUBI" + Some(tag).filterNot(_.isEmpty).map("," + _).getOrElse("")
    conf.set(FlinkProcessBuilder.TAG_KEY, newTag)
  }

  private[kyuubi] def checkApplicationAccessPath(path: String, conf: KyuubiConf): Unit = {
    val localDirAllowList = conf.get(KyuubiConf.SESSION_LOCAL_DIR_ALLOW_LIST)
    if (localDirAllowList.nonEmpty) {
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
      case ("FLINK", _) =>
        // running flink on other platforms is not yet supported
        setupFlinkK8sTag(applicationTag, conf)
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
}
