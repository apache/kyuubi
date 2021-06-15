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

package org.apache.kyuubi.server


import java.io.File
import java.net.InetAddress
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import com.google.common.io.Files
import org.apache.commons.lang.StringEscapeUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.AbstractService

class MiniYarnService(name: String) extends AbstractService(name) {
  def this() = this(classOf[MiniYarnService].getSimpleName)

  private var hadoopConfDir: File = _
  private var yarnConf: YarnConfiguration = _
  private var yarnCluster: MiniYARNCluster = _

  private def newYarnConfig(): YarnConfiguration = {
    val yarnConfig = new YarnConfiguration()
    // Disable the disk utilization check to avoid the test hanging when people's disks are
    // getting full.
    yarnConfig.set("yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage",
      "100.0")

    // capacity-scheduler.xml is missing in hadoop-client-minicluster so this is a workaround
    yarnConfig.set("yarn.scheduler.capacity.root.queues", "default,two_cores_queue")

    yarnConfig.setInt("yarn.scheduler.capacity.root.default.capacity", 100)
    yarnConfig.setFloat("yarn.scheduler.capacity.root.default.user-limit-factor", 1)
    yarnConfig.setInt("yarn.scheduler.capacity.root.default.maximum-capacity", 100)
    yarnConfig.set("yarn.scheduler.capacity.root.default.state", "RUNNING")
    yarnConfig.set("yarn.scheduler.capacity.root.default.acl_submit_applications", "*")
    yarnConfig.set("yarn.scheduler.capacity.root.default.acl_administer_queue", "*")

    yarnConfig.setInt("yarn.scheduler.capacity.root.two_cores_queue.maximum-capacity", 100)
    yarnConfig.setInt("yarn.scheduler.capacity.root.two_cores_queue.maximum-applications", 2)
    yarnConfig.setInt("yarn.scheduler.capacity.root.two_cores_queue.maximum-allocation-vcores", 2)
    yarnConfig.setFloat("yarn.scheduler.capacity.root.two_cores_queue.user-limit-factor", 1)
    yarnConfig.set("yarn.scheduler.capacity.root.two_cores_queue.acl_submit_applications", "*")
    yarnConfig.set("yarn.scheduler.capacity.root.two_cores_queue.acl_administer_queue", "*")

    yarnConfig.setInt("yarn.scheduler.capacity.node-locality-delay", -1)
    // Set bind host to localhost to avoid java.net.BindException
    yarnConfig.set("yarn.resourcemanager.bind-host", "localhost")

    // enable proxy
    val currentUser = UserGroupInformation.getCurrentUser.getShortUserName
    yarnConfig.set(s"hadoop.proxyuser.$currentUser.groups", "*")
    yarnConfig.set(s"hadoop.proxyuser.$currentUser.hosts", "*")
    yarnConfig
  }

  override def initialize(conf: KyuubiConf): Unit = {
    hadoopConfDir = Utils.createTempDir().toFile
    yarnConf = newYarnConfig()
    yarnCluster = new MiniYARNCluster(name, 1, 1, 1)
    yarnCluster.init(yarnConf)
    super.initialize(conf)
  }

  override def start(): Unit = {
    yarnCluster.start()
    val config = yarnCluster.getConfig()
    eventually(timeout(10.seconds), interval(100.milliseconds)) {
      config.get(YarnConfiguration.RM_ADDRESS).split(":")(1) != "0"
    }
    info(s"RM address in configuration is ${config.get(YarnConfiguration.RM_ADDRESS)}")
    saveHadoopConf()
    super.start()
  }

  override def stop(): Unit = {
    if (yarnCluster != null) yarnCluster.stop()
    super.stop()
  }

  def getHadoopConf(): Map[String, String] = {
    val hostName = InetAddress.getLocalHost.getHostName
    yarnCluster.getConfig.iterator().asScala.map { kv =>
      kv.getKey -> kv.getValue.replaceAll(hostName, "localhost")
    }.toMap
  }

  private def saveHadoopConf(): Unit = {
    val propertyString = getHadoopConf().map { case (k, v) =>
      s"""
         |<property>
         |  <name>$k</name>
         |  <value>${StringEscapeUtils.escapeXml(v)}</value>
         |</property>""".stripMargin
    }.mkString("\n")
    val xmlContent =
      s"""
         |<configuration>
         |$propertyString
         |</configuration>
         |""".stripMargin
    val confFile = new File(hadoopConfDir, "yarn-site.xml")
    Files.write(xmlContent.getBytes(StandardCharsets.UTF_8), confFile)
  }

  def getHadoopConfDir(): String = {
    hadoopConfDir.getAbsolutePath
  }
}
