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

package org.apache.kyuubi

import java.io.File
import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.google.common.io.Files
import org.apache.commons.lang.StringEscapeUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import org.scalactic.source.Position
import org.scalatest.Tag

trait WithMiniYarnCluster extends KyuubiFunSuite {
  private var yarnCluster: MiniYARNCluster = _
  protected val hadoopConfDir: File
  private var isBindSuccessful = true

  def newYarnConfig(): YarnConfiguration = {
    new YarnConfiguration()
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)
                             (implicit pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      assume(isBindSuccessful, "Mini Yarn cluster should be able to bind.")
      testFun
    }
  }

  lazy val yarnClusterConfig: Map[String, String] = {
    val hostName = InetAddress.getLocalHost.getHostName
    yarnCluster.getConfig.iterator().asScala.map { kv =>
      kv.getKey -> kv.getValue.replaceAll(hostName, "localhost")
    }.toMap
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Disable the disk utilization check to avoid the test hanging when people's disks are
    // getting full.
    val yarnConf = newYarnConfig()
    yarnConf.set("yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage",
      "100.0")

    // capacity-scheduler.xml is missing in hadoop-client-minicluster so this is a workaround
    yarnConf.set("yarn.scheduler.capacity.root.queues", "default")
    yarnConf.setInt("yarn.scheduler.capacity.root.default.capacity", 100)
    yarnConf.setFloat("yarn.scheduler.capacity.root.default.user-limit-factor", 1)
    yarnConf.setInt("yarn.scheduler.capacity.root.default.maximum-capacity", 100)
    yarnConf.set("yarn.scheduler.capacity.root.default.state", "RUNNING")
    yarnConf.set("yarn.scheduler.capacity.root.default.acl_submit_applications", "*")
    yarnConf.set("yarn.scheduler.capacity.root.default.acl_administer_queue", "*")
    yarnConf.setInt("yarn.scheduler.capacity.node-locality-delay", -1)

    // Set bind host to localhost to avoid java.net.BindException
    yarnConf.set("yarn.resourcemanager.bind-host", "localhost")

    // enable proxy
    val currentUser = UserGroupInformation.getCurrentUser.getShortUserName
    yarnConf.set(s"hadoop.proxyuser.$currentUser.groups", "*")
    yarnConf.set(s"hadoop.proxyuser.$currentUser.hosts", "*")

    try {
      yarnCluster = new MiniYARNCluster(getClass().getName(), 1, 1, 1)
      yarnCluster.init(yarnConf)
      yarnCluster.start()
    } catch {
      case e: Throwable if org.apache.commons.lang3.exception.ExceptionUtils.indexOfThrowable(
        e, classOf[java.net.BindException]) != -1 =>
        isBindSuccessful = false
        return
    }

    val config = yarnCluster.getConfig()
    val startTimeNs = System.nanoTime()
    while (config.get(YarnConfiguration.RM_ADDRESS).split(":")(1) == "0") {
      if (System.nanoTime() - startTimeNs > TimeUnit.SECONDS.toNanos(10)) {
        throw new IllegalStateException("Timed out waiting for RM to come up.")
      }
      debug("RM address still not set in configuration, waiting...")
      TimeUnit.MILLISECONDS.sleep(100)
    }

    info(s"RM address in configuration is ${config.get(YarnConfiguration.RM_ADDRESS)}")

    assert(hadoopConfDir.isDirectory())
    File.createTempFile("token", ".txt", hadoopConfDir)
    saveHadoopConfFiles(yarnClusterConfig)
  }

  override def afterAll(): Unit = {
    try {
      if (yarnCluster != null) yarnCluster.stop()
    } finally {
      super.afterAll()
    }
  }

  private def saveHadoopConfFiles(conf: Map[String, String]): Unit = {
    val coreSiteProperties = mutable.Map[String, String]()
    val hdfsSiteProperties = mutable.Map[String, String]()
    val yarnSiteProperties = mutable.Map[String, String]()
    val mapRedProperties = mutable.Map[String, String]()

    def writeXmlFile(name: String, properties: Map[String, String]): Unit = {
      val propertyString = properties.map { case (k, v) =>
        s"""
          |<property>
          |  <name>${k}</name>
          |  <value>${StringEscapeUtils.escapeXml(v)}</value>
          |</property>""".stripMargin
      }.mkString("\n")
      val xmlContent =
        s"""
          |<configuration>
          |$propertyString
          |</configuration>
          |""".stripMargin
      val xmlFile = new File(hadoopConfDir, name)
      Files.write(xmlContent.getBytes(StandardCharsets.UTF_8), xmlFile)
    }

    conf.foreach { case (k, v) =>
      if (k.startsWith("dfs")) {
        hdfsSiteProperties += k -> v
      } else if (k.startsWith("yarn")) {
        yarnSiteProperties +=  k -> v
      } else if (k.startsWith("mapred")) {
        mapRedProperties +=  k -> v
      } else {
        coreSiteProperties += k -> v
      }
    }

    writeXmlFile("core-site.xml", coreSiteProperties.toMap)
    writeXmlFile("hdfs-site.xml", hdfsSiteProperties.toMap)
    writeXmlFile("yarn-site.xml", yarnSiteProperties.toMap)
    writeXmlFile("mapred-site.xml", mapRedProperties.toMap)
  }
}
