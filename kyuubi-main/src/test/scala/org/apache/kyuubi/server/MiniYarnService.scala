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
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.AbstractService

class MiniYarnService(name: String) extends AbstractService(name) {
  def this() = this(classOf[MiniYarnService].getSimpleName)

  private var hadoopConfDir: File = _
  private var yarnConf: YarnConfiguration = _
  private var yarnCluster: MiniYARNCluster = _

  private def newYarnConfig(): YarnConfiguration = {
    val yarnConf = new YarnConfiguration()
    // Disable the disk utilization check to avoid the test hanging when people's disks are
    // getting full.
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
    yarnConf
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
    val startTimeNs = System.nanoTime()
    while (config.get(YarnConfiguration.RM_ADDRESS).split(":")(1) == "0") {
      if (System.nanoTime() - startTimeNs > TimeUnit.SECONDS.toNanos(10)) {
        throw new IllegalStateException("Timed out waiting for RM to come up.")
      }
      debug("RM address still not set in configuration, waiting...")
      TimeUnit.MILLISECONDS.sleep(100)
    }
    info(s"RM address in configuration is ${config.get(YarnConfiguration.RM_ADDRESS)}")
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

  def getHadoopConfDir(): String = {
    hadoopConfDir.getAbsolutePath
  }
}
