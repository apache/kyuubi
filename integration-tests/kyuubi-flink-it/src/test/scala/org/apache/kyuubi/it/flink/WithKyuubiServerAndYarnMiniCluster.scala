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

package org.apache.kyuubi.it.flink

import java.io.{File, FileWriter}

import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.kyuubi.{KyuubiFunSuite, Utils, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.KYUUBI_ENGINE_ENV_PREFIX
import org.apache.kyuubi.server.{MiniDFSService, MiniYarnService}

trait WithKyuubiServerAndYarnMiniCluster extends KyuubiFunSuite with WithKyuubiServer {

  val kyuubiHome: String = Utils.getCodeSourceLocation(getClass).split("integration-tests").head

  override protected val conf: KyuubiConf = new KyuubiConf(false)

  protected var miniHdfsService: MiniDFSService = _

  protected var miniYarnService: MiniYarnService = _

  private val yarnConf: YarnConfiguration = {
    val yarnConfig = new YarnConfiguration()
    yarnConfig.set("yarn.resource-types.<resource>.minimum-allocation", "256mb")
    yarnConfig.set("yarn.resourcemanager.webapp.address", "localhost:8088")

    // Disable the disk utilization check to avoid the test hanging when people's disks are
    // getting full.
    yarnConfig.set(
      "yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage",
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

    yarnConfig
  }

  override def beforeAll(): Unit = {
    miniHdfsService = new MiniDFSService()
    miniHdfsService.initialize(conf)
    miniHdfsService.start()
    conf.set(s"$KYUUBI_ENGINE_ENV_PREFIX.HDFS_CONF_DIR", miniHdfsService.getHadoopConfDir)

    val hdfsServiceUrl = s"hdfs://localhost:${miniHdfsService.getDFSPort}"
    yarnConf.set("fs.defaultFS", hdfsServiceUrl)
    yarnConf.addResource(miniHdfsService.getHadoopConf)

    val cp = System.getProperty("java.class.path")
    // exclude kyuubi flink engine jar that has SPI for EmbeddedExecutorFactory
    // which can't be initialized on the client side
    val hadoopJars = cp.split(":").filter(s => !s.contains("flink"))
    val hadoopClasspath = hadoopJars.mkString(":")
    yarnConf.set("yarn.application.classpath", hadoopClasspath)

    miniYarnService = new MiniYarnService()
    miniYarnService.setYarnConf(yarnConf)
    miniYarnService.initialize(conf)
    miniYarnService.start()

    val hadoopConfDir = Utils.createTempDir().toFile
    val writer = new FileWriter(new File(hadoopConfDir, "yarn-site.xml"))
    yarnConf.writeXml(writer)
    writer.close()

    conf.set(s"$KYUUBI_ENGINE_ENV_PREFIX.KYUUBI_HOME", kyuubiHome)
    conf.set(s"$KYUUBI_ENGINE_ENV_PREFIX.HADOOP_CLASSPATH", hadoopClasspath)
    conf.set(s"$KYUUBI_ENGINE_ENV_PREFIX.HADOOP_CONF_DIR", hadoopConfDir.getAbsolutePath)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (miniYarnService != null) {
      miniYarnService.stop()
      miniYarnService = null
    }
    if (miniHdfsService != null) {
      miniHdfsService.stop()
      miniHdfsService = null
    }
  }
}
