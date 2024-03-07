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
import java.nio.file.Paths

import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.kyuubi.{KyuubiFunSuite, Utils, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.KYUUBI_ENGINE_ENV_PREFIX
import org.apache.kyuubi.server.{MiniDFSService, MiniYarnService}
import org.apache.kyuubi.util.JavaUtils

trait WithKyuubiServerAndYarnMiniCluster extends KyuubiFunSuite with WithKyuubiServer {

  val kyuubiHome: String = JavaUtils.getCodeSourceLocation(getClass).split("integration-tests").head

  override protected val conf: KyuubiConf = new KyuubiConf(false)

  protected var miniHdfsService: MiniDFSService = _

  protected var miniYarnService: MiniYarnService = _

  private val yarnConf: YarnConfiguration = {
    val yarnConfig = new YarnConfiguration()

    // configurations copied from org.apache.flink.yarn.YarnTestBase
    yarnConfig.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 32)
    yarnConfig.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 4096)

    yarnConfig.setBoolean(YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME, true)
    yarnConfig.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2)
    yarnConfig.setInt(YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS, 2)
    yarnConfig.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, 4)
    yarnConfig.setInt(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 3600)
    yarnConfig.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, false)
    // memory is overwritten in the MiniYARNCluster.
    // so we have to change the number of cores for testing.
    yarnConfig.setInt(YarnConfiguration.NM_VCORES, 666)
    yarnConfig.setFloat(YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE, 99.0f)
    yarnConfig.setInt(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS, 1000)
    yarnConfig.setInt(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, 5000)

    // capacity-scheduler.xml is missing in hadoop-client-minicluster so this is a workaround
    yarnConfig.set("yarn.scheduler.capacity.root.queues", "default,four_cores_queue")

    yarnConfig.setInt("yarn.scheduler.capacity.root.default.capacity", 100)
    yarnConfig.setFloat("yarn.scheduler.capacity.root.default.user-limit-factor", 1)
    yarnConfig.setInt("yarn.scheduler.capacity.root.default.maximum-capacity", 100)
    yarnConfig.set("yarn.scheduler.capacity.root.default.state", "RUNNING")
    yarnConfig.set("yarn.scheduler.capacity.root.default.acl_submit_applications", "*")
    yarnConfig.set("yarn.scheduler.capacity.root.default.acl_administer_queue", "*")

    yarnConfig.setInt("yarn.scheduler.capacity.root.four_cores_queue.maximum-capacity", 100)
    yarnConfig.setInt("yarn.scheduler.capacity.root.four_cores_queue.maximum-applications", 10)
    yarnConfig.setInt("yarn.scheduler.capacity.root.four_cores_queue.maximum-allocation-vcores", 4)
    yarnConfig.setFloat("yarn.scheduler.capacity.root.four_cores_queue.user-limit-factor", 1)
    yarnConfig.set("yarn.scheduler.capacity.root.four_cores_queue.acl_submit_applications", "*")
    yarnConfig.set("yarn.scheduler.capacity.root.four_cores_queue.acl_administer_queue", "*")

    yarnConfig.setInt("yarn.scheduler.capacity.node-locality-delay", -1)
    // Set bind host to localhost to avoid java.net.BindException
    yarnConfig.set(YarnConfiguration.RM_BIND_HOST, "localhost")
    yarnConfig.set(YarnConfiguration.NM_BIND_HOST, "localhost")

    yarnConfig
  }

  override def beforeAll(): Unit = {
    miniHdfsService = new MiniDFSService()
    miniHdfsService.initialize(conf)
    miniHdfsService.start()

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
    val writer = new FileWriter(new File(hadoopConfDir, "core-site.xml"))
    yarnConf.writeXml(writer)
    writer.close()

    val flinkHome = {
      val candidates = Paths.get(kyuubiHome, "externals", "kyuubi-download", "target")
        .toFile.listFiles(f => f.getName.contains("flink"))
      if (candidates == null) None else candidates.map(_.toPath).headOption
    }
    if (flinkHome.isEmpty) {
      throw new IllegalStateException(s"Flink home not found in $kyuubiHome/externals")
    }

    conf.set(s"$KYUUBI_ENGINE_ENV_PREFIX.KYUUBI_HOME", kyuubiHome)
    conf.set(s"$KYUUBI_ENGINE_ENV_PREFIX.FLINK_HOME", flinkHome.get.toString)
    conf.set(
      s"$KYUUBI_ENGINE_ENV_PREFIX.FLINK_CONF_DIR",
      s"${flinkHome.get.toString}${File.separator}conf")
    conf.set(s"$KYUUBI_ENGINE_ENV_PREFIX.HADOOP_CLASSPATH", hadoopClasspath)
    conf.set(s"$KYUUBI_ENGINE_ENV_PREFIX.HADOOP_CONF_DIR", hadoopConfDir.getAbsolutePath)
    conf.set(s"flink.containerized.master.env.HADOOP_CLASSPATH", hadoopClasspath)
    conf.set(s"flink.containerized.master.env.HADOOP_CONF_DIR", hadoopConfDir.getAbsolutePath)
    conf.set(s"flink.containerized.taskmanager.env.HADOOP_CONF_DIR", hadoopConfDir.getAbsolutePath)

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
