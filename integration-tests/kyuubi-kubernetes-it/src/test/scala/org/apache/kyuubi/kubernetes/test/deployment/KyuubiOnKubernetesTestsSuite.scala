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

package org.apache.kyuubi.kubernetes.test.deployment

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.hadoop.net.NetUtils

import org.apache.kyuubi.{Utils, WithSimpleDFSService}
import org.apache.kyuubi.config.KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_HOST
import org.apache.kyuubi.kubernetes.test.WithKyuubiServerOnKubernetes
import org.apache.kyuubi.operation.SparkQueryTests
import org.apache.kyuubi.zookeeper.ZookeeperConf.ZK_CLIENT_PORT_ADDRESS

/**
 * This test is for Kyuubi Server on Kubernetes with Spark engine local deploy-mode:
 *
 *   Real World                              Kubernetes Pod
 *  ------------         -----------------------------------------------------
 *  |          |  JDBC   |                                                   |
 *  |  Client  |  ---->  |  Kyuubi Server  ---->  Spark Engine (local mode)  |
 *  |          |         |                                                   |
 *  ------------         -----------------------------------------------------
 */
class KyuubiOnKubernetesWithLocalSparkTestsSuite extends WithKyuubiServerOnKubernetes {
  override protected def connectionConf: Map[String, String] = {
    super.connectionConf ++ Map("spark.master" -> "local", "spark.executor.instances" -> "1")
  }
//
//  override protected def jdbcUrl: String = getJdbcUrl(connectionConf)
//
//  override protected lazy val user: String = "local"
}

class KyuubiOnKubernetesWithSparkTestsBase extends WithKyuubiServerOnKubernetes {
  override protected def connectionConf: Map[String, String] = {
    super.connectionConf ++
      Map(
        "spark.master" -> s"k8s://$getMiniKubeApiMaster",
        "spark.kubernetes.container.image" -> "apache/spark:3.3.1",
        "spark.executor.memory" -> "512M",
        "spark.driver.memory" -> "512M",
        "spark.kubernetes.driver.request.cores" -> "250m",
        "spark.kubernetes.executor.request.cores" -> "250m",
        "spark.executor.instances" -> "1")
  }
}

/**
 * This test is for Kyuubi Server on Kubernetes with Spark engine On Kubernetes client deploy-mode:
 *
 *   Real World                              Kubernetes Pod
 *  ------------       -------------------------------------------------      ---------------------
 *  |          | JDBC  |                                               |      |                   |
 *  |  Client  | ----> | Kyuubi Server  --> Spark Engine (client mode) |  --> |  Spark Executors  |
 *  |          |       |                                               |      |                   |
 *  ------------       -------------------------------------------------      ---------------------
 */
class KyuubiOnKubernetesWithClientSparkTestsSuite
  extends KyuubiOnKubernetesWithSparkTestsBase {
  override protected def connectionConf: Map[String, String] = {
    super.connectionConf ++ Map(
      "spark.submit.deployMode" -> "client",
      "spark.driver.host" -> getKyuubiServerIp,
      "kyuubi.frontend.connection.url.use.hostname" -> "false")
  }

//  override protected def jdbcUrl: String = getJdbcUrl(connectionConf)
//
//  override protected lazy val user: String = "client"
}

/**
 * This test is for Kyuubi Server on Kubernetes with Spark engine On Kubernetes client deploy-mode:
 *
 *   Real World                                   Kubernetes Pod
 *  ----------       -----------------     -----------------------------      ---------------------
 *  |        | JDBC  |               |     |                           |      |                   |
 *  | Client | ----> | Kyuubi Server | --> |Spark Engine (cluster mode)|  --> |  Spark Executors  |
 *  |        |       |               |     |                           |      |                   |
 *  ----------       -----------------     -----------------------------      ---------------------
 */
class KyuubiOnKubernetesWithClusterSparkTestsSuite
  extends KyuubiOnKubernetesWithSparkTestsBase with WithSimpleDFSService with SparkQueryTests {
  private val localhostAddress = Utils.findLocalInetAddress.getHostAddress
  private val driverTemplate =
    Thread.currentThread().getContextClassLoader.getResource("driver.yml")

  override def beforeAll(): Unit = {
    super.beforeAll()
    val fs = FileSystem.get(getHadoopConf)
    fs.mkdirs(
      new Path("/spark"),
      new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ_WRITE))
    fs.copyFromLocalFile(new Path(driverTemplate.getPath), new Path("/spark/driver.yml"))
  }

  override val hadoopConf: Configuration = {
    val hdfsConf: Configuration = new Configuration()
    hdfsConf.set("dfs.namenode.rpc-bind-host", "0.0.0.0")
    hdfsConf.set("dfs.namenode.servicerpc-bind-host", "0.0.0.0")
    hdfsConf.set("dfs.datanode.hostname", localhostAddress)
    hdfsConf.set("dfs.datanode.address", s"0.0.0.0:${NetUtils.getFreeSocketPort}")
//    // spark use 185 as userid in docker
    hdfsConf.set("hadoop.proxyuser.cluster.groups", "*")
    hdfsConf.set("hadoop.proxyuser.cluster.hosts", "*")
    hdfsConf.set("hadoop.proxyuser.kyuubi.groups", "*")
    hdfsConf.set("hadoop.proxyuser.kyuubi.hosts", "*")
    hdfsConf
  }

  override protected def connectionConf: Map[String, String] = {
    super.connectionConf ++
      Map(
        "spark.submit.deployMode" -> "cluster",
        "spark.kubernetes.file.upload.path" -> s"hdfs://$localhostAddress:$getDFSPort/spark",
        "spark.kubernetes.driver.podTemplateFile" ->
          s"hdfs://$localhostAddress:$getDFSPort/spark/driver.yml",
        "spark.hadoop.dfs.client.use.datanode.hostname" -> "true",
        "spark.kubernetes.authenticate.driver.serviceAccountName" -> "kyuubi",
        ZK_CLIENT_PORT_ADDRESS.key -> getKyuubiServerIp,
        FRONTEND_THRIFT_BINARY_BIND_HOST.key -> getKyuubiServerIp)
  }

  override protected def jdbcUrl: String = getJdbcUrl(connectionConf)

  override protected lazy val user: String = "cluster"
}
