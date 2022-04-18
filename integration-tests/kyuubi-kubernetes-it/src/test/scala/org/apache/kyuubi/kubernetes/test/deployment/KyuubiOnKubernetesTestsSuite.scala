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
import org.apache.hadoop.net.NetUtils

import org.apache.kyuubi.{Utils, WithSimpleDFSService}
import org.apache.kyuubi.config.KyuubiConf.{FRONTEND_CONNECTION_URL_USE_HOSTNAME, FRONTEND_THRIFT_BINARY_BIND_HOST}
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
class KyuubiOnKubernetesWithLocalSparkTestsSuite extends WithKyuubiServerOnKubernetes
  with SparkQueryTests {
  override protected def setConnectionSparkConf(): Unit = {
    connectionConf += "spark.master" -> "local"
    connectionConf += "spark.executor.instances" -> "1"
  }

  override protected def jdbcUrl: String = getJdbcUrl
}

class KyuubiOnKubernetesWithSparkOnKubernetesTestsBase extends WithKyuubiServerOnKubernetes
  with SparkQueryTests {
  override protected def jdbcUrl: String = getJdbcUrl

  override def setConnectionSparkConf(): Unit = {
    connectionConf += "spark.master" -> s"k8s://$getMiniKubeApiMaster"
    connectionConf += "spark.executor.memory" -> "512M"
    connectionConf += "spark.driver.memory" -> "512M"
    connectionConf += "spark.kubernetes.driver.request.cores" -> "250m"
    connectionConf += "spark.kubernetes.executor.request.cores" -> "250m"
    connectionConf += "spark.executor.instances" -> "1"
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
class KyuubiOnKubernetesWithClientSparkOnKubernetesTestsSuite
  extends KyuubiOnKubernetesWithSparkOnKubernetesTestsBase {
  override def setConnectionSparkConf(): Unit = {
    super.setConnectionSparkConf()
    connectionConf += "spark.submit.deployMode" -> "client"
  }
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
class KyuubiOnKubernetesWithClusterSparkOnKubernetesTestsSuite
  extends KyuubiOnKubernetesWithSparkOnKubernetesTestsBase with WithSimpleDFSService {
  private val localhostAddress = Utils.findLocalInetAddress.getHostAddress
  private val driverTemplate =
    Thread.currentThread().getContextClassLoader.getResource("driver.yml")

  override val hadoopConf: Configuration = {
    val hdfsConf: Configuration = new Configuration()
    hdfsConf.set("dfs.namenode.rpc-bind-host", "0.0.0.0")
    hdfsConf.set("dfs.namenode.servicerpc-bind-host", "0.0.0.0")
    hdfsConf.set("dfs.datanode.hostname", localhostAddress)
    hdfsConf.set("dfs.datanode.address", s"0.0.0.0:${NetUtils.getFreeSocketPort}")
    // spark use 185 as userid in docker
    hdfsConf.set("hadoop.proxyuser.185.groups", "*")
    hdfsConf.set("hadoop.proxyuser.185.hosts", "*")
    hdfsConf
  }

  override def setConnectionSparkConf(): Unit = {
    super.setConnectionSparkConf()
    connectionConf += "spark.submit.deployMode" -> "client"
    connectionConf +=
      "spark.kubernetes.file.upload.path" -> s"hdfs://$localhostAddress:$getDFSPort/spark"
    connectionConf += "spark.hadoop.dfs.client.use.datanode.hostname" -> "true"
    connectionConf += "spark.kubernetes.authenticate.driver.serviceAccountName" -> "spark"
    connectionConf += "spark.kubernetes.driver.podTemplateFile" -> driverTemplate.getPath
    connectionConf += ZK_CLIENT_PORT_ADDRESS.key -> localhostAddress
    connectionConf += FRONTEND_CONNECTION_URL_USE_HOSTNAME.key -> "false"
    connectionConf += FRONTEND_THRIFT_BINARY_BIND_HOST.key -> localhostAddress
  }
}
