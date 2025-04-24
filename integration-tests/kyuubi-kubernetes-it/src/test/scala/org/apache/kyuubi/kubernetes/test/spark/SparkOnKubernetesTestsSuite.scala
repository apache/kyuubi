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

package org.apache.kyuubi.kubernetes.test.spark

import java.util.UUID

import scala.concurrent.duration._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.net.NetUtils

import org.apache.kyuubi._
import org.apache.kyuubi.client.util.BatchUtils._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.{ApplicationInfo, ApplicationManagerInfo, ApplicationOperation, KubernetesApplicationOperation}
import org.apache.kyuubi.engine.ApplicationState.{FAILED, NOT_FOUND, RUNNING}
import org.apache.kyuubi.engine.spark.SparkProcessBuilder
import org.apache.kyuubi.kubernetes.test.MiniKube
import org.apache.kyuubi.operation.SparkQueryTests
import org.apache.kyuubi.session.KyuubiSessionManager
import org.apache.kyuubi.util.JavaUtils
import org.apache.kyuubi.util.Validator.KUBERNETES_EXECUTOR_POD_NAME_PREFIX
import org.apache.kyuubi.zookeeper.ZookeeperConf.ZK_CLIENT_PORT_ADDRESS

abstract class SparkOnKubernetesSuiteBase
  extends WithKyuubiServer with Logging with BatchTestHelper {
  private val apiServerAddress = {
    MiniKube.getKubernetesClient.getMasterUrl.toString
  }

  protected val appMgrInfo =
    ApplicationManagerInfo(Some(s"k8s://$apiServerAddress"), Some("minikube"), None)

  protected def sparkOnK8sConf: KyuubiConf = {
    // TODO Support more Spark version
    // Spark official docker image: https://hub.docker.com/r/apache/spark/tags
    KyuubiConf().set("spark.master", s"k8s://$apiServerAddress")
      .set("spark.kubernetes.container.image", "apache/spark:3.5.2")
      .set("spark.kubernetes.container.image.pullPolicy", "IfNotPresent")
      .set("spark.executor.instances", "1")
      .set("spark.executor.memory", "512M")
      .set("spark.driver.memory", "512M")
      .set("spark.kubernetes.driver.request.cores", "250m")
      .set("spark.kubernetes.executor.request.cores", "250m")
      .set(KUBERNETES_CONTEXT.key, "minikube")
      .set(FRONTEND_PROTOCOLS.key, "THRIFT_BINARY,REST")
      .set(ENGINE_INIT_TIMEOUT.key, "PT10M")
  }
}

/**
 * This test is for Kyuubi Server with Spark engine Using client deploy-mode on Kubernetes:
 *
 *                        Real World                                   Kubernetes Pod
 *  -------------------------------------------------------         ---------------------
 *  |          JDBC                                       |         |                   |
 *  |  Client  ---->  Kyuubi Server  ---->  Spark Driver  |  ---->  |  Spark Executors  |
 *  |                                                     |         |                   |
 *  -------------------------------------------------------         ---------------------
 */
class SparkClientModeOnKubernetesSuiteBase extends SparkOnKubernetesSuiteBase {
  override protected val conf: KyuubiConf = {
    sparkOnK8sConf.set("spark.submit.deployMode", "client")
  }
}

class SparkClientModeOnKubernetesSuite extends SparkClientModeOnKubernetesSuiteBase
  with SparkQueryTests {
  override protected def jdbcUrl: String = getJdbcUrl
}

/**
 * This test is for Kyuubi Server with Spark engine Using cluster deploy-mode on Kubernetes:
 *
 *               Real World                         Kubernetes Pod                Kubernetes Pod
 *  ----------------------------------          ---------------------         ---------------------
 *  |          JDBC                   |         |                   |         |                   |
 *  |  Client  ---->  Kyuubi Server   |  ---->  |    Spark Driver   |  ---->  |  Spark Executors  |
 *  |                                 |         |                   |         |                   |
 *  ----------------------------------          ---------------------         ---------------------
 */
class SparkClusterModeOnKubernetesSuiteBase
  extends SparkOnKubernetesSuiteBase with WithSimpleDFSService {
  private val localhostAddress = JavaUtils.findLocalInetAddress.getHostAddress
  private val driverTemplate =
    Thread.currentThread().getContextClassLoader.getResource("driver.yml")

  override val hadoopConf: Configuration = {
    val hdfsConf: Configuration = new Configuration()
    hdfsConf.set("dfs.namenode.rpc-bind-host", "0.0.0.0")
    hdfsConf.set("dfs.namenode.servicerpc-bind-host", "0.0.0.0")
    hdfsConf.set("dfs.datanode.hostname", localhostAddress)
    hdfsConf.set("dfs.datanode.address", s"0.0.0.0:${NetUtils.getFreeSocketPort}")
    // before HADOOP-18206 (3.4.0), HDFS MetricsLogger strongly depends on
    // commons-logging, we should disable it explicitly, otherwise, it throws
    // ClassNotFound: org.apache.commons.logging.impl.Log4JLogger
    hdfsConf.set("dfs.namenode.metrics.logger.period.seconds", "0")
    hdfsConf.set("dfs.datanode.metrics.logger.period.seconds", "0")
    // spark use 185 as userid in docker
    hdfsConf.set("hadoop.proxyuser.185.groups", "*")
    hdfsConf.set("hadoop.proxyuser.185.hosts", "*")
    hdfsConf
  }

  override protected lazy val conf: KyuubiConf = {
    sparkOnK8sConf.set("spark.submit.deployMode", "cluster")
      .set("spark.kubernetes.file.upload.path", s"hdfs://$localhostAddress:$getDFSPort/spark")
      .set("spark.hadoop.dfs.client.use.datanode.hostname", "true")
      .set("spark.kubernetes.authenticate.driver.serviceAccountName", "spark")
      .set("spark.kubernetes.driver.podTemplateFile", driverTemplate.getPath)
      .set(ZK_CLIENT_PORT_ADDRESS.key, localhostAddress)
      .set(FRONTEND_THRIFT_BINARY_BIND_HOST.key, localhostAddress)
  }
}

class SparkClusterModeOnKubernetesSuite
  extends SparkClusterModeOnKubernetesSuiteBase with SparkQueryTests {
  override protected def jdbcUrl: String = getJdbcUrl
}

// [KYUUBI #4467] KubernetesApplicationOperator doesn't support client mode
class KyuubiOperationKubernetesClusterClientModeSuite
  extends SparkClientModeOnKubernetesSuiteBase {
  private lazy val k8sOperation: KubernetesApplicationOperation = {
    val operation = new KubernetesApplicationOperation
    operation.initialize(conf, None)
    operation
  }

  private def sessionManager: KyuubiSessionManager =
    server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]

  ignore("Spark Client Mode On Kubernetes Kyuubi KubernetesApplicationOperation Suite") {
    val batchRequest = newSparkBatchRequest(conf.getAll ++ Map(
      KYUUBI_BATCH_ID_KEY -> UUID.randomUUID().toString))

    val sessionHandle = sessionManager.openBatchSession(
      "kyuubi",
      "passwd",
      "localhost",
      batchRequest)

    eventually(timeout(3.minutes), interval(50.milliseconds)) {
      val state = k8sOperation.getApplicationInfoByTag(
        appMgrInfo,
        sessionHandle.identifier.toString)
      assert(state.id != null)
      assert(state.name != null)
      assert(state.state == RUNNING)
    }

    val killResponse = k8sOperation.killApplicationByTag(
      appMgrInfo,
      sessionHandle.identifier.toString)
    assert(killResponse._1)
    assert(killResponse._2 startsWith "Succeeded to terminate:")

    val appInfo = k8sOperation.getApplicationInfoByTag(
      appMgrInfo,
      sessionHandle.identifier.toString)
    assert(appInfo == ApplicationInfo(null, null, NOT_FOUND))

    val failKillResponse = k8sOperation.killApplicationByTag(
      appMgrInfo,
      sessionHandle.identifier.toString)
    assert(!failKillResponse._1)
    assert(failKillResponse._2 === ApplicationOperation.NOT_FOUND)
  }
}

class KyuubiOperationKubernetesClusterClusterModeSuite
  extends SparkClusterModeOnKubernetesSuiteBase {
  private lazy val k8sOperation: KubernetesApplicationOperation = {
    val operation = new KubernetesApplicationOperation
    operation.initialize(conf, None)
    operation
  }

  private def sessionManager: KyuubiSessionManager =
    server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]

  test("Check if spark.kubernetes.executor.podNamePrefix is invalid") {
    Seq("_123", "spark_exec", "spark@", "a" * 238).foreach { invalid =>
      conf.set(KUBERNETES_EXECUTOR_POD_NAME_PREFIX, invalid)
      val builder = new SparkProcessBuilder("test", true, conf)
      val e = intercept[KyuubiException](builder.validateConf())
      assert(e.getMessage === s"'$invalid' in spark.kubernetes.executor.podNamePrefix is" +
        s" invalid. must conform https://kubernetes.io/docs/concepts/overview/" +
        "working-with-objects/names/#dns-subdomain-names and the value length <= 237")
    }
    // clean test conf
    conf.unset(KUBERNETES_EXECUTOR_POD_NAME_PREFIX)
  }

  test("Spark Cluster Mode On Kubernetes Kyuubi KubernetesApplicationOperation Suite") {
    val driverPodNamePrefix = "kyuubi-spark-driver"
    conf.set(
      "spark.kubernetes.driver.pod.name",
      driverPodNamePrefix + "-" + System.currentTimeMillis())

    val batchRequest = newSparkBatchRequest(conf.getAll ++ Map(
      KYUUBI_BATCH_ID_KEY -> UUID.randomUUID().toString))

    val sessionHandle = sessionManager.openBatchSession(
      "runner",
      "passwd",
      "localhost",
      batchRequest)

    // wait for driver pod start
    eventually(timeout(3.minutes), interval(5.second)) {
      // trigger k8sOperation init here
      val appInfo = k8sOperation.getApplicationInfoByTag(
        appMgrInfo,
        sessionHandle.identifier.toString)
      assert(appInfo.state == RUNNING)
      assert(appInfo.name.startsWith(driverPodNamePrefix))
    }

    val killResponse = k8sOperation.killApplicationByTag(
      appMgrInfo,
      sessionHandle.identifier.toString)
    assert(killResponse._1)
    assert(killResponse._2 endsWith "is completed")
    assert(killResponse._2 contains sessionHandle.identifier.toString)

    eventually(timeout(3.minutes), interval(50.milliseconds)) {
      val appInfo = k8sOperation.getApplicationInfoByTag(
        appMgrInfo,
        sessionHandle.identifier.toString)
      // We may kill engine start but not ready
      // An EOF Error occurred when the driver was starting
      assert(appInfo.state == FAILED || appInfo.state == NOT_FOUND)
    }

    val failKillResponse = k8sOperation.killApplicationByTag(
      appMgrInfo,
      sessionHandle.identifier.toString)
    assert(!failKillResponse._1)
  }
}
