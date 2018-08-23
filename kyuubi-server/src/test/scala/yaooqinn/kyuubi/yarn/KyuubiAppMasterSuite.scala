/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.yarn

import org.apache.curator.test.TestingServer
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException
import org.apache.spark.{KyuubiConf, KyuubiSparkUtil, SparkConf, SparkFunSuite}
import org.apache.spark.KyuubiConf._
import org.mockito.Mockito.{doNothing, when}
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar

import yaooqinn.kyuubi.service.State
import yaooqinn.kyuubi.utils.ReflectUtils

class KyuubiAppMasterSuite extends SparkFunSuite with MockitoSugar with Matchers {

  private val propertyFile = this.getClass.getClassLoader.getResource("kyuubi-test.conf").getPath
  private val args = Array("--properties-file", propertyFile)

  private val appMasterArguments = AppMasterArguments(args)
  private val conf = new SparkConf().set(KyuubiConf.FRONTEND_BIND_PORT.key, "0")
      .set("spark.hadoop.yarn.am.liveness-monitor.expiry-interval-ms", "0")

  override def beforeAll(): Unit = {
    appMasterArguments.propertiesFile.map(KyuubiSparkUtil.getPropertiesFromFile) match {
      case Some(props) => props.foreach { case (k, v) =>
        conf.set(k, v)
        sys.props(k) = v
      }
      case _ =>
    }
    KyuubiSparkUtil.setupCommonConfig(conf)
    super.beforeAll()
  }

  test("new application master") {
    val appMaster = new KyuubiAppMaster()
    assert(appMaster.getServiceState === State.NOT_INITED)
    assert(appMaster.getName === classOf[KyuubiAppMaster].getSimpleName)
    assert(appMaster.getStartTime === 0)
    assert(appMaster.getConf === null)
  }

  test("init application master") {
    val appMaster = new KyuubiAppMaster()
    appMaster.init(conf)
    appMaster.getServiceState should be(State.INITED)
    appMaster.getStartTime should be(0)
    appMaster.getConf should be(conf)
    appMaster.getConf.get("spark.kyuubi.test") should be("1")
  }

  test("start application master") {
    val appMaster = new KyuubiAppMaster()
    appMaster.init(conf)
    val amClient = mock[AMRMClient[AMRMClient.ContainerRequest]]
    ReflectUtils.setFieldValue(appMaster,
      "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$amRMClient", amClient)
    doNothing().when(amClient).start()
    appMaster.start()
    appMaster.getServiceState should be(State.STARTED)
    appMaster.getStartTime should not be 0
    appMaster.getConf should be(conf)
    appMaster.stop()
    appMaster.getServices.foreach( _.getServiceState should be(State.STOPPED))
  }

  test("start application master in ha mode") {
    val zkServer = new TestingServer(2181, true)

    val connectString = zkServer.getConnectString
    val conf1 = conf.clone()
    conf1.set(KyuubiConf.HA_ENABLED.key, "true")
    conf1.set(HA_ZOOKEEPER_QUORUM.key, connectString)
    conf1.set(HA_ZOOKEEPER_CONNECTION_BASESLEEPTIME.key, "100ms")
    conf1.set(HA_ZOOKEEPER_ZNODE_CREATION_TIMEOUT.key, "1s")
    conf1.set(HA_ZOOKEEPER_SESSION_TIMEOUT.key, "15s")
    conf1.set(HA_ZOOKEEPER_CONNECTION_MAX_RETRIES.key, "1")

    val appMaster = new KyuubiAppMaster()
    appMaster.init(conf1)
    val amClient = mock[AMRMClient[AMRMClient.ContainerRequest]]
    ReflectUtils.setFieldValue(appMaster,
      "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$amRMClient", amClient)
    doNothing().when(amClient).start()
    appMaster.start()
    appMaster.getServiceState should be(State.STARTED)
    appMaster.getStartTime should not be 0
    appMaster.getConf should be(conf1)
    appMaster.stop()
    appMaster.getServices.foreach( _.getServiceState should be(State.STOPPED))
    zkServer.stop()
  }

  test("heartbeat throws InterruptedException") {
    val appMaster = new KyuubiAppMaster()
    appMaster.init(conf)
    val amClient = mock[AMRMClient[AMRMClient.ContainerRequest]]
    ReflectUtils.setFieldValue(appMaster,
      "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$amRMClient", amClient)
    doNothing().when(amClient).start()
    when(amClient.allocate(0.1f)).thenThrow(classOf[InterruptedException])
    appMaster.start()
    val failureCount = "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$failureCount"
    ReflectUtils.getFieldValue(appMaster, failureCount) should be(0)
    appMaster.getServiceState should be(State.STARTED)
    appMaster.getStartTime should not be 0
    appMaster.getConf should be(conf)
  }

  test("heartbeat throws ApplicationAttemptNotFoundException") {
    val appMaster = new KyuubiAppMaster()
    appMaster.init(conf)
    val amClient = mock[AMRMClient[AMRMClient.ContainerRequest]]
    ReflectUtils.setFieldValue(appMaster,
      "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$amRMClient", amClient)
    doNothing().when(amClient).start()
    when(amClient.allocate(0.1f)).thenThrow(classOf[ApplicationAttemptNotFoundException])
    appMaster.start()
    Thread.sleep(150)
    appMaster.getServiceState should be(State.STARTED)
    appMaster.getStartTime should not be 0
    appMaster.getConf should be(conf)
    ReflectUtils.getFieldValue(appMaster, "amStatus") should be(FinalApplicationStatus.FAILED)
    val failureCount = "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$failureCount"
    ReflectUtils.getFieldValue(appMaster, failureCount) should be(1)
  }

  test("heartbeat throws other exceptions") {
    val appMaster = new KyuubiAppMaster()
    appMaster.init(conf)
    val amClient = mock[AMRMClient[AMRMClient.ContainerRequest]]
    ReflectUtils.setFieldValue(appMaster,
      "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$amRMClient", amClient)
    doNothing().when(amClient).start()
    when(amClient.allocate(0.1f)).thenThrow(classOf[Throwable])
    appMaster.start()
    Thread.sleep(500) // enough time to scheduling heartbeat task
    appMaster.getServiceState should be(State.STARTED)
    appMaster.getStartTime should not be 0
    appMaster.getConf should be(conf)
    ReflectUtils.getFieldValue(appMaster, "amStatus") should be(FinalApplicationStatus.FAILED)
    val failureCount = "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$failureCount"
    ReflectUtils.getFieldValue(appMaster, failureCount) should be(
      YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS + 2)
  }

  test("heartbeat throws fatal") {
    val appMaster = new KyuubiAppMaster()
    appMaster.init(conf)
    val amClient = mock[AMRMClient[AMRMClient.ContainerRequest]]
    ReflectUtils.setFieldValue(appMaster,
      "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$amRMClient", amClient)
    doNothing().when(amClient).start()
    when(amClient.allocate(0.1f)).thenThrow(classOf[OutOfMemoryError])
    appMaster.start()
    Thread.sleep(500)
    appMaster.getServiceState should be(State.STARTED)
    appMaster.getStartTime should not be 0
    appMaster.getConf should be(conf)
    ReflectUtils.getFieldValue(appMaster, "amStatus") should be(FinalApplicationStatus.FAILED)
    val failureCount = "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$failureCount"
    ReflectUtils.getFieldValue(appMaster, failureCount) should be(1)
  }

  test("start application master without initialization") {
    val appMaster = new KyuubiAppMaster()
    // TODO:(Kent Yao) should throw illegal state exception
    // for contain_id is null in ut
    intercept[NullPointerException](appMaster.start())
  }

}
