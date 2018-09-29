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

package yaooqinn.kyuubi.server

import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.hadoop.minikdc.MiniKdc
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{KyuubiConf, KyuubiSparkUtil, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.scalatest.BeforeAndAfterEach

import yaooqinn.kyuubi.service.{ServiceException, State}
import yaooqinn.kyuubi.utils.ReflectUtils

class KyuubiServerSuite extends SparkFunSuite with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    System.setProperty(KyuubiConf.FRONTEND_BIND_PORT.key, "0")
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    System.clearProperty(KyuubiConf.FRONTEND_BIND_PORT.key)
    super.afterEach()
  }

  test("validate spark requirements for kyuubi") {
    KyuubiServer.validate()
    val oldVersion = KyuubiSparkUtil.SPARK_VERSION
    val version = "1.6.3"
    ReflectUtils.setFieldValue(KyuubiSparkUtil, "SPARK_VERSION", version)
    assert(KyuubiSparkUtil.SPARK_VERSION === version)
    val e = intercept[ServiceException](KyuubiServer.validate())
    assert(e.getMessage.startsWith(version))
    ReflectUtils.setFieldValue(KyuubiSparkUtil, "SPARK_VERSION", oldVersion)
    assert(KyuubiSparkUtil.SPARK_VERSION === oldVersion)

  }

  test("init KyuubiServer") {
    val conf = new SparkConf(true).set(KyuubiConf.FRONTEND_BIND_PORT.key, "0")
    KyuubiSparkUtil.setupCommonConfig(conf)
    val server = new KyuubiServer()
    server.init(conf)
    assert(server.getServiceState === State.INITED)
    assert(server.feService !== null)
    assert(server.beService !== null)
    assert(server.beService.getSessionManager !== null)
    assert(server.beService.getSessionManager.getOperationMgr !== null)
    assert(server.getStartTime === 0)
    server.start()
    assert(server.getServices.nonEmpty)
    assert(server.getStartTime !== 0)
    assert(server.getServiceState === State.STARTED)
    assert(ReflectUtils.getFieldValue(server, "started").asInstanceOf[AtomicBoolean].get)
    server.stop()
    assert(server.getServiceState === State.STOPPED)
    assert(!ReflectUtils.getFieldValue(server, "started").asInstanceOf[AtomicBoolean].get)
  }

  test("start kyuubi server") {
    val server = KyuubiServer.startKyuubiServer()
    assert(server.getServiceState === State.STARTED)
    val conf = server.getConf
    KyuubiConf.getAllDefaults.filter(_._1 != KyuubiConf.FRONTEND_BIND_PORT.key)
      .foreach { case (k, v) =>
        assert(conf.get(k) === v)
      }
    assert(server.feService.getServiceState === State.STARTED)
    assert(server.beService.getServiceState === State.STARTED)
    server.stop()

    try {
      System.setProperty(KyuubiConf.HA_ENABLED.key, "true")
      intercept[IllegalArgumentException](KyuubiServer.startKyuubiServer())
      System.setProperty(KyuubiConf.HA_MODE.key, "failover")
      intercept[IllegalArgumentException](KyuubiServer.startKyuubiServer())
    } finally {
      System.clearProperty(KyuubiConf.HA_ENABLED.key)
      System.clearProperty(KyuubiConf.HA_MODE.key)
    }
  }

  test("disable fs caches for secured cluster") {

    var kdc: MiniKdc = null
    val baseDir = KyuubiSparkUtil.createTempDir(namePrefix = "kyuubi-kdc")
    try {
      val kdcConf = MiniKdc.createConf()
      kdcConf.setProperty(MiniKdc.INSTANCE, "KyuubiKrbServer")
      kdcConf.setProperty(MiniKdc.ORG_NAME, "KYUUBI")
      kdcConf.setProperty(MiniKdc.ORG_DOMAIN, "COM")

      if (kdc == null) {
        kdc = new MiniKdc(kdcConf, baseDir)
        kdc.start()
      }
    } catch {
      case e: IOException =>
        throw new AssertionError("unable to create temporary directory: " + e.getMessage)
    }

    assert(!UserGroupInformation.isSecurityEnabled)
    val conf = new SparkConf(true)
    val authType = "spark.hadoop.hadoop.security.authentication"
    conf.set(authType, "KERBEROS")
    System.setProperty("java.security.krb5.realm", kdc.getRealm)
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    UserGroupInformation.setConfiguration(hadoopConf)
    assert(UserGroupInformation.isSecurityEnabled)
    KyuubiSparkUtil.setupCommonConfig(conf)
    assert(conf.contains(KyuubiSparkUtil.HDFS_CLIENT_CACHE))
    assert(conf.get(KyuubiSparkUtil.HDFS_CLIENT_CACHE) === "true")
    assert(conf.get(KyuubiSparkUtil.HDFS_CLIENT_CACHE) === "true")
    System.clearProperty("java.security.krb5.realm")
    if (kdc !== null) {
      kdc.stop()
    }
    conf.remove(authType)
    UserGroupInformation.setConfiguration(SparkHadoopUtil.get.newConfiguration(conf))
    assert(!UserGroupInformation.isSecurityEnabled)
  }
}
