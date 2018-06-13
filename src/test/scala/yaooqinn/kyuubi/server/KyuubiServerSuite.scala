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

import java.io.{File, IOException}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.hadoop.minikdc.MiniKdc
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{KyuubiSparkUtil, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil

import yaooqinn.kyuubi.KyuubiServerException
import yaooqinn.kyuubi.utils.ReflectUtils

class KyuubiServerSuite extends SparkFunSuite {

  test("testSetupCommonConfig") {
    val conf = new SparkConf(true).set(KyuubiSparkUtil.METASTORE_JARS, "maven")
    KyuubiServer.setupCommonConfig(conf)
    val name = "spark.app.name"
    assert(conf.get(name) === "KyuubiServer")
    assert(conf.get(KyuubiSparkUtil.SPARK_UI_PORT) === KyuubiSparkUtil.SPARK_UI_PORT_DEFAULT)
    assert(conf.get(KyuubiSparkUtil.MULTIPLE_CONTEXTS) ===
      KyuubiSparkUtil.MULTIPLE_CONTEXTS_DEFAULT)
    assert(conf.get(KyuubiSparkUtil.CATALOG_IMPL) === KyuubiSparkUtil.CATALOG_IMPL_DEFAULT)
    assert(conf.get(KyuubiSparkUtil.DEPLOY_MODE) === KyuubiSparkUtil.DEPLOY_MODE_DEFAULT)
    assert(conf.get(KyuubiSparkUtil.METASTORE_JARS) === "builtin")
    assert(conf.get(KyuubiSparkUtil.SPARK_UI_PORT) === KyuubiSparkUtil.SPARK_UI_PORT_DEFAULT)
    assert(conf.get(KyuubiSparkUtil.SPARK_LOCAL_DIR)
      .startsWith(System.getProperty("java.io.tmpdir")))
    assert(!conf.contains(KyuubiSparkUtil.HDFS_CLIENT_CACHE))
    val foo = "spark.foo"
    val e = intercept[NoSuchElementException](conf.get(foo))
    assert(e.getMessage === foo)
    val bar = "bar"
    val conf2 = new SparkConf(loadDefaults = true)
      .set(name, "test")
      .set(foo, bar)
      .set(KyuubiSparkUtil.SPARK_UI_PORT, "1234")
    KyuubiServer.setupCommonConfig(conf2)
    assert(conf.get(name) === "KyuubiServer") // app name will be overwritten
    assert(conf2.get(KyuubiSparkUtil.SPARK_UI_PORT) === KyuubiSparkUtil.SPARK_UI_PORT_DEFAULT)
    assert(conf2.get(foo) === bar)
  }

  test("testValidate") {
    KyuubiServer.validate()
    val oldVersion = KyuubiSparkUtil.SPARK_VERSION
    val version = "1.6.3"
    ReflectUtils.setFieldValue(KyuubiSparkUtil, "SPARK_VERSION", version)
    assert(KyuubiSparkUtil.SPARK_VERSION === version)
    val e = intercept[KyuubiServerException](KyuubiServer.validate())
    assert(e.getMessage.startsWith(version))
    ReflectUtils.setFieldValue(KyuubiSparkUtil, "SPARK_VERSION", oldVersion)
    assert(KyuubiSparkUtil.SPARK_VERSION === oldVersion)

  }

  test("init KyuubiServer") {
    val conf = new SparkConf(true)
    KyuubiServer.setupCommonConfig(conf)
    val server = new KyuubiServer()
    server.init(conf)
    assert(server.feService !== null)
    assert(server.beService !== null)
    assert(server.beService.getSessionManager !== null)
    assert(server.beService.getSessionManager.getOperationMgr !== null)
    server.start()
    assert(ReflectUtils.getFieldValue(server, "started").asInstanceOf[AtomicBoolean].get)
    server.stop()
    assert(!ReflectUtils.getFieldValue(server, "started").asInstanceOf[AtomicBoolean].get)
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
    conf.set("spark.hadoop.hadoop.security.authentication", "KERBEROS")
    System.setProperty("java.security.krb5.realm", kdc.getRealm)
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    UserGroupInformation.setConfiguration(hadoopConf)
    KyuubiServer.setupCommonConfig(conf)
    assert(conf.contains(KyuubiSparkUtil.HDFS_CLIENT_CACHE))
    assert(conf.get(KyuubiSparkUtil.HDFS_CLIENT_CACHE) === "true")
    assert(conf.get(KyuubiSparkUtil.HDFS_CLIENT_CACHE) === "true")
    System.clearProperty("java.security.krb5.realm")
    if (kdc !== null) {
      kdc.stop()
    }
  }
}
