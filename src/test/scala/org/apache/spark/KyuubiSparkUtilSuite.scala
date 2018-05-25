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

package org.apache.spark

import java.security.PrivilegedExceptionAction

import org.apache.hadoop.security.UserGroupInformation

import yaooqinn.kyuubi.{KyuubiServerException, SPARK_COMPILE_VERSION}

class KyuubiSparkUtilSuite extends SparkFunSuite {

  test("get current user name") {
    val user = KyuubiSparkUtil.getCurrentUserName()
    assert(user === System.getProperty("user.name"))
  }

  test("get user with impersonation") {
    val currentUser = UserGroupInformation.getCurrentUser
    val user1 = KyuubiSparkUtil.getCurrentUserName()
    assert(user1 === currentUser.getShortUserName)
    val remoteUser = UserGroupInformation.createRemoteUser("test")
    remoteUser.doAs(new PrivilegedExceptionAction[Unit] {
      override def run(): Unit = {
        val user2 = KyuubiSparkUtil.getCurrentUserName()
        assert(user2 === remoteUser.getShortUserName)
      }
    })
  }

  test("Spark version test") {
    assert(SPARK_VERSION === SPARK_COMPILE_VERSION)
  }

  test("testLocalHostName") {
    val hostname = KyuubiSparkUtil.localHostName()
    assert("localhost" === hostname)
  }

  test("testHIVE_VAR_PREFIX") {
    val conf = "spark.foo"
    val hiveConf = "set:hivevar:" + conf
    hiveConf match {
      case KyuubiSparkUtil.HIVE_VAR_PREFIX(c) =>
        assert(c === conf)
    }
  }

  test("testMajorVersion") {
    assert(KyuubiSparkUtil.majorVersion("2.1.2") === 2)
    assert(KyuubiSparkUtil.majorVersion("2.1.2-SNAPSHOT") === 2)
  }

  test("testMinorVersion") {
    assert(KyuubiSparkUtil.minorVersion("2.1.2-SNAPSHOT") === 1)
    assert(KyuubiSparkUtil.minorVersion("2.3") === 3)
  }

  test("testDEPLOY_MODE_DEFAULT") {
    assert(KyuubiSparkUtil.DEPLOY_MODE=== "spark.submit.deployMode")
    assert(KyuubiSparkUtil.DEPLOY_MODE_DEFAULT === "client")
  }

  test("testPRINCIPAL") {
    assert(KyuubiSparkUtil.PRINCIPAL === "spark.yarn.principal")
  }

  test("testKEYTAB") {
    assert(KyuubiSparkUtil.KEYTAB === "spark.yarn.keytab")
  }

  test("testNewConfiguration") {
    val conf = new SparkConf(loadDefaults = true).setMaster("local").setAppName("test")
    conf.set("spark.hadoop.foo", "bar")
    assert(KyuubiSparkUtil.newConfiguration(conf).get("foo") === "bar")
  }

  test("testGetJobGroupIDKey") {
    assert(KyuubiSparkUtil.getJobGroupIDKey() === "spark.jobGroup.id")
  }

  test("testMULTIPLE_CONTEXTS_DEFAULT") {
    assert(KyuubiSparkUtil.MULTIPLE_CONTEXTS === "spark.driver.allowMultipleContexts")
    assert(KyuubiSparkUtil.MULTIPLE_CONTEXTS_DEFAULT === "true")
  }

  test("testCreateTempDir") {
    val tmpDir = KyuubiSparkUtil.createTempDir(namePrefix = "test_kyuubi")
    assert(tmpDir.exists())
    assert(tmpDir.isDirectory)
  }

  test("testExceptionString") {
    val e1: Throwable = null
    assert(KyuubiSparkUtil.exceptionString(e1) === "")
    val msg = "test exception"
    val e2 = new KyuubiServerException(msg, e1)
    assert(KyuubiSparkUtil.exceptionString(e2).contains(msg))
  }

  test("testMETASTORE_JARS") {
    assert(KyuubiSparkUtil.METASTORE_JARS === "spark.sql.hive.metastore.jars")
  }

  test("testDRIVER_BIND_ADDR") {
    assert(KyuubiSparkUtil.DRIVER_BIND_ADDR === "spark.driver.bindAddress")

  }

  test("testCATALOG_IMPL") {
    assert(KyuubiSparkUtil.CATALOG_IMPL === "spark.sql.catalogImplementation")
    assert(KyuubiSparkUtil.CATALOG_IMPL_DEFAULT === "hive")

  }

  test("testSPARK_HADOOP_PREFIX") {
    assert(KyuubiSparkUtil.SPARK_HADOOP_PREFIX === "spark.hadoop.")
  }

  test("testSPARK_UI_PORT") {
    assert(KyuubiSparkUtil.SPARK_UI_PORT === "spark.ui.port")
    assert(KyuubiSparkUtil.SPARK_UI_PORT_DEFAULT === "0")

  }

  test("testSPARK_PREFIX") {
    assert(KyuubiSparkUtil.SPARK_PREFIX === "spark.")
  }

  test("testIsSparkVersionOrHigher") {
    assert(KyuubiSparkUtil.equalOrHigherThan("2.0.2"))
    assert(KyuubiSparkUtil.equalOrHigherThan(SPARK_COMPILE_VERSION))
    assert(!KyuubiSparkUtil.equalOrHigherThan("2.4.1"))
    assert(!KyuubiSparkUtil.equalOrHigherThan("3.0.0"))
  }

  test("testTimeStringAsMs") {
    assert(KyuubiSparkUtil.timeStringAsMs("50s") === 50000L)
    assert(KyuubiSparkUtil.timeStringAsMs("50min") === 50 * 60 * 1000L)
    assert(KyuubiSparkUtil.timeStringAsMs("100ms") === 100L)
  }
}
