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

package yaooqinn.kyuubi.spark

import java.util.UUID
import java.util.concurrent.CountDownLatch

import scala.concurrent.TimeoutException

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.author.AuthzHelper
import yaooqinn.kyuubi.ui.KyuubiServerMonitor

class SparkSessionWithUGISuite extends SparkFunSuite {

  private val user = UserGroupInformation.getCurrentUser
  private val conf = new SparkConf(loadDefaults = true)
    .setMaster("local")
    .setAppName("spark session test")
  KyuubiSparkUtil.setupCommonConfig(conf)
  conf.remove(KyuubiSparkUtil.CATALOG_IMPL)
  private val userName = user.getShortUserName
  var spark: SparkSession = _
  val cache = new SparkSessionCacheManager()

  override protected def beforeAll(): Unit = {
    spark = SparkSession.builder().config(conf).getOrCreate()
    cache.init(conf)
    cache.start()
    cache.set(userName, spark)
  }

  protected override def afterAll(): Unit = {
    cache.stop()
    spark.stop()
  }

  test("error initializing spark context with an exception") {
    val confClone = conf.clone().remove(KyuubiSparkUtil.MULTIPLE_CONTEXTS)
    val userName = UUID.randomUUID().toString
    val ugi = UserGroupInformation.createRemoteUser(userName)
    val sswu = new SparkSessionWithUGI(ugi, confClone, cache)
    assert(!SparkSessionWithUGI.isPartiallyConstructed(userName))
    val e = intercept[KyuubiSQLException](sswu.init(Map.empty))
    assert(e.getCause.isInstanceOf[SparkException])
    assert(e.getMessage.contains("Only one SparkContext"))
    assert(sswu.sparkSession === null)
    assert(System.getProperty("SPARK_YARN_MODE") === null)
    assert(cache.getAndIncrease(userName).isEmpty)
  }

  test("timeout initializing spark context") {
    val confClone = conf.clone().set(KyuubiConf.BACKEND_SESSION_INIT_TIMEOUT.key, "0")
    val userName = UUID.randomUUID().toString
    val ugi = UserGroupInformation.createRemoteUser(userName)
    val sswu = new SparkSessionWithUGI(ugi, confClone, cache)
    assert(!SparkSessionWithUGI.isPartiallyConstructed(userName))
    val e = intercept[KyuubiSQLException](sswu.init(Map.empty))
    assert(e.getCause.isInstanceOf[TimeoutException])
  }

  test("test init failed with no such database") {
    val sparkSessionWithUGI = new SparkSessionWithUGI(user, conf, cache)
    val e = intercept[NoSuchDatabaseException](
      sparkSessionWithUGI.init(Map("use:database" -> "fakedb")))
    assert(e.getMessage().contains("fakedb"))
    assert(cache.getAndIncrease(userName).nonEmpty)
  }

  test("test init success with empty session conf") {
    val sparkSessionWithUGI = new SparkSessionWithUGI(user, conf, cache)
    sparkSessionWithUGI.init(Map.empty)
    assert(sparkSessionWithUGI.sparkSession.sparkContext.sparkUser === userName)
  }

  test("test init with authorization configured.") {
    val confClone = conf.clone()
      .set(KyuubiConf.AUTHORIZATION_METHOD.key, "yaooqinn.kyuubi.TestRule")
      .set(KyuubiConf.AUTHORIZATION_ENABLE.key, "true")

    AuthzHelper.init(confClone)
    val sparkSessionWithUGI = new SparkSessionWithUGI(user, confClone, cache)
    sparkSessionWithUGI.init(Map.empty)
    assert(sparkSessionWithUGI.sparkSession.experimental.extraOptimizations.nonEmpty)
  }

  test("test init success with spark properties") {
    val sessionConf = Map("set:hivevar:spark.foo" -> "bar", "abc" -> "xyz")
    val sparkSessionWithUGI = new SparkSessionWithUGI(user, conf, cache)
    sparkSessionWithUGI.init(sessionConf)
    assert(sparkSessionWithUGI.sparkSession.conf.get("spark.foo") === "bar")
    assert(sparkSessionWithUGI.sparkSession.conf.getOption("abc").isEmpty)

  }

  test("test init success with hive/hadoop/extra properties") {
    val sessionConf = Map("set:hivevar:foo" -> "bar")
    val sparkSessionWithUGI = new SparkSessionWithUGI(user, conf, cache)
    sparkSessionWithUGI.init(sessionConf)
    assert(sparkSessionWithUGI.sparkSession.conf.get("spark.hadoop.foo") === "bar")
  }

  test("test init with new spark context") {
    val userName1 = "test"
    val ru = UserGroupInformation.createRemoteUser(userName1)
    val sessionConf = Map("set:hivevar:spark.foo" -> "bar", "set:hivevar:foo" -> "bar")
    val sparkSessionWithUGI = new SparkSessionWithUGI(ru, conf, cache)
    sparkSessionWithUGI.init(sessionConf)
    assert(sparkSessionWithUGI.sparkSession.conf.get("spark.foo") === "bar")
    assert(sparkSessionWithUGI.sparkSession.conf.get("spark.hadoop.foo") === "bar")
    assert(!sparkSessionWithUGI.sparkSession.sparkContext.getConf.contains(KyuubiSparkUtil.KEYTAB))
    assert(KyuubiServerMonitor.getListener(userName1).nonEmpty)
    sparkSessionWithUGI.sparkSession.stop()
  }

  test("testSetPartiallyConstructed") {
    val confClone = conf.clone().set(KyuubiConf.BACKEND_SESSION_INIT_TIMEOUT.key, "1s")
    val username = "testSetPartiallyConstructed"
    SparkSessionWithUGI.setPartiallyConstructed(username)
    val ru = UserGroupInformation.createRemoteUser(username)

    val sparkSessionWithUGI = new SparkSessionWithUGI(ru, confClone, cache)
    val e = intercept[KyuubiSQLException](sparkSessionWithUGI.init(Map.empty))
    assert(e.getMessage.startsWith(username))
    assert(!SparkSessionWithUGI.isPartiallyConstructed(userName))
    assert(!SparkSessionWithUGI.isPartiallyConstructed("Kent Yao"))
  }

  test("testSetFullyConstructed") {
    SparkSessionWithUGI.setPartiallyConstructed("Kent")
    assert(SparkSessionWithUGI.isPartiallyConstructed("Kent"))
    SparkSessionWithUGI.setFullyConstructed("Kent")
    assert(!SparkSessionWithUGI.isPartiallyConstructed("Kent"))
  }

  test("testIsPartiallyConstructed") {
    assert(!SparkSessionWithUGI.isPartiallyConstructed(userName))
  }

  test("user name should be switched") {
    val proxyUserName = "Kent"
    val proxyUser = UserGroupInformation.createProxyUser(proxyUserName, user)
    val sparkSessionWithUGI = new SparkSessionWithUGI(proxyUser, conf, cache)
    sparkSessionWithUGI.init(Map.empty)
    assert(sparkSessionWithUGI.sparkSession.sparkContext.sparkUser === proxyUserName)
  }

  test("get spark context by diff users") {
    val userName1 = UUID.randomUUID().toString
    val userName2 = UUID.randomUUID().toString

    val ugi1 = UserGroupInformation.createRemoteUser(userName1)
    val ugi2 = UserGroupInformation.createRemoteUser(userName2)

    val sswu1 = new SparkSessionWithUGI(ugi1, conf, cache)
    val sswu2 = new SparkSessionWithUGI(ugi2, conf, cache)

    val latch = new CountDownLatch(2)

    val t1 = new Thread {
      override def run(): Unit = {
        try {
          Thread.sleep(100)
          sswu1.init(Map.empty)
        } finally {
          latch.countDown()
        }
      }
    }

    val t2 = new Thread {
      override def run(): Unit = {
        try {
          Thread.sleep(100)
          sswu2.init(Map.empty)
        } finally {
          latch.countDown()
        }
      }
    }
    t1.start()
    t2.start()
    latch.await()

    assert(cache.getAndIncrease(userName1).get.getReuseTimes === 2)
    assert(sswu1.sparkSession !== sswu2.sparkSession)
    assert(sswu1.sparkSession.sparkContext !== sswu2.sparkSession.sparkContext)
  }

  test("get spark context by same user") {
    val userName = UUID.randomUUID().toString
    val ugi = UserGroupInformation.createRemoteUser(userName)
    val sswu1 = new SparkSessionWithUGI(ugi, conf, cache)
    val sswu2 = new SparkSessionWithUGI(ugi, conf, cache)

    val latch = new CountDownLatch(2)

    val t1 = new Thread {
      override def run(): Unit = {
        try {
          Thread.sleep(100)
          sswu1.init(Map.empty)
        } finally {
          latch.countDown()
        }
      }
    }

    val t2 = new Thread {
      override def run(): Unit = {
        try {
          Thread.sleep(100)
          sswu2.init(Map.empty)
        } finally {
          latch.countDown()
        }
      }
    }
    t1.start()
    t2.start()
    latch.await()

    assert(cache.getAndIncrease(userName).get.getReuseTimes === 3)
    assert(sswu1.sparkSession !== sswu2.sparkSession)
    assert(sswu1.sparkSession.sparkContext === sswu2.sparkSession.sparkContext)
  }
}
