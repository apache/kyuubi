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

import scala.concurrent.{Promise, TimeoutException}
import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.server.KyuubiServer
import yaooqinn.kyuubi.ui.KyuubiServerMonitor
import yaooqinn.kyuubi.utils.ReflectUtils

class SparkSessionWithUGISuite extends SparkFunSuite {

  val user = UserGroupInformation.getCurrentUser
  val conf = new SparkConf(loadDefaults = true).setAppName("spark session test")
  KyuubiServer.setupCommonConfig(conf)
  conf.remove(KyuubiSparkUtil.CATALOG_IMPL)
  conf.setMaster("local")
  val userName = user.getShortUserName
  var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    val sc = ReflectUtils
      .newInstance(classOf[SparkContext].getName, Seq(classOf[SparkConf]), Seq(conf))
      .asInstanceOf[SparkContext]
    spark = ReflectUtils.newInstance(
      classOf[SparkSession].getName,
      Seq(classOf[SparkContext]),
      Seq(sc)).asInstanceOf[SparkSession]
    SparkSessionCacheManager.startCacheManager(conf)
    SparkSessionCacheManager.get.set(userName, spark)
  }

  protected override def afterAll(): Unit = {
    SparkSessionCacheManager.get.stop()
    spark.stop()
  }

  test("test init failed with sc init failing") {
    assert(!spark.sparkContext.isStopped)
    val confClone = conf.clone().remove(KyuubiSparkUtil.MULTIPLE_CONTEXTS)
      .set(KyuubiConf.BACKEND_SESSTION_INIT_TIMEOUT.key, "3")
    val userName1 = "test1"
    val ru = UserGroupInformation.createRemoteUser(userName1)
    val sparkSessionWithUGI = new SparkSessionWithUGI(ru, confClone)
    assert(!SparkSessionWithUGI.isPartiallyConstructed(userName1))
    val e = intercept[KyuubiSQLException](sparkSessionWithUGI.init(Map.empty))
    assert(e.getCause.isInstanceOf[TimeoutException])
    val se = e.getSuppressed.head
    assert(se.isInstanceOf[SparkException])
    assert(se.getMessage.startsWith("Only one SparkContext"))
    assert(sparkSessionWithUGI.sparkSession === null)
    assert(System.getProperty("SPARK_YARN_MODE") === null)
    assert(SparkSessionCacheManager.get.getAndIncrease(userName1).isEmpty)
  }

  test("test init failed with no such database") {
    val sparkSessionWithUGI = new SparkSessionWithUGI(user, conf)
    intercept[NoSuchDatabaseException](sparkSessionWithUGI.init(Map("use:database" -> "fakedb")))
    assert(ReflectUtils.getFieldValue(sparkSessionWithUGI,
      "yaooqinn$kyuubi$spark$SparkSessionWithUGI$$initialDatabase") === Some("use fakedb"))
    assert(SparkSessionCacheManager.get.getAndIncrease(userName).nonEmpty)
  }

  test("test init success with empty session conf") {
    val sparkSessionWithUGI = new SparkSessionWithUGI(user, conf)
    sparkSessionWithUGI.init(Map.empty)
    assert(sparkSessionWithUGI.sparkSession.sparkContext.sparkUser === userName)
    assert(sparkSessionWithUGI.userName === userName)
  }

  test("test init success with spark properties") {
    val sessionConf = Map("set:hivevar:spark.foo" -> "bar")
    val sparkSessionWithUGI = new SparkSessionWithUGI(user, conf)
    sparkSessionWithUGI.init(sessionConf)
    assert(sparkSessionWithUGI.sparkSession.conf.get("spark.foo") === "bar")
  }

  test("test init success with hive/hadoop/extra properties") {
    val sessionConf = Map("set:hivevar:foo" -> "bar")
    val sparkSessionWithUGI = new SparkSessionWithUGI(user, conf)
    sparkSessionWithUGI.init(sessionConf)
    assert(sparkSessionWithUGI.sparkSession.conf.get("spark.hadoop.foo") === "bar")
  }

  test("test init with new spark context") {
    val userName1 = "test"
    val ru = UserGroupInformation.createRemoteUser(userName1)
    val sessionConf = Map("set:hivevar:spark.foo" -> "bar", "set:hivevar:foo" -> "bar")
    val sparkSessionWithUGI = new SparkSessionWithUGI(ru, conf)
    sparkSessionWithUGI.init(sessionConf)
    assert(sparkSessionWithUGI.sparkSession.conf.get("spark.foo") === "bar")
    assert(sparkSessionWithUGI.sparkSession.conf.get("spark.hadoop.foo") === "bar")
    assert(!sparkSessionWithUGI.sparkSession.sparkContext.getConf.contains(KyuubiSparkUtil.KEYTAB))
    assert(KyuubiServerMonitor.getListener(userName1).nonEmpty)
    sparkSessionWithUGI.sparkSession.stop()
  }

  test("testSetPartiallyConstructed") {
    val confClone = conf.clone().set(KyuubiConf.BACKEND_SESSION_WAIT_OTHER_TIMES.key, "3")
    SparkSessionWithUGI.setPartiallyConstructed(userName)
    val sparkSessionWithUGI = new SparkSessionWithUGI(user, confClone)
    val e = intercept[KyuubiSQLException](sparkSessionWithUGI.init(Map.empty))
    assert(e.getMessage.startsWith("A partially constructed SparkContext for"))
    assert(e.getMessage.contains(userName))
    assert(e.getMessage.contains("has last more than 15 seconds"))
    assert(SparkSessionWithUGI.isPartiallyConstructed(userName))
    assert(!SparkSessionWithUGI.isPartiallyConstructed("Kent Yao"))
    SparkSessionWithUGI.setFullyConstructed(userName)
  }

  test("test init failed with time out exception") {
    // point to an non-exist cluster manager
    val confClone = conf.clone().setMaster("spark://localhost:7077")
      .set(KyuubiConf.BACKEND_SESSTION_INIT_TIMEOUT.key, "3")
    val userName1 = "test"
    val ru = UserGroupInformation.createRemoteUser(userName1)
    val sparkSessionWithUGI = new SparkSessionWithUGI(ru, confClone)
    assert(!SparkSessionWithUGI.isPartiallyConstructed(userName1))
    val e = intercept[KyuubiSQLException](sparkSessionWithUGI.init(Map.empty))
    assert(e.getCause.isInstanceOf[TimeoutException])
    assert(e.getMessage.startsWith("Get SparkSession"))
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

  test("stop sparkcontext") {
    val sparkSessionWithUGI = new SparkSessionWithUGI(user, conf)
    sparkSessionWithUGI.init(Map.empty)
    val promise = ReflectUtils.getFieldValue(sparkSessionWithUGI,
      "yaooqinn$kyuubi$spark$SparkSessionWithUGI$$promisedSparkContext")
      .asInstanceOf[Promise[SparkContext]]
    val future = promise.future
    ReflectUtils.invokeMethod(sparkSessionWithUGI, "stopContext")
    future.foreach { sc =>
      assert(sc.isStopped)
    }
  }
}
