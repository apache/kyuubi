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

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{KyuubiSparkUtil, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException

import yaooqinn.kyuubi.server.KyuubiServer
import yaooqinn.kyuubi.session.{KyuubiSession, SessionManager}
import yaooqinn.kyuubi.utils.ReflectUtils

class SparkSessionWithUGISuite extends SparkFunSuite {

  val user = UserGroupInformation.getCurrentUser
  val conf = new SparkConf(loadDefaults = true).setAppName("spark session test")
  KyuubiServer.setupCommonConfig(conf)
  conf.remove(KyuubiSparkUtil.CATALOG_IMPL)
  conf.setMaster("local")
  val userName = user.getShortUserName
  val passwd = ""
  val statement = "show tables"
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

  test("testInit failed with no such database") {
    val sparkSessionWithUGI = new SparkSessionWithUGI(user, conf)
    intercept[NoSuchDatabaseException](sparkSessionWithUGI.init(Map("use:database" -> "fakedb")))

  }

  test("testInit success") {
    val sparkSessionWithUGI = new SparkSessionWithUGI(user, conf)
    sparkSessionWithUGI.init(Map.empty)
    assert(sparkSessionWithUGI.sparkSession.sparkContext.sparkUser === userName)
    assert(sparkSessionWithUGI.userName === userName)
  }

  test("testSetPartiallyConstructed") {
    SparkSessionWithUGI.setPartiallyConstructed("Kent")
    assert(SparkSessionWithUGI.isPartiallyConstructed("Kent"))
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
}
