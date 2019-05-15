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

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.scalatest.Matchers

import yaooqinn.kyuubi.service.State

class SparkSessionCacheManagerSuite extends SparkFunSuite with Matchers {

  override def afterAll(): Unit = {
    System.clearProperty("SPARK_YARN_MODE")
    super.afterAll()
  }
  test("new cache") {
    val cache = new SparkSessionCacheManager()
    cache.getStartTime should be(0)
    cache.getName should be(classOf[SparkSessionCacheManager].getSimpleName)
    cache.getConf should be(null)
    cache.getServiceState should be(State.NOT_INITED)
    cache.stop()
  }

  test("init cache") {
    val cache = new SparkSessionCacheManager()
    val conf = new SparkConf()
    KyuubiSparkUtil.setupCommonConfig(conf)
    cache.init(conf)
    cache.getStartTime should be(0)
    cache.getConf should be(conf)
    cache.getServiceState should be(State.INITED)
    cache.stop()
  }

  test("start cache") {
    val cache = new SparkSessionCacheManager()
    val conf = new SparkConf()
    KyuubiSparkUtil.setupCommonConfig(conf)
    cache.init(conf)
    cache.start()
    cache.getStartTime / 1000 should be(System.currentTimeMillis() / 1000)
    cache.getConf should be(conf)
    cache.getServiceState should be(State.STARTED)
    cache.stop()
  }

  test("stop cache") {
    val cache = new SparkSessionCacheManager()
    val conf = new SparkConf().set(KyuubiConf.BACKEND_SESSION_CHECK_INTERVAL.key, "1s")
    KyuubiSparkUtil.setupCommonConfig(conf)
    cache.init(conf)
    cache.start()
    Thread.sleep(2000)
    cache.stop()
    cache.getConf should be(conf)
    cache.getServiceState should be(State.STOPPED)
  }

  test("spark session cache should be null if max cache time reached") {
    val cache = new SparkSessionCacheManager()
    val conf = new SparkConf().setMaster("local")
      .set(KyuubiConf.BACKEND_SESSION_MAX_CACHE_TIME.key, "5s")
      .set(KyuubiConf.BACKEND_SESSION_CHECK_INTERVAL.key, "1s")
    KyuubiSparkUtil.setupCommonConfig(conf)
    val session = SparkSession.builder().config(conf).getOrCreate()
    cache.init(conf)
    cache.start()
    val userName = KyuubiSparkUtil.getCurrentUserName
    cache.set(userName, session)
    assert(cache.getAndIncrease(userName).nonEmpty)
    cache.decrease(userName)
    Thread.sleep(7000)
    val maybeCache2 = cache.getAndIncrease(userName)
    assert(maybeCache2.isEmpty, s"reason: ${maybeCache2.map(_.isCrashed).mkString}")
    session.stop()
    cache.stop()
  }
}
