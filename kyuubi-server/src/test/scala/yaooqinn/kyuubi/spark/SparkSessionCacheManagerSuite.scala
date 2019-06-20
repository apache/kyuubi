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
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterEach, Matchers}
import org.scalatest.mock.MockitoSugar

import yaooqinn.kyuubi.metrics.MetricsSystem
import yaooqinn.kyuubi.service.State

class SparkSessionCacheManagerSuite
  extends SparkFunSuite with Matchers with MockitoSugar with BeforeAndAfterEach {

  override def afterAll(): Unit = {
    System.clearProperty("SPARK_YARN_MODE")
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    MetricsSystem.close()
    super.beforeEach()
  }

  override def beforeAll(): Unit = super.beforeAll()
  test("new cache") {
    val cache = new SparkSessionCacheManager()
    cache.getStartTime should be(0)
    cache.getName should be(classOf[SparkSessionCacheManager].getSimpleName)
    cache.getConf should be(null)
    cache.getServiceState should be(State.NOT_INITED)
    cache.stop()
  }

  test("init cache") {
    withCacheMgrInit { (cache, conf) =>
      cache.getStartTime should be(0)
      cache.getConf should be(conf)
      cache.getServiceState should be(State.INITED)
    }
  }

  test("start cache") {
    withCacheMgrStarted { (cache, conf) =>
      cache.getStartTime / 1000 should be(System.currentTimeMillis() / 1000)
      cache.getConf should be(conf)
      cache.getServiceState should be(State.STARTED)
    }
  }

  test("stop cache") {
    withCacheMgrStarted { (cache, conf) =>
      cache.stop()
      cache.getConf should be(conf)
      cache.getServiceState should be(State.STOPPED)
    }
  }

  test("spark session cache should be null if max cache time reached") {
    withCacheMgrStarted { (cache, conf) =>
      val session = SparkSession.builder().config(conf).getOrCreate()
      val userName = KyuubiSparkUtil.getCurrentUserName
      cache.set(userName, session)
      assert(cache.getAndIncrease(userName).nonEmpty)
      cache.decrease(userName)
      Thread.sleep(10000)
      val maybeCache2 = cache.getAndIncrease(userName)
      assert(maybeCache2.isEmpty, s"reason, crash ${maybeCache2.map(_.isCrashed).mkString}")
      session.stop()
    }
  }

  test("spark session cleaner thread test") {
    withCacheMgrStarted { (cacheManager, conf) =>
      val session = SparkSession.builder().config(conf).getOrCreate()
      val ss1 = mock[SparkSession]
      val sc1 = mock[SparkContext]

      when(ss1.sparkContext).thenReturn(sc1)
      when(sc1.isStopped).thenReturn(true)
      when(ss1.conf).thenReturn(session.conf)

      cacheManager.set("alice", ss1)
      cacheManager.set("bob", session)
      val latestLogout = System.currentTimeMillis() - 2 * KyuubiSparkUtil.timeStringAsMs(
        conf.get(KyuubiConf.BACKEND_SESSION_IDLE_TIMEOUT.key))
      cacheManager.getAndIncrease("bob").foreach { c =>
        c.updateLogoutTime(latestLogout)
        c.decReuseTimeAndGet
        c.decReuseTimeAndGet
      }
      Thread.sleep(1000)
      assert(cacheManager.getAndIncrease("alice").isEmpty)
      assert(cacheManager.getAndIncrease("bob").isEmpty)
      assert(cacheManager.getAndIncrease("tom").isEmpty)
    }
  }

  def withCacheMgrInit(f: (SparkSessionCacheManager, SparkConf) => Unit): Unit = {
    val cache = new SparkSessionCacheManager()
    val conf = new SparkConf()
    try {
      cache.init(conf)
      f(cache, conf)
    } finally {
      cache.stop()
    }
  }

  def withCacheMgrStarted(f: (SparkSessionCacheManager, SparkConf) => Unit): Unit = {
    val cache = new SparkSessionCacheManager()
    val conf = new SparkConf().setMaster("local")
      .set(KyuubiConf.BACKEND_SESSION_MAX_CACHE_TIME.key, "5s")
      .set(KyuubiConf.BACKEND_SESSION_CHECK_INTERVAL.key, "50ms")
    KyuubiSparkUtil.setupCommonConfig(conf)

    try {
      cache.init(conf)
      cache.start()
      f(cache, conf)
    } finally {
      cache.stop()
    }
  }

}
