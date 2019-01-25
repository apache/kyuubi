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
import org.mockito.Mockito._
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar

import yaooqinn.kyuubi.service.State
import yaooqinn.kyuubi.utils.ReflectUtils

class SparkSessionCacheManagerSuite extends SparkFunSuite with Matchers with MockitoSugar {

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
    cache.getStartTime / 100 should be(System.currentTimeMillis() / 100)
    cache.getConf should be(conf)
    cache.getServiceState should be(State.STARTED)
    val ss = mock[SparkSession]
    val userName = KyuubiSparkUtil.getCurrentUserName
    val sc = mock[SparkContext]
    when(ss.sparkContext).thenReturn(sc)

    cache.decrease(userName) // None
    cache.getAndIncrease(userName) // None

    cache.set(userName, ss)
    when(sc.isStopped).thenReturn(false)
    cache.getAndIncrease(userName)

    when(sc.isStopped).thenReturn(true)
    cache.getAndIncrease(userName)

    when(sc.isStopped).thenReturn(false)
    cache.decrease(userName)
    when(sc.isStopped).thenReturn(true)
    cache.decrease(userName)

    Thread.sleep(2000)
    val field = cache.getClass.getDeclaredField("sessionCleaner")
    field.setAccessible(true)

    when(sc.isStopped).thenReturn(false)
    val runnable = field.get(cache).asInstanceOf[Runnable]
    runnable.run() // > 0

    when(sc.isStopped).thenReturn(false)
    cache.decrease(userName)
    when(sc.isStopped).thenReturn(false)
    runnable.run() // not expiry
    when(sc.isStopped).thenReturn(false)
    ReflectUtils.setFieldValue(
      cache, "yaooqinn$kyuubi$spark$SparkSessionCacheManager$$idleTimeout", 0)
    runnable.run()
    System.clearProperty("SPARK_YARN_MODE")
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

}
