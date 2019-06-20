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

import org.apache.spark.{KyuubiConf, KyuubiSparkUtil, SparkConf, SparkFunSuite}
import org.apache.spark.sql.SparkSession

class SparkSessionCacheSuite extends SparkFunSuite {
  private val conf = new SparkConf()
    .setMaster("local")
    .set(KyuubiConf.BACKEND_SESSION_MAX_CACHE_TIME.key, "10s")
  KyuubiSparkUtil.setupCommonConfig(conf)
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sql("show tables")
    super.beforeAll()
  }
  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    super.afterAll()git
  }

  test("spark session cache") {
    val start = System.currentTimeMillis()
    val cache = SparkSessionCache.init(spark)
    val end = System.currentTimeMillis()
    print("init cache using " + (end - start))
    print("\n")
    assert(!cache.isCrashed)
    assert(!cache.isIdle)
    assert(!cache.needClear, s"cache status [crash:${cache.isCrashed}, expired:${cache.isExpired}]")
    assert(!cache.isExpired)
    assert(cache.spark === spark)
    assert(cache.getReuseTimes === 1)
    assert(cache.incReuseTimeAndGet === 2)
    assert(cache.decReuseTimeAndGet === 1)
    val expired = cache.isExpired
    assert(!expired)
    Thread.sleep(10000)
    assert(cache.decReuseTimeAndGet === 0)
    assert(cache.isExpired)
    assert(cache.needClear)
  }

  test("cache status idled") {
    val start = System.currentTimeMillis()
    val cache = SparkSessionCache.init(spark)
    val end = System.currentTimeMillis()
    print("init cache using " + (end - start))
    print("\n")
    assert(!cache.isIdle, "cache is not idled, reuse time = 1")
    cache.decReuseTimeAndGet
    assert(!cache.isIdle, "cache is not idled, reuse time = 0, but latest logout is unset")
    val latestLogout = System.currentTimeMillis() - 2 * KyuubiSparkUtil.timeStringAsMs(
      spark.conf.get(KyuubiConf.BACKEND_SESSION_IDLE_TIMEOUT.key))
    cache.updateLogoutTime(latestLogout)
    assert(cache.isIdle, "cache is idled, reuse time = 0, idle timeout exceeded")
    cache.incReuseTimeAndGet
    assert(!cache.isIdle, "cache is not idled, idle timeout exceeded however reuse time = 1")
  }
}
