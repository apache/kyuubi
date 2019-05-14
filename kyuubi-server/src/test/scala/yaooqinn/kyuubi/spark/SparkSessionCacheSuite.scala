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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.{KyuubiSparkUtil, SparkConf, SparkFunSuite}
import org.apache.spark.sql.SparkSession

class SparkSessionCacheSuite extends SparkFunSuite {

  test("spark session catch") {
    val conf = new SparkConf().setMaster("local")
    KyuubiSparkUtil.setupCommonConfig(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val cache = SparkSessionCache(spark)
    assert(cache.spark === spark)
    assert(cache.times.get() === 1)
    assert(cache.times.incrementAndGet() === 2)
    assert(cache.times.decrementAndGet() === 1)
    val now = System.currentTimeMillis
    val cache2 = new SparkSessionCache(spark, new AtomicInteger(1), now)
    assert(cache.spark === cache2.spark )
    assert(cache2.initTime === now)

    spark.stop()
    assert(cache.spark.sparkContext.isStopped)
  }
}
