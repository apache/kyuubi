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

import org.apache.spark.KyuubiConf._
import org.apache.spark.KyuubiSparkUtil
import org.apache.spark.sql.SparkSession

/**
 * A recorder for how many client sessions have been cloned by the original [[SparkSession]], which
 * helps the [[SparkSessionCacheManager]] cache and recycle [[SparkSession]] instances.
 *
 * @param spark the original [[SparkSession]] instances
 * @param times times of the original [[SparkSession]] instance has been cloned, start from 1
 * @param initTime Start time of the SparkSession
 */
private[spark]
class SparkSessionCache private(val spark: SparkSession, times: AtomicInteger, initTime: Long) {

  private val maxCacheTime =
    KyuubiSparkUtil.timeStringAsMs(spark.conf.get(BACKEND_SESSION_MAX_CACHE_TIME))

  private val idleTimeout: Long =
    KyuubiSparkUtil.timeStringAsMs(spark.conf.get(BACKEND_SESSION_IDLE_TIMEOUT))

  @volatile
  private var latestLogout: Long = Long.MaxValue

  def updateLogoutTime(time: Long): Unit = latestLogout = time

  /**
   * When all connections are disconnected and idle timeout reached is since the user last time
   * logout.
   *
   */
  def isIdle: Boolean = {
    times.get <= 0 && System.currentTimeMillis - latestLogout > idleTimeout
  }

  /**
   * Whether the cached [[SparkSession]] instance is already stopped.
   */
  def isCrashed: Boolean = spark.sparkContext.isStopped

  /**
   * If the last time is between [maxCacheTime, maxCacheTime * 1.25], we will try to stop this
   * SparkSession only when all connection are disconnected.
   * If the last time is above maxCacheTime * 1.25, we will stop this SparkSession whether it has
   * connections linked or jobs running with.
   *
   */
  def isExpired: Boolean = {
    val now = System.currentTimeMillis
    (now - initTime >= maxCacheTime && times.get <= 0 ) || (now - initTime > maxCacheTime * 1.25)
  }

  def needClear: Boolean = isCrashed || isExpired

  def getReuseTimes: Int = times.get()

  def incReuseTimeAndGet: Int = times.incrementAndGet()

  def decReuseTimeAndGet: Int = times.decrementAndGet()
}

object SparkSessionCache {
  def init(spark: SparkSession): SparkSessionCache =
    new SparkSessionCache(spark, new AtomicInteger(1), System.currentTimeMillis)
}
