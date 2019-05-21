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

import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}

import scala.collection.JavaConverters._

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.spark.KyuubiConf._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import yaooqinn.kyuubi.Logging
import yaooqinn.kyuubi.service.AbstractService
import yaooqinn.kyuubi.ui.KyuubiServerMonitor

/**
 * Manager for cached [[SparkSession]]s.
 */
class SparkSessionCacheManager private(name: String) extends AbstractService(name) with Logging {

  def this() = this(classOf[SparkSessionCacheManager].getSimpleName)

  private val cacheManager =
    Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat(getClass.getSimpleName + "-%d").build())

  private val userToSession = new ConcurrentHashMap[String, SparkSessionCache]
  private val sessionCleaner = new Runnable {
    override def run(): Unit = {
      userToSession.asScala.foreach {
        case (user, ssc) if ssc.isCrashed => removeSparkSession(user, doCheck = true)
        case (user, ssc) => tryStopIdleCache(user, ssc)
      }
    }
  }

  /**
   * Stop the idle [[SparkSession]] instance, then it can be cleared by the `sessionCleaner` or
   * when the user reconnecting action.
   *
   */
  private def tryStopIdleCache(user: String, ssc: SparkSessionCache): Unit = this.synchronized {
    if (ssc.isIdle) {
      info(s"Stopping idle SparkSession for user [$user]")
      ssc.spark.stop()
      KyuubiServerMonitor.detachUITab(user)
      System.setProperty("SPARK_YARN_MODE", "true")
    }
  }

  private def removeSparkSession(user: String, doCheck: Boolean = false): Unit = this.synchronized {
    if (doCheck) {
      // if we do remove SparkSession in sessionCleaner thread, we should double check whether the
      // SparkSessionCache is removed or not recreated.
      val cache = userToSession.get(user)
      if(cache != null && cache.isCrashed) {
        info(s"Cleaning stopped SparkSession for user [$user]")
        userToSession.remove(user)
        KyuubiServerMonitor.detachUITab(user)
      }
    } else {
      val cache = userToSession.remove(user)
      cache.spark.stop()
      KyuubiServerMonitor.detachUITab(user)
      System.setProperty("SPARK_YARN_MODE", "true")
    }
  }

  def set(user: String, sparkSession: SparkSession): Unit = {
    val sessionCache = SparkSessionCache.init(sparkSession)
    userToSession.put(user, sessionCache)
  }

  def getAndIncrease(user: String): Option[SparkSessionCache] = this.synchronized {
    Option(userToSession.get(user)) match {
      case Some(ssc) if ssc.needClear =>
        removeSparkSession(user)
        info(s"SparkSession for [$user] needs to be cleared, will create a new one")
        None
      case Some(ssc) if !ssc.needClear =>
        val currentTime = ssc.incReuseTimeAndGet
        info(s"SparkSession for [$user] is reused for $currentTime time(s) after + 1")
        Some(ssc)
      case _ =>
        info(s"SparkSession for [$user] isn't cached, will create a new one")
        None
    }
  }

  def decrease(user: String): Unit = {
    Option(userToSession.get(user)) match {
      case Some(ssc) =>
        ssc.updateLogoutTime(System.currentTimeMillis)
        val currentTime = ssc.decReuseTimeAndGet
        info(s"SparkSession for [$user] is reused for $currentTime time(s) after - 1")
      case _ =>
        warn(s"SparkSession for [$user] was not found in the cache.")
    }
  }

  override def init(conf: SparkConf): Unit = {
    super.init(conf)
  }

  /**
   * Periodically close idle SparkSessions in 'spark.kyuubi.session.clean.interval(default 1min)'
   */
  override def start(): Unit = {
    val interval = math.max(conf.getTimeAsSeconds(BACKEND_SESSION_CHECK_INTERVAL), 1)
    info(s"Scheduling SparkSession cache cleaning every $interval seconds")
    cacheManager.scheduleAtFixedRate(sessionCleaner, interval, interval, TimeUnit.SECONDS)
    super.start()
  }

  override def stop(): Unit = {
    info("Stopping SparkSession Cache Manager")
    cacheManager.shutdown()
    userToSession.asScala.values.foreach(_.spark.stop())
    userToSession.clear()
    super.stop()
  }
}
