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

import java.text.SimpleDateFormat
import java.util.Date
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
  private val userLatestLogout = new ConcurrentHashMap[String, Long]
  private var idleTimeout: Long = _
  private var maxCacheTime: Long = _

  private val sessionCleaner = new Runnable {
    override def run(): Unit = {
      userToSession.asScala.foreach {
        case (user, ssc) if ssc.spark.sparkContext.isStopped =>
          warn(s"SparkSession for $user might already be stopped outside Kyuubi," +
            s" cleaning it..")
          removeSparkSession(user)
        case (user, ssc) if ssc.times.get > 0 || !userLatestLogout.containsKey(user) =>
          debug(s"There are ${ssc.times.get} active connection(s) bound to the SparkSession" +
            s" instance of $user")
        case (user, _) if now - userLatestLogout.get(user) >= idleTimeout =>
          info(s"Stopping idle SparkSession for user [$user].")
          removeSparkSession(user)
        case (user, ssc) if isSessionCleanable(ssc) =>
          info(s"Stopping expired SparkSession for user [$user].")
          removeSparkSession(user)
        case _ =>
      }
    }
  }

  private def removeSparkSession(user: String): Unit = {
    Option(userLatestLogout.remove(user)) match {
      case Some(t) => info(s"User [$user] last time logout at " +
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(t)))
      case _ =>
    }
    KyuubiServerMonitor.detachUITab(user)
    val cache = userToSession.remove(user)
    cache.spark.stop()
    System.setProperty("SPARK_YARN_MODE", "true")
  }

  /**
   * If the last time is between [maxCacheTime, maxCacheTime * 1.25], we will try to stop this
   * SparkSession only when all connection are disconnected.
   * If the last time is above maxCacheTime * 1.25, we will stop this SparkSession whether it has
   * connections linked or jobs running with.
   *
   */
  private def isSessionCleanable(cache: SparkSessionCache): Boolean = {
    (now - cache.initTime >= maxCacheTime && cache.times.get <= 0 ) ||
      (now - cache.initTime > maxCacheTime * 1.25)
  }

  private def now: Long = System.currentTimeMillis()

  def set(user: String, sparkSession: SparkSession): Unit = {
    val sessionCache = SparkSessionCache(sparkSession)
    userToSession.put(user, sessionCache)
  }

  def getAndIncrease(user: String): Option[SparkSession] = {
    Option(userToSession.get(user)) match {
      case Some(ssc) if !ssc.spark.sparkContext.isStopped =>
        val currentTime = ssc.times.incrementAndGet()
        info(s"SparkSession for [$user] is reused for $currentTime time(s) after + 1")
        Some(ssc.spark)
      case _ =>
        info(s"SparkSession for [$user] isn't cached, will create a new one.")
        None
    }
  }

  def decrease(user: String): Unit = {
    Option(userToSession.get(user)) match {
      case Some(ssc) if !ssc.spark.sparkContext.isStopped =>
        userLatestLogout.put(user, now)
        val currentTime = ssc.times.decrementAndGet()
        info(s"SparkSession for [$user] is reused for $currentTime time(s) after - 1")
      case _ =>
        warn(s"SparkSession for [$user] was not found in the cache.")
    }
  }

  override def init(conf: SparkConf): Unit = {
    idleTimeout = math.max(conf.getTimeAsMs(BACKEND_SESSION_IDLE_TIMEOUT), 60 * 1000)
    maxCacheTime = math.max(conf.getTimeAsMs(BACKEND_SESSION_MAX_CACHE_TIME), 0)
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
