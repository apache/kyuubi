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
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.spark.KyuubiConf._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import yaooqinn.kyuubi.Logging
import yaooqinn.kyuubi.ui.KyuubiServerMonitor

class SparkSessionCacheManager(conf: SparkConf) extends Logging {
  private val cacheManager =
    Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat(getClass.getSimpleName + "-%d").build())

  private[this] val userToSparkSession =
    new ConcurrentHashMap[String, (SparkSession, AtomicInteger)]

  private[this] val userLatestLogout = new ConcurrentHashMap[String, Long]
  private[this] val idleTimeout =
    math.max(conf.getTimeAsMs(BACKEND_SESSION_IDLE_TIMEOUT.key), 60 * 1000)

  def set(user: String, sparkSession: SparkSession): Unit = {
    userToSparkSession.put(user, (sparkSession, new AtomicInteger(1)))
  }

  def getAndIncrease(user: String): Option[SparkSession] = {
    Some(userToSparkSession.get(user)) match {
      case Some((ss, times)) if !ss.sparkContext.isStopped =>
        info(s"SparkSession for [$user] is reused for ${times.incrementAndGet()} time(s) after + 1")
        Some(ss)
      case _ =>
        info(s"SparkSession for [$user] isn't cached, will create a new one.")
        None
    }
  }

  def decrease(user: String): Unit = {
    Some(userToSparkSession.get(user)) match {
      case Some((ss, times)) if !ss.sparkContext.isStopped =>
        userLatestLogout.put(user, System.currentTimeMillis())
        info(s"SparkSession for [$user] is reused for ${times.decrementAndGet()} time(s) after -1")
      case _ =>
        warn(s"SparkSession for [$user] was not found in the cache.")
    }
  }

  private[this] def removeSparkSession(user: String): Unit = {
    userToSparkSession.remove(user)
    KyuubiServerMonitor.detachUITab(user)
  }

  private[this] val sessionCleaner = new Runnable {
    override def run(): Unit = {
      userToSparkSession.asScala.foreach {
        case (user, (session, _)) if session.sparkContext.isStopped =>
          warn(s"SparkSession for $user might already be stopped by forces outside Kyuubi," +
            s" cleaning it..")
          removeSparkSession(user)
        case (user, (_, times)) if times.get() > 0 =>
          debug(s"There are $times active connection(s) bound to the SparkSession instance" +
            s" of $user ")
        case (user, (_, _)) if !userLatestLogout.containsKey(user) =>
        case (user, (session, _))
          if userLatestLogout.get(user) + idleTimeout <= System.currentTimeMillis() =>
          info(s"Stopping idle SparkSession for user [$user].")
          removeSparkSession(user)
          session.stop()
          if (conf.get("spark.master").startsWith("yarn")) {
            System.setProperty("SPARK_YARN_MODE", "true")
          }
        case _ =>
      }
    }
  }
  /**
   * Periodically close idle SparkSessions in 'spark.kyuubi.session.clean.interval(default 5min)'
   */
  def start(): Unit = {
    // at least 1 minutes
    val interval = math.max(conf.getTimeAsSeconds(BACKEND_SESSION_CHECK_INTERVAL.key), 60)
    info(s"Scheduling SparkSession cache cleaning every $interval seconds")
    cacheManager.scheduleAtFixedRate(sessionCleaner, interval, interval, TimeUnit.SECONDS)
  }

  def stop(): Unit = {
    info("Stopping SparkSession Cache Manager")
    cacheManager.shutdown()
    userToSparkSession.asScala.values.foreach { kv => kv._1.stop() }
    userToSparkSession.clear()
  }
}

object SparkSessionCacheManager {

  type Usage = AtomicInteger

  private[this] var sparkSessionCacheManager: SparkSessionCacheManager = _

  def startCacheManager(conf: SparkConf): Unit = {
    sparkSessionCacheManager = new SparkSessionCacheManager(conf)
    sparkSessionCacheManager.start()
  }

  def get: SparkSessionCacheManager = sparkSessionCacheManager
}
