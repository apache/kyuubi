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
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.hadoop.security.Credentials
import org.apache.spark.{KyuubiSparkUtil, SparkConf}
import org.apache.spark.KyuubiConf._
import org.apache.spark.scheduler.cluster.KyuubiSparkExecutorUtils
import org.apache.spark.sql.SparkSession

import yaooqinn.kyuubi.Logging
import yaooqinn.kyuubi.service.AbstractService
import yaooqinn.kyuubi.ui.KyuubiServerMonitor

class SparkSessionCacheManager private(name: String) extends AbstractService(name) with Logging {

  def this() = this(classOf[SparkSessionCacheManager].getSimpleName)

  private val cacheManager =
    Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat(getClass.getSimpleName + "-%d").build())

  private val userToSession = new ConcurrentHashMap[String, (SparkSession, AtomicInteger)]
  private val userLatestLogout = new ConcurrentHashMap[String, Long]
  private var idleTimeout: Long = _

  private val userToCredentials = new ConcurrentHashMap[String, Credentials]
  private var needPopulateToken: Boolean = _

  private val sessionCleaner = new Runnable {
    override def run(): Unit = {
      userToSession.asScala.foreach {
        case (user, (session, _)) if session.sparkContext.isStopped =>
          warn(s"SparkSession for $user might already be stopped outside Kyuubi," +
            s" cleaning it..")
          removeSparkSession(user)
        case (user, (session, times)) if times.get() > 0 || !userLatestLogout.containsKey(user) =>
          debug(s"There are $times active connection(s) bound to the SparkSession instance" +
            s" of $user ")
          if (needPopulateToken) {
            val credentials = userToCredentials.getOrDefault(user, new Credentials)
            KyuubiSparkExecutorUtils.populateTokens(session.sparkContext, credentials)
          }
        case (user, (session, _))
          if userLatestLogout.get(user) + idleTimeout <= System.currentTimeMillis() =>
          info(s"Stopping idle SparkSession for user [$user].")
          removeSparkSession(user)
          session.stop()
          System.setProperty("SPARK_YARN_MODE", "true")
        case (user, (session, _)) =>
          if (needPopulateToken) {
            val credentials = userToCredentials.getOrDefault(user, new Credentials)
            KyuubiSparkExecutorUtils.populateTokens(session.sparkContext, credentials)
          }
      }
    }
  }

  private def removeSparkSession(user: String): Unit = {
    Option(userLatestLogout.remove(user)) match {
      case Some(t) => info(s"User [$user] last time logout at " +
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(t)))
      case _ =>
    }
    userToSession.remove(user)
    KyuubiServerMonitor.detachUITab(user)
  }

  def set(user: String, sparkSession: SparkSession): Unit = {
    userToSession.put(user, (sparkSession, new AtomicInteger(1)))
  }

  def getAndIncrease(user: String): Option[SparkSession] = {
    Option(userToSession.get(user)) match {
      case Some((ss, times)) if !ss.sparkContext.isStopped =>
        val currentTime = times.incrementAndGet()
        info(s"SparkSession for [$user] is reused for $currentTime time(s) after + 1")
        Some(ss)
      case _ =>
        info(s"SparkSession for [$user] isn't cached, will create a new one.")
        None
    }
  }

  def decrease(user: String): Unit = {
    Option(userToSession.get(user)) match {
      case Some((ss, times)) if !ss.sparkContext.isStopped =>
        userLatestLogout.put(user, System.currentTimeMillis())
        val currentTime = times.decrementAndGet()
        info(s"SparkSession for [$user] is reused for $currentTime time(s) after - 1")
      case _ =>
        warn(s"SparkSession for [$user] was not found in the cache.")
    }
  }

  def setupCredentials(user: String, creds: Credentials): Unit = {
    userToCredentials.put(user, creds)
  }

  override def init(conf: SparkConf): Unit = {
    idleTimeout = math.max(conf.getTimeAsMs(BACKEND_SESSION_IDLE_TIMEOUT.key), 60 * 1000)
    needPopulateToken = conf.get(BACKEND_SESSION_LONG_CACHE).toBoolean &&
      KyuubiSparkUtil.classIsLoadable(conf.get(BACKEND_SESSION_TOKEN_UPDATE_CLASS))
    super.init(conf)
  }

  /**
   * Periodically close idle SparkSessions in 'spark.kyuubi.session.clean.interval(default 1min)'
   */
  override def start(): Unit = {
    val interval = math.max(conf.getTimeAsSeconds(BACKEND_SESSION_CHECK_INTERVAL.key), 1)
    info(s"Scheduling SparkSession cache cleaning every $interval seconds")
    cacheManager.scheduleAtFixedRate(sessionCleaner, interval, interval, TimeUnit.SECONDS)
    super.start()
  }

  override def stop(): Unit = {
    info("Stopping SparkSession Cache Manager")
    cacheManager.shutdown()
    userToSession.asScala.values.foreach { kv => kv._1.stop() }
    userToSession.clear()
    super.stop()
  }
}
