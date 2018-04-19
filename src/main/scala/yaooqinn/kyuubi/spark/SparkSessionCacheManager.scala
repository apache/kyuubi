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

  def setSparkSession(user: String, sparkSession: SparkSession): Unit = {
    userToSparkSession.put(user, (sparkSession, new AtomicInteger(1)))
  }

  def getSparkSession(user: String): Option[(SparkSession, AtomicInteger)] = {
    Some(userToSparkSession.get(user))
  }

  def removeSparkSession(user: String): Unit = userToSparkSession.remove(user)

  private[this] val sessionCleaner = new Runnable {
    override def run(): Unit = {
      userToSparkSession.asScala.foreach {
        case (user, (session, times)) =>
          if (times.get() <= 0 || session.sparkContext.isStopped) {
            removeSparkSession(user)
            KyuubiServerMonitor.detachUITab(user)
            session.stop()
            if (conf.get("spark.master").startsWith("yarn")) {
              System.setProperty("SPARK_YARN_MODE", "true")
            }
          }
        case _ =>
      }
    }
  }
  /**
   * Periodically close idle SparkSessions in 'spark.kyuubi.session.clean.interval(default 20min)'
   */
  def start(): Unit = {
    // at least 10 min
    val interval = math.max(conf.getTimeAsSeconds(BACKEND_SESSION_CHECK_INTERVAL.key), 10 * 60)
    info(s"Scheduling SparkSession cache cleaning every $interval seconds")
    cacheManager.scheduleAtFixedRate(sessionCleaner, interval, interval, TimeUnit.SECONDS)
  }

  def stop(): Unit = {
    cacheManager.shutdown()
    userToSparkSession.asScala.values.foreach { kv => kv._1.stop() }
    userToSparkSession.clear()
  }
}

object SparkSessionCacheManager {
  private[this] var sparkSessionCacheManager: SparkSessionCacheManager = _

  def startCacheManager(conf: SparkConf): Unit = {
    sparkSessionCacheManager = new SparkSessionCacheManager(conf)
    sparkSessionCacheManager.start()
  }

  def get: SparkSessionCacheManager = sparkSessionCacheManager
}
