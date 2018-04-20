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

import java.lang.reflect.UndeclaredThrowableException
import java.security.PrivilegedExceptionAction
import java.util.concurrent.TimeUnit

import scala.collection.mutable.{HashSet => MHSet}
import scala.concurrent.{Await, Promise, TimeoutException}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkConf, SparkContext, SparkUtils}
import org.apache.spark.KyuubiConf._
import org.apache.spark.SparkUtils._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.ui.KyuubiServerTab

import yaooqinn.kyuubi.{KyuubiSQLException, Logging}
import yaooqinn.kyuubi.ui.{KyuubiServerListener, KyuubiServerMonitor}
import yaooqinn.kyuubi.utils.ReflectUtils

class SparkSessionWithUGI(user: UserGroupInformation, conf: SparkConf) extends Logging {
  private[this] var _sparkSession: SparkSession = _
  def sparkSession: SparkSession = _sparkSession
  private[this] val promisedSparkContext = Promise[SparkContext]()
  private[this] var initialDatabase: Option[String] = None

  private[this] def newContext(): Thread = {
    new Thread(s"Start-SparkContext-$userName") {
      override def run(): Unit = {
        try {
          promisedSparkContext.trySuccess(new SparkContext(conf))
        } catch {
          case NonFatal(e) => throw e
        }
      }
    }
  }

  /**
   * Invoke SparkContext.stop() if not succeed initializing it
   */
  private[this] def stopContext(): Unit = {
    promisedSparkContext.future.map { sc =>
      warn(s"Error occurred during initializing SparkContext for $userName, stopping")
      sc.stop
      System.setProperty("SPARK_YARN_MODE", "true")
    }
  }

  /**
   * Setting configuration from connection strings before SparkConext init.
   * @param sessionConf configurations for user connection string
   */
  private[this] def configureSparkConf(sessionConf: Map[String, String]): Unit = {
    for ((key, value) <- sessionConf) {
      key match {
        case HIVE_VAR_PREFIX(DEPRECATED_QUEUE) => conf.set(QUEUE, value)
        case HIVE_VAR_PREFIX(k) =>
          if (k.startsWith(SPARK_PREFIX)) {
            conf.set(k, value)
          } else {
            conf.set(SPARK_HADOOP_PREFIX + k, value)
          }
        case "use:database" => initialDatabase = Some("use " + value)
        case _ =>
      }
    }

    // proxy user does not have rights to get token as real user
    conf.remove(SparkUtils.KEYTAB)
    conf.remove(SparkUtils.PRINCIPAL)
  }

  /**
   * Setting configuration from connection strings for existing SparkSession
   * @param sessionConf configurations for user connection string
   */
  private[this] def configureSparkSession(sessionConf: Map[String, String]): Unit = {
    for ((key, value) <- sessionConf) {
      key match {
        case HIVE_VAR_PREFIX(k) =>
          if (k.startsWith(SPARK_PREFIX)) {
            _sparkSession.conf.set(k, value)
          } else {
            _sparkSession.conf.set(SPARK_HADOOP_PREFIX + k, value)
          }
        case "use:database" => initialDatabase = Some("use " + value)
        case _ =>
      }
    }
  }

  private[this] def getOrCreate(sessionConf: Map[String, String]): Unit = synchronized {
    var checkRound = math.max(conf.get(BACKEND_SESSION_WAIT_OTHER_TIMES.key).toInt, 15)
    val interval = conf.getTimeAsMs(BACKEND_SESSION_WAIT_OTHER_INTERVAL.key)
    // if user's sc is being constructed by another
    while (SparkSessionWithUGI.isPartiallyConstructed(userName)) {
      wait(interval)
      checkRound -= 1
      if (checkRound <= 0) {
        throw new KyuubiSQLException(s"A partially constructed SparkContext for [$userName] " +
          s"has last more than ${checkRound * interval} seconds")
      }
      info(s"A partially constructed SparkContext for [$userName], $checkRound times countdown.")
    }

    SparkSessionCacheManager.get.getAndIncrease(userName) match {
      case Some(ss) =>
        _sparkSession = ss.newSession()
        configureSparkSession(sessionConf)
      case _ =>
        SparkSessionWithUGI.setPartiallyConstructed(userName)
        notifyAll()
        create(sessionConf)
    }
  }

  private[this] def create(sessionConf: Map[String, String]): Unit = {
    info(s"--------- Create new SparkSession for $userName ----------")
    val appName = s"KyuubiSession[$userName]@" + conf.get(FRONTEND_BIND_HOST.key)
    conf.setAppName(appName)
    configureSparkConf(sessionConf)
    val totalWaitTime: Long = conf.getTimeAsSeconds(BACKEND_SESSTION_INIT_TIMEOUT.key)
    try {
      user.doAs(new PrivilegedExceptionAction[Unit] {
        override def run(): Unit = {
          newContext().start()
          val context =
            Await.result(promisedSparkContext.future, Duration(totalWaitTime, TimeUnit.SECONDS))
          _sparkSession = ReflectUtils.newInstance(
            classOf[SparkSession].getName,
            Seq(classOf[SparkContext]),
            Seq(context)).asInstanceOf[SparkSession]
        }
      })
      SparkSessionCacheManager.get.set(userName, _sparkSession)
    } catch {
      case ute: UndeclaredThrowableException =>
        ute.getCause match {
          case te: TimeoutException =>
            stopContext()
            throw new KyuubiSQLException(
              s"Get SparkSession for [$userName] failed: " + te, "08S01", 1001, te)
          case _ =>
            stopContext()
            throw new KyuubiSQLException(ute.toString, "08S01", ute.getCause)
        }
      case e: Exception =>
        stopContext()
        throw new KyuubiSQLException(
          s"Get SparkSession for [$userName] failed: " + e, "08S01", e)
    } finally {
      SparkSessionWithUGI.setFullyConstructed(userName)
      newContext().join()
    }

    KyuubiServerMonitor.setListener(userName, new KyuubiServerListener(conf))
    KyuubiServerMonitor.getListener(userName)
      .foreach(_sparkSession.sparkContext.addSparkListener)
    val uiTab = new KyuubiServerTab(userName, _sparkSession.sparkContext)
    KyuubiServerMonitor.addUITab(_sparkSession.sparkContext.sparkUser, uiTab)
  }

  def userName: String = user.getShortUserName

  @throws[KyuubiSQLException]
  def init(sessionConf: Map[String, String]): Unit = {
    try {
      getOrCreate(sessionConf)
      initialDatabase.foreach { db =>
        user.doAs(new PrivilegedExceptionAction[Unit] {
          override def run(): Unit = _sparkSession.sql(db)
        })
      }
    } catch {
      case ute: UndeclaredThrowableException => ute.getCause match {
        case e: HiveAccessControlException =>
          throw new KyuubiSQLException(e.getMessage, "08S01", e.getCause)
        case e: NoSuchDatabaseException =>
          throw new KyuubiSQLException(e.getMessage, "08S01", e.getCause)
        case e: KyuubiSQLException => throw e
      }
    }
  }
}

object SparkSessionWithUGI extends Logging {
  private[this] val userSparkContextBeingConstruct = new MHSet[String]()

  def setPartiallyConstructed(user: String): Unit = {
    userSparkContextBeingConstruct.add(user)
  }

  def isPartiallyConstructed(user: String): Boolean = {
    userSparkContextBeingConstruct.contains(user)
  }

  def setFullyConstructed(user: String): Unit = {
    userSparkContextBeingConstruct.remove(user)
  }
}
