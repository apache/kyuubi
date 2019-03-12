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

import java.util.concurrent.TimeUnit

import scala.collection.mutable.{HashSet => MHSet}
import scala.concurrent.{Await, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{KyuubiSparkUtil, SparkConf, SparkContext}
import org.apache.spark.KyuubiConf._
import org.apache.spark.KyuubiSparkUtil._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ui.KyuubiSessionTab

import yaooqinn.kyuubi.{KyuubiSQLException, Logging}
import yaooqinn.kyuubi.author.AuthzHelper
import yaooqinn.kyuubi.ui.{KyuubiServerListener, KyuubiServerMonitor}
import yaooqinn.kyuubi.utils.{KyuubiHadoopUtil, ReflectUtils}

class SparkSessionWithUGI(
    user: UserGroupInformation,
    conf: SparkConf,
    cache: SparkSessionCacheManager) extends Logging {
  import SparkSessionWithUGI._

  private var _sparkSession: SparkSession = _
  private val userName: String = user.getShortUserName
  private val promisedSparkContext = Promise[SparkContext]()
  private var initialDatabase: Option[String] = None
  private var sparkException: Option[Throwable] = None

  private lazy val newContext: Thread = {
    val threadName = "SparkContext-Starter-" + userName
    new Thread(threadName) {
      override def run(): Unit = {
        try {
          promisedSparkContext.trySuccess {
            new SparkContext(conf)
          }
        } catch {
          case e: Exception =>
            sparkException = Some(e)
            throw e
        }
      }
    }
  }

  /**
   * Invoke SparkContext.stop() if not succeed initializing it
   */
  private def stopContext(): Unit = {
    promisedSparkContext.future.map { sc =>
      warn(s"Error occurred during initializing SparkContext for $userName, stopping")
      try {
        sc.stop
      } catch {
        case NonFatal(e) => error(s"Error Stopping $userName's SparkContext", e)
      } finally {
        System.setProperty("SPARK_YARN_MODE", "true")
      }
    }
  }

  /**
   * Setting configuration from connection strings before SparkContext init.
   *
   * @param sessionConf configurations for user connection string
   */
  private def configureSparkConf(sessionConf: Map[String, String]): Unit = {
    for ((key, value) <- sessionConf) {
      key match {
        case HIVE_VAR_PREFIX(DEPRECATED_QUEUE) => conf.set(QUEUE, value)
        case HIVE_VAR_PREFIX(k) =>
          if (k.startsWith(SPARK_PREFIX)) {
            conf.set(k, value)
          } else {
            conf.set(SPARK_HADOOP_PREFIX + k, value)
          }
        case USE_DB => initialDatabase = Some("use " + value)
        case _ =>
      }
    }

    // proxy user does not have rights to get token as real user
    conf.remove(KyuubiSparkUtil.KEYTAB)
    conf.remove(KyuubiSparkUtil.PRINCIPAL)
  }

  /**
   * Setting configuration from connection strings for existing SparkSession
   *
   * @param sessionConf configurations for user connection string
   */
  private def configureSparkSession(sessionConf: Map[String, String]): Unit = {
    for ((key, value) <- sessionConf) {
      key match {
        case HIVE_VAR_PREFIX(k) =>
          if (k.startsWith(SPARK_PREFIX)) {
            _sparkSession.conf.set(k, value)
          } else {
            _sparkSession.conf.set(SPARK_HADOOP_PREFIX + k, value)
          }
        case USE_DB => initialDatabase = Some("use " + value)
        case _ =>
      }
    }
  }

  private def getOrCreate(
      sessionConf: Map[String, String]): Unit = SPARK_INSTANTIATION_LOCK.synchronized {
    val totalRounds = math.max(conf.get(BACKEND_SESSION_WAIT_OTHER_TIMES).toInt, 15)
    var checkRound = totalRounds
    val interval = conf.getTimeAsMs(BACKEND_SESSION_WAIT_OTHER_INTERVAL)
    // if user's sc is being constructed by another
    while (isPartiallyConstructed(userName)) {
      checkRound -= 1
      if (checkRound <= 0) {
        throw new KyuubiSQLException(s"A partially constructed SparkContext for [$userName] " +
          s"has last more than ${totalRounds * interval / 1000} seconds")
      }
      info(s"A partially constructed SparkContext for [$userName], $checkRound times countdown.")
      SPARK_INSTANTIATION_LOCK.wait(interval)
    }

    cache.getAndIncrease(userName) match {
      case Some(ss) =>
        _sparkSession = ss.newSession()
        configureSparkSession(sessionConf)
      case _ =>
        setPartiallyConstructed(userName)
        SPARK_INSTANTIATION_LOCK.notifyAll()
        create(sessionConf)
    }
  }

  private def create(sessionConf: Map[String, String]): Unit = {
    info(s"--------- Create new SparkSession for $userName ----------")
    // kyuubi|user name|canonical host name| port
    val appName = Seq(
      "kyuubi", userName, conf.get(FRONTEND_BIND_HOST), conf.get(FRONTEND_BIND_PORT)).mkString("|")
    conf.setAppName(appName)
    configureSparkConf(sessionConf)
    val totalWaitTime: Long = conf.getTimeAsSeconds(BACKEND_SESSION_INIT_TIMEOUT)
    try {
      KyuubiHadoopUtil.doAs(user) {
        newContext.start()
        val context =
          Await.result(promisedSparkContext.future, Duration(totalWaitTime, TimeUnit.SECONDS))
        _sparkSession = ReflectUtils.newInstance(
          classOf[SparkSession].getName,
          Seq(classOf[SparkContext]),
          Seq(context)).asInstanceOf[SparkSession]
      }
      cache.set(userName, _sparkSession)
    } catch {
      case e: Exception =>
        if (conf.getOption("spark.master").contains("yarn")) {
          KyuubiHadoopUtil.doAs(user) {
            KyuubiHadoopUtil.killYarnAppByName(appName)
          }
        }
        stopContext()
        val cause = findCause(e)
        val msg =
          s"""
             |Get SparkSession for [$userName] failed
             |Diagnosis:
             |${sparkException.map(_.getMessage).getOrElse(cause.getMessage)}
             |Please check if the yarn queue [${conf.get(QUEUE)}] is quite busy
           """.stripMargin
        val ke = new KyuubiSQLException(msg, "08S01", 1001, cause)
        sparkException.foreach(ke.addSuppressed)
        throw ke
    } finally {
      setFullyConstructed(userName)
      newContext.join()
    }

    KyuubiServerMonitor.setListener(userName, new KyuubiServerListener(conf))
    KyuubiServerMonitor.getListener(userName)
      .foreach(_sparkSession.sparkContext.addSparkListener)
    val uiTab = new KyuubiSessionTab(userName, _sparkSession.sparkContext)
    KyuubiServerMonitor.addUITab(_sparkSession.sparkContext.sparkUser, uiTab)
  }

  @throws[KyuubiSQLException]
  def init(sessionConf: Map[String, String]): Unit = {
    getOrCreate(sessionConf)

    try {
      initialDatabase.foreach { db =>
        KyuubiHadoopUtil.doAs(user) {
          _sparkSession.sql(db)
        }
      }
    } catch {
      case e: Exception =>
        cache.decrease(userName)
        throw findCause(e)
    }

    // KYUUBI-99: Add authorizer support after use initial db
    AuthzHelper.get.foreach { auth =>
      _sparkSession.experimental.extraOptimizations ++= auth.rule
    }
  }

  def sparkSession: SparkSession = _sparkSession

}

object SparkSessionWithUGI {

  val SPARK_INSTANTIATION_LOCK = new Object()

  private val userSparkContextBeingConstruct = new MHSet[String]()

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
