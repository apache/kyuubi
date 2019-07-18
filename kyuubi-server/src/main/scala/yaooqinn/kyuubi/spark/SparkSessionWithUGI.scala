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

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.concurrent.{Await, Promise, TimeoutException}
import scala.concurrent.duration.Duration
import scala.util.Try

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{KyuubiSparkUtil, SparkConf, SparkContext}
import org.apache.spark.KyuubiConf._
import org.apache.spark.KyuubiSparkUtil._
import org.apache.spark.sql.{SparkSession, SparkSQLUtils}
import org.apache.spark.ui.KyuubiSessionTab

import yaooqinn.kyuubi.{KyuubiSQLException, Logging}
import yaooqinn.kyuubi.author.AuthzHelper
import yaooqinn.kyuubi.ui.{KyuubiServerListener, KyuubiServerMonitor}
import yaooqinn.kyuubi.utils.{KyuubiHadoopUtil, KyuubiHiveUtil, ReflectUtils}

class SparkSessionWithUGI(
    user: UserGroupInformation,
    conf: SparkConf,
    cacheMgr: SparkSessionCacheManager) extends Logging {
  import KyuubiHadoopUtil._
  import SparkSessionWithUGI._

  private var _sparkSession: SparkSession = _
  private val userName: String = user.getShortUserName
  private val promisedSparkContext = Promise[SparkContext]()
  private var initialDatabase: String = "use default"
  private val startTime = System.currentTimeMillis()
  private val timeout = KyuubiSparkUtil.timeStringAsMs(conf.get(BACKEND_SESSION_INIT_TIMEOUT))

  private lazy val newContext: Thread = {
    val threadName = "SparkContext-Starter-" + userName
    new Thread(threadName) {
      override def run(): Unit = promisedSparkContext.complete(Try(new SparkContext(conf)))
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
        case HIVE_VAR_PREFIX(k) =>
          if (k.startsWith(SPARK_PREFIX)) {
            conf.set(k, value)
          } else {
            conf.set(SPARK_HADOOP_PREFIX + k, value)
          }
        case USE_DB => initialDatabase = "use " + value
        case _ =>
      }
    }
    // proxy user does not have rights to get token as real user
    conf.remove(KEYTAB)
    conf.remove(PRINCIPAL)
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
        case USE_DB => initialDatabase = "use " + value
        case _ =>
      }
    }
  }

  private def getOrCreate(
      sessionConf: Map[String, String]): Unit = {
    cacheMgr.getAndIncrease(userName) match {
      case Some(ssc) =>
        _sparkSession = ssc.spark.newSession()
        configureSparkSession(sessionConf)
      case _ if isPartiallyConstructed(userName) =>
        if (System.currentTimeMillis() - startTime > timeout) {
          throw new KyuubiSQLException(userName + " has a constructing sc, timeout, aborting")
        } else {
          SPARK_INSTANTIATION_LOCK.synchronized {
            SPARK_INSTANTIATION_LOCK.wait(1000)
          }
          getOrCreate(sessionConf)
        }
      case _ =>
        SPARK_INSTANTIATION_LOCK.synchronized {
          if (isPartiallyConstructed(userName)) {
            getOrCreate(sessionConf)
          } else {
            setPartiallyConstructed(userName)
          }
        }
        if (_sparkSession == null) {
          create(sessionConf)
        }
    }
  }

  private def create(sessionConf: Map[String, String]): Unit = {
    // kyuubi|user name|canonical host name|port|uuid
    val appName = Seq(
      "kyuubi",
      userName,
      conf.get(FRONTEND_BIND_HOST),
      conf.get(FRONTEND_BIND_PORT),
      UUID.randomUUID().toString).mkString("|")
    conf.setAppName(appName)
    configureSparkConf(sessionConf)
    info(s"Create new SparkSession for " + userName + " as " + appName)

    try {
      doAs(user) {
        newContext.start()
        val context =
          Await.result(promisedSparkContext.future, Duration(timeout, TimeUnit.SECONDS))
        _sparkSession = ReflectUtils.newInstance(
          classOf[SparkSession].getName,
          Seq(classOf[SparkContext]),
          Seq(context)).asInstanceOf[SparkSession]
      }
      cacheMgr.set(userName, _sparkSession)
    } catch {
      case e: TimeoutException =>
        if (conf.getOption("spark.master").contains("yarn")) {
          doAsAndLogNonFatal(user)(killYarnAppByName(appName))
        }
        val msg =
          s"""
             |Failed to get SparkSession for [$userName]
             |Diagnosis: ${e.getMessage}
             |Please check whether the specified yarn queue [${conf.getOption(QUEUE)
            .getOrElse("")}] has sufficient resources left
           """.stripMargin
        throw new KyuubiSQLException(msg, "08S01", 1001, e)
      case e: Exception => throw new KyuubiSQLException(e)
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
      doAs(user) {
        SparkSQLUtils.initializeMetaStoreClient(_sparkSession)
        _sparkSession.sql(initialDatabase)
        KyuubiHiveUtil.addDelegationTokensToHiveState(user)
      }
    } catch {
      case e: Exception =>
        cacheMgr.decrease(userName)
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

  private val userSparkContextBeingConstruct = new ConcurrentHashMap[String, Boolean]()

  def setPartiallyConstructed(user: String): Unit = {
    userSparkContextBeingConstruct.put(user, true)
  }

  def isPartiallyConstructed(user: String): Boolean = {
    Option(userSparkContextBeingConstruct.get(user)).getOrElse(false)
  }

  def setFullyConstructed(user: String): Unit = {
    userSparkContextBeingConstruct.remove(user)
  }
}
