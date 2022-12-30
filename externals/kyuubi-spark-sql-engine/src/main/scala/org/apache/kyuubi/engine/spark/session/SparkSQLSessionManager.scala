/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.engine.spark.session

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.ShareLevel
import org.apache.kyuubi.engine.ShareLevel._
import org.apache.kyuubi.engine.spark.{KyuubiSparkUtil, SparkSQLEngine}
import org.apache.kyuubi.engine.spark.operation.SparkSQLOperationManager
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_NAMESPACE
import org.apache.kyuubi.ha.client.{DiscoveryClient, DiscoveryClientProvider, DiscoveryPaths, ServiceDiscovery}
import org.apache.kyuubi.session._
import org.apache.kyuubi.util.ThreadUtils

/**
 * A [[SessionManager]] constructed with [[SparkSession]] which give it the ability to talk with
 * Spark and let Spark do all the rest heavy work :)
 *
 *  @param name Service Name
 * @param spark A [[SparkSession]] instance that this [[SessionManager]] holds to create individual
 *              [[SparkSession]] for [[org.apache.kyuubi.session.Session]]s.
 */
class SparkSQLSessionManager private (name: String, spark: SparkSession)
  extends SessionManager(name) {

  def this(spark: SparkSession) = this(classOf[SparkSQLSessionManager].getSimpleName, spark)

  val operationManager = new SparkSQLOperationManager()

  private lazy val singleSparkSession = conf.get(ENGINE_SINGLE_SPARK_SESSION)
  private lazy val shareLevel = ShareLevel.withName(conf.get(ENGINE_SHARE_LEVEL))

  private lazy val userIsolatedSparkSession = conf.get(ENGINE_USER_ISOLATED_SPARK_SESSION)
  private lazy val userIsolatedIdleInterval =
    conf.get(ENGINE_USER_ISOLATED_SPARK_SESSION_IDLE_INTERVAL)
  private lazy val userIsolatedIdleTimeout =
    conf.get(ENGINE_USER_ISOLATED_SPARK_SESSION_IDLE_TIMEOUT)
  private val userIsolatedCacheLock = new Object
  private lazy val userIsolatedCache = new java.util.HashMap[String, SparkSession]()
  private lazy val userIsolatedCacheCount =
    new java.util.HashMap[String, (Integer, java.lang.Long)]()
  private var userIsolatedSparkSessionThread: Option[ScheduledExecutorService] = None

  private var discoveryClient: Option[DiscoveryClient] = None
  private var sessionsPath: String = _

  private def startUserIsolatedCacheChecker(): Unit = {
    if (!userIsolatedSparkSession) {
      userIsolatedSparkSessionThread =
        Some(ThreadUtils.newDaemonSingleThreadScheduledExecutor("user-isolated-cache-checker"))
      userIsolatedSparkSessionThread.foreach {
        _.scheduleWithFixedDelay(
          () => {
            userIsolatedCacheLock.synchronized {
              val iter = userIsolatedCacheCount.entrySet().iterator()
              while (iter.hasNext) {
                val kv = iter.next()
                if (kv.getValue._1 == 0 &&
                  kv.getValue._2 + userIsolatedIdleTimeout < System.currentTimeMillis()) {
                  userIsolatedCache.remove(kv.getKey)
                  iter.remove()
                }
              }
            }
          },
          userIsolatedIdleInterval,
          userIsolatedIdleInterval,
          TimeUnit.MILLISECONDS)
      }
    }
  }

  private def startRegisterSessionsPath(): Unit = {
    shareLevel match {
      case CONNECTION =>
      // TODO add enable config
      case _ if ServiceDiscovery.supportServiceDiscovery(conf) =>
        val namespace = conf.get(HA_NAMESPACE)
        // /kyuubi_1.6.1-incubating_USER_SPARK_SQL/tom/engine-pool-0
        // /kyuubi_1.6.1-incubating_USER_SPARK_SQL/sessions/tom/engine-pool-0
        // TODO sessions name configurable?
        val sessionsPrefixPath = namespace.split("/", 3).drop(1).mkString("/", "/sessions/", "")
        discoveryClient = Some(DiscoveryClientProvider.createDiscoveryClient(conf))
        discoveryClient.get.createClient()
        // TODO etcd mode?
        sessionsPath = discoveryClient.get.create(sessionsPrefixPath, "PERSISTENT")
//        sessionsPath = discoveryClient.get.create(sessionsPrefixPath, "PERSISTENT_SEQUENTIAL")
      case _ =>
    }
  }

  override def start(): Unit = {
    startUserIsolatedCacheChecker()
    startRegisterSessionsPath()
    super.start()
  }

  override def stop(): Unit = {
    super.stop()
    userIsolatedSparkSessionThread.foreach(_.shutdown())
    discoveryClient.foreach { client =>
//      client.delete(sessionsPath, true)
      client.closeClient()
    }
  }

  private def getOrNewSparkSession(user: String): SparkSession = {
    if (singleSparkSession) {
      spark
    } else {
      shareLevel match {
        // it's unnecessary to create a new spark session in connection share level
        // since the session is only one
        case CONNECTION => spark
        case USER => newSparkSession(spark)
        case GROUP | SERVER if userIsolatedSparkSession => newSparkSession(spark)
        case GROUP | SERVER =>
          userIsolatedCacheLock.synchronized {
            if (userIsolatedCache.containsKey(user)) {
              val (count, _) = userIsolatedCacheCount.get(user)
              userIsolatedCacheCount.put(user, (count + 1, System.currentTimeMillis()))
              userIsolatedCache.get(user)
            } else {
              userIsolatedCacheCount.put(user, (1, System.currentTimeMillis()))
              val newSession = newSparkSession(spark)
              userIsolatedCache.put(user, newSession)
              newSession
            }
          }
      }
    }
  }

  private def newSparkSession(rootSparkSession: SparkSession): SparkSession = {
    val newSparkSession = rootSparkSession.newSession()
    KyuubiSparkUtil.initializeSparkSession(newSparkSession, conf.get(ENGINE_SESSION_INITIALIZE_SQL))
    newSparkSession
  }

  override protected def createSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): Session = {
    val sparkSession =
      try {
        getOrNewSparkSession(user)
      } catch {
        case e: Exception => throw KyuubiSQLException(e)
      }

    val session = new SparkSessionImpl(
      protocol,
      user,
      password,
      ipAddress,
      conf,
      this,
      sparkSession)
    createSessionInfo(session.handle)
    session
  }

  override def closeSession(sessionHandle: SessionHandle): Unit = {
    if (!userIsolatedSparkSession) {
      val session = getSession(sessionHandle)
      if (session != null) {
        userIsolatedCacheLock.synchronized {
          if (userIsolatedCacheCount.containsKey(session.user)) {
            val (count, _) = userIsolatedCacheCount.get(session.user)
            userIsolatedCacheCount.put(session.user, (count - 1, System.currentTimeMillis()))
          }
        }
      }
    }
    super.closeSession(sessionHandle)
    deleteSessionInfo(sessionHandle)
    if (shareLevel == ShareLevel.CONNECTION) {
      info("Session stopped due to shared level is Connection.")
      stopSession()
    }
  }

  private def stopSession(): Unit = {
    SparkSQLEngine.currentEngine.foreach(_.stop())
  }

  private def createSessionInfo(handle: SessionHandle) = {
    discoveryClient.foreach { client =>
      val sessionPath = DiscoveryPaths.makePath(
        sessionsPath,
        handle.identifier.toString)
      // TODO etcd mode?
      client.create(sessionPath, "EPHEMERAL")
    }
  }

  private def deleteSessionInfo(handle: SessionHandle) = {
    discoveryClient.foreach { client =>
      val sessionPath = DiscoveryPaths.makePath(
        sessionsPath,
        handle.identifier.toString)
      client.delete(sessionPath)
    }
  }

  override protected def isServer: Boolean = false
}
