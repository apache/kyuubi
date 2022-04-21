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

import java.util.concurrent.ConcurrentHashMap

import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.{KyuubiSQLException, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.ShareLevel
import org.apache.kyuubi.engine.ShareLevel._
import org.apache.kyuubi.engine.spark.{KyuubiSparkUtil, SparkSQLEngine}
import org.apache.kyuubi.engine.spark.operation.SparkSQLOperationManager
import org.apache.kyuubi.session._

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

  override def initialize(conf: KyuubiConf): Unit = {
    val absPath = Utils.getAbsolutePathFromWork(conf.get(ENGINE_OPERATION_LOG_DIR_ROOT))
    _operationLogRoot = Some(absPath.toAbsolutePath.toString)
    super.initialize(conf)
  }

  val operationManager = new SparkSQLOperationManager()

  private lazy val singleSparkSession = conf.get(ENGINE_SINGLE_SPARK_SESSION)
  private lazy val shareLevel = ShareLevel.withName(conf.get(ENGINE_SHARE_LEVEL))

  private lazy val userIsolatedSparkSession = conf.get(ENGINE_USER_ISOLATED_SPARK_SESSION)
  private lazy val userIsolatedSparkSessionCache = new ConcurrentHashMap[String, SparkSession]()

  private def getOrNewSparkSession(user: String): SparkSession = {
    if (singleSparkSession) {
      spark
    } else {
      shareLevel match {
        // it's unnecessary to create a new spark session in connection share level
        // since the session is only one
        case CONNECTION => spark
        case USER => newSparkSession(spark)
        case GROUP | SERVER if userIsolatedSparkSession =>
          if (userIsolatedSparkSessionCache.containsKey(user)) {
            userIsolatedSparkSessionCache.get(user)
          } else {
            val newSession = newSparkSession(spark)
            userIsolatedSparkSessionCache.put(user, newSession)
            newSession
          }
        case GROUP | SERVER => newSparkSession(spark)
        case _ => throw new IllegalStateException(s"Unrecognized share level: $shareLevel")
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
    val clientIp = conf.getOrElse(CLIENT_IP_KEY, ipAddress)
    val sparkSession =
      try {
        getOrNewSparkSession(user)
      } catch {
        case e: Exception => throw KyuubiSQLException(e)
      }

    new SparkSessionImpl(
      protocol,
      user,
      password,
      ipAddress,
      clientIp,
      conf,
      this,
      sparkSession)
  }

  override def closeSession(sessionHandle: SessionHandle): Unit = {
    super.closeSession(sessionHandle)
    if (shareLevel == ShareLevel.CONNECTION) {
      info("Session stopped due to shared level is Connection.")
      stopSession()
    }
  }

  private def stopSession(): Unit = {
    SparkSQLEngine.currentEngine.foreach(_.stop())
  }

  override protected def isServer: Boolean = false
}
