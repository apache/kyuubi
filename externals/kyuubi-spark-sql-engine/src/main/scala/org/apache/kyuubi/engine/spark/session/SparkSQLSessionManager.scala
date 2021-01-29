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

import java.util.concurrent.TimeUnit

import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
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

  val operationManager = new SparkSQLOperationManager()

  @volatile private var _latestLogoutTime: Long = System.currentTimeMillis()
  def latestLogoutTime: Long = _latestLogoutTime

  override def openSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): SessionHandle = {
    info(s"Opening session for $user@$ipAddress")
    val sessionImpl = new SparkSessionImpl(protocol, user, password, ipAddress, conf, this)
    val handle = sessionImpl.handle
    try {
      val sparkSession = spark.newSession()
      conf.foreach {
        case (HIVE_VAR_PREFIX(key), value) => setModifiableConfig(sparkSession, key, value)
        case (HIVE_CONF_PREFIX(key), value) => setModifiableConfig(sparkSession, key, value)
        case ("use:database", database) => sparkSession.catalog.setCurrentDatabase(database)
        case (key, value) => setModifiableConfig(sparkSession, key, value)
      }
      sessionImpl.open()
      operationManager.setSparkSession(handle, sparkSession)
      setSession(handle, sessionImpl)
      info(s"$user's session with $handle is opened, current opening sessions" +
      s" $getOpenSessionCount")
      handle
    } catch {
      case e: Exception =>
        sessionImpl.close()
        throw KyuubiSQLException(e)
    }
  }

  override def closeSession(sessionHandle: SessionHandle): Unit = {
    _latestLogoutTime = System.currentTimeMillis()
    super.closeSession(sessionHandle)
    operationManager.removeSparkSession(sessionHandle)
  }

  private def setModifiableConfig(spark: SparkSession, key: String, value: String): Unit = {
    if (spark.conf.isModifiable(key)) {
      spark.conf.set(key, value)
    } else {
      warn(s"Spark config $key is static and will be ignored")
    }
  }

  override protected def isServer: Boolean = false

  override def start(): Unit = {
    startTimeoutChecker()
    super.start()
  }

  private def startTimeoutChecker(): Unit = {
    val interval = conf.get(KyuubiConf.ENGINE_CHECK_INTERVAL)
    val idleTimeout = conf.get(KyuubiConf.ENGINE_IDLE_TIMEOUT)
    val checkTask = new Runnable {
      override def run(): Unit = {
        while (getOpenSessionCount > 0 ||
          System.currentTimeMillis - latestLogoutTime < idleTimeout) {
          TimeUnit.MILLISECONDS.sleep(interval)
        }
        info(s"Idled for more than $idleTimeout ms, terminating")
        sys.exit(0)
      }
    }
    submitBackgroundOperation(checkTask)
  }
}
