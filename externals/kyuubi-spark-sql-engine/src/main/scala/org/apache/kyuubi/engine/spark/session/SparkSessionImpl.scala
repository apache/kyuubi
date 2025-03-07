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

import java.util.ServiceLoader
import java.util.concurrent.atomic.AtomicLong

import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.ui.SparkUIUtils.formatDuration

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_HANDLE_KEY
import org.apache.kyuubi.engine.spark.events.SessionEvent
import org.apache.kyuubi.engine.spark.operation.SparkSQLOperationManager
import org.apache.kyuubi.engine.spark.udf.KDFRegistry
import org.apache.kyuubi.engine.spark.util.SparkCatalogUtils
import org.apache.kyuubi.events.EventBus
import org.apache.kyuubi.operation.{Operation, OperationHandle}
import org.apache.kyuubi.session._
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TGetInfoType, TGetInfoValue, TProtocolVersion}

class SparkSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: SessionManager,
    val spark: SparkSession)
  extends AbstractSession(protocol, user, password, ipAddress, conf, sessionManager) {

  override val handle: SessionHandle =
    conf.get(KYUUBI_SESSION_HANDLE_KEY).map(SessionHandle.fromUUID).getOrElse(SessionHandle())
  private val sessionRunTime = new AtomicLong(0)
  private val sessionCpuTime = new AtomicLong(0)

  private def setModifiableConfig(key: String, value: String): Unit = {
    try {
      spark.conf.set(key, value)
    } catch {
      case e: AnalysisException => warn(e.getMessage())
    }
  }

  private val sessionEvent = SessionEvent(this)

  override def open(): Unit = {

    val (useCatalogAndDatabaseConf, otherConf) = normalizedConf.partition { case (k, _) =>
      Array(USE_CATALOG, USE_DATABASE).contains(k)
    }

    useCatalogAndDatabaseConf.get(USE_CATALOG).foreach { catalog =>
      try {
        SparkCatalogUtils.setCurrentCatalog(spark, catalog)
      } catch {
        case e if e.getMessage.contains("Cannot find catalog plugin class for catalog") =>
          warn(e.getMessage())
      }
    }

    useCatalogAndDatabaseConf.get("use:database").foreach { database =>
      try {
        spark.sessionState.catalogManager.setCurrentNamespace(Array(database))
      } catch {
        case e
            if database == "default" &&
              StringUtils.containsAny(
                e.getMessage,
                "not found",
                "SCHEMA_NOT_FOUND",
                "is not authorized to perform: glue:GetDatabase") =>
      }
    }

    otherConf.foreach {
      case (key, value) => setModifiableConfig(key, value)
    }
    ServiceLoader.load(classOf[KDFRegistry])
      .foreach(_.registerAll(spark))
    EventBus.post(sessionEvent)
    super.open()
  }

  override protected def runOperation(operation: Operation): OperationHandle = {
    sessionEvent.totalOperations += 1
    super.runOperation(operation)
  }

  override def getInfo(infoType: TGetInfoType): TGetInfoValue = withAcquireRelease() {
    infoType match {
      case TGetInfoType.CLI_SERVER_NAME | TGetInfoType.CLI_DBMS_NAME =>
        TGetInfoValue.stringValue("Spark SQL")
      case TGetInfoType.CLI_DBMS_VER => TGetInfoValue.stringValue(org.apache.spark.SPARK_VERSION)
      case TGetInfoType.CLI_ODBC_KEYWORDS => TGetInfoValue.stringValue("Unimplemented")
      case TGetInfoType.CLI_MAX_COLUMN_NAME_LEN |
          TGetInfoType.CLI_MAX_SCHEMA_NAME_LEN |
          TGetInfoType.CLI_MAX_TABLE_NAME_LEN => TGetInfoValue.lenValue(128)
      case _ => throw KyuubiSQLException(s"Unrecognized GetInfoType value: $infoType")
    }
  }

  override def close(): Unit = {
    info(s"sessionId=${sessionEvent.sessionId}, " +
      s"sessionRunTime=${formatDuration(sessionRunTime.get())}, " +
      s"sessionCpuTime=${formatDuration(sessionCpuTime.get() / 1000000)}")
    sessionEvent.endTime = System.currentTimeMillis()
    sessionEvent.sessionRunTime = sessionRunTime.get()
    sessionEvent.sessionCpuTime = sessionCpuTime.get()
    EventBus.post(sessionEvent)
    super.close()
    spark.sessionState.catalog.getTempViewNames().foreach(spark.catalog.uncacheTable)
    sessionManager.operationManager.asInstanceOf[SparkSQLOperationManager].closeILoop(handle)
    sessionManager.operationManager.asInstanceOf[SparkSQLOperationManager].closePythonProcess(
      handle)
  }

  def increaseRunAndCpuTime(runTime: Long, cpuTime: Long): Unit = {
    sessionEvent.sessionRunTime = sessionRunTime.addAndGet(runTime)
    sessionEvent.sessionCpuTime = sessionCpuTime.addAndGet(cpuTime)
  }
}
