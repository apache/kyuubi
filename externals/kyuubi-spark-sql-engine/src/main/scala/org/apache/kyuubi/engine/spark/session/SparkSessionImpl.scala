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

import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.apache.spark.sql.{AnalysisException, SparkSession}

import org.apache.kyuubi.engine.spark.events.SessionEvent
import org.apache.kyuubi.engine.spark.operation.SparkSQLOperationManager
import org.apache.kyuubi.engine.spark.shim.SparkCatalogShim
import org.apache.kyuubi.engine.spark.udf.KDFRegistry
import org.apache.kyuubi.events.EventBus
import org.apache.kyuubi.operation.{Operation, OperationHandle}
import org.apache.kyuubi.session.{AbstractSession, SessionManager}

class SparkSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: SessionManager,
    val spark: SparkSession)
  extends AbstractSession(protocol, user, password, ipAddress, conf, sessionManager) {

  private def setModifiableConfig(key: String, value: String): Unit = {
    try {
      spark.conf.set(key, value)
    } catch {
      case e: AnalysisException => warn(e.getMessage())
    }
  }

  private val sessionEvent = SessionEvent(this)

  def serverIpAddress(): String = serverIpAddress

  override def open(): Unit = {
    normalizedConf.foreach {
      case ("use:database", database) =>
        try {
          SparkCatalogShim().setCurrentDatabase(spark, database)
        } catch {
          case e
              if database == "default" && e.getMessage != null &&
                e.getMessage.contains("not found") =>
          // use:database is from hive so the catalog is always session catalog which must have
          // default namespace `default`. But as spark support v2 catalog, catalog may not have
          // default namespace. Here we do nothing for compatible both session and v2 catalog.
        }
      case (key, value) => setModifiableConfig(key, value)
    }
    KDFRegistry.registerAll(spark)
    EventBus.post(sessionEvent)
    super.open()
  }

  override protected def runOperation(operation: Operation): OperationHandle = {
    sessionEvent.totalOperations += 1
    super.runOperation(operation)
  }

  override def close(): Unit = {
    sessionEvent.endTime = System.currentTimeMillis()
    EventBus.post(sessionEvent)
    super.close()
    spark.sessionState.catalog.getTempViewNames().foreach(spark.catalog.uncacheTable(_))
    sessionManager.operationManager.asInstanceOf[SparkSQLOperationManager].closeILoop(handle)
  }
}
