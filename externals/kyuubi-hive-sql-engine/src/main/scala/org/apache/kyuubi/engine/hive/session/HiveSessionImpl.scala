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

package org.apache.kyuubi.engine.hive.session

import java.util

import scala.collection.JavaConverters._

import org.apache.hive.common.util.HiveVersionInfo
import org.apache.hive.service.cli.session.HiveSession

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.engine.hive.events.HiveSessionEvent
import org.apache.kyuubi.engine.hive.udf.KDFRegistry
import org.apache.kyuubi.events.EventBus
import org.apache.kyuubi.operation.{Operation, OperationHandle}
import org.apache.kyuubi.session.{AbstractSession, SessionHandle, SessionManager}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TGetInfoType, TGetInfoValue, TProtocolVersion}
import org.apache.kyuubi.util.reflect.{DynFields, DynMethods}

class HiveSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: SessionManager,
    override val handle: SessionHandle,
    val hive: HiveSession)
  extends AbstractSession(protocol, user, password, ipAddress, conf, sessionManager) {

  private val sessionEvent = HiveSessionEvent(this)

  override def open(): Unit = {
    val confClone = new util.HashMap[String, String]()
    confClone.putAll(conf.asJava) // pass conf.asScala not support `put` method
    hive.open(confClone)
    KDFRegistry.registerAll()
    EventBus.post(sessionEvent)
  }

  override protected def runOperation(operation: Operation): OperationHandle = {
    sessionEvent.totalOperations += 1
    super.runOperation(operation)
  }

  override def getInfo(infoType: TGetInfoType): TGetInfoValue = withAcquireRelease() {
    infoType match {
      case TGetInfoType.CLI_SERVER_NAME => TGetInfoValue.stringValue("Hive")
      case TGetInfoType.CLI_DBMS_NAME => TGetInfoValue.stringValue("Apache Hive")
      case TGetInfoType.CLI_DBMS_VER => TGetInfoValue.stringValue(HiveVersionInfo.getVersion)
      case TGetInfoType.CLI_ODBC_KEYWORDS =>
        try {
          // HIVE-17765 expose Hive keywords.
          // exclude these keywords to be consistent with Hive behavior.
          val excludes = DynFields.builder()
            .hiddenImpl("org.apache.hive.service.cli.session.HiveSessionImpl", "ODBC_KEYWORDS")
            .buildStaticChecked[util.Set[String]]().get()
          val keywords = DynMethods.builder("getKeywords")
            .impl("org.apache.hadoop.hive.ql.parse.ParseUtils", classOf[util.Set[String]])
            .buildStaticChecked()
            .invoke[String](excludes)
          TGetInfoValue.stringValue(keywords)
        } catch {
          case _: ReflectiveOperationException =>
            TGetInfoValue.stringValue("Unimplemented")
        }
      case TGetInfoType.CLI_MAX_COLUMN_NAME_LEN |
          TGetInfoType.CLI_MAX_SCHEMA_NAME_LEN |
          TGetInfoType.CLI_MAX_TABLE_NAME_LEN => TGetInfoValue.lenValue(128)
      case _ => throw KyuubiSQLException(s"Unrecognized GetInfoType value: $infoType")
    }
  }

  override def close(): Unit = {
    sessionEvent.endTime = System.currentTimeMillis()
    EventBus.post(sessionEvent)
    super.close()
    try {
      hive.close()
    } catch {
      case e: Exception =>
        error(s"Failed to close hive runtime session: ${e.getMessage}")
    }
  }
}
