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

import java.util.HashMap

import scala.collection.JavaConverters._

import org.apache.hive.service.cli.HiveSQLException
import org.apache.hive.service.cli.session.HiveSession
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.engine.hive.events.SessionEvent
import org.apache.kyuubi.events.EventLogging
import org.apache.kyuubi.operation.{Operation, OperationHandle}
import org.apache.kyuubi.session.{AbstractSession, SessionHandle, SessionManager}

class HiveSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    serverIpAddress: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: SessionManager,
    override val handle: SessionHandle,
    val hive: HiveSession)
  extends AbstractSession(protocol, user, password, ipAddress, conf, sessionManager) {

  private val sessionEvent = SessionEvent(this)

  def serverIpAddress(): String = serverIpAddress

  override def open(): Unit = {
    val confClone = new HashMap[String, String]()
    confClone.putAll(conf.asJava) // pass conf.asScala not support `put` method
    hive.open(confClone)
    EventLogging.onEvent(sessionEvent)
  }

  override protected def runOperation(operation: Operation): OperationHandle = {
    sessionEvent.totalOperations += 1
    super.runOperation(operation)
  }

  override def close(): Unit = {
    sessionEvent.endTime = System.currentTimeMillis()
    EventLogging.onEvent(sessionEvent)
    super.close()
    try {
      hive.close()
    } catch {
      case e: HiveSQLException =>
        error(s"Failed to close hive runtime session: ${e.getMessage}")
    }
  }
}
