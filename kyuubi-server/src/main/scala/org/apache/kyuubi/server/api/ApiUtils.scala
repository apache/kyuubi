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

package org.apache.kyuubi.server.api

import scala.collection.JavaConverters._

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.client.api.v1.dto.{OperationData, ServerData, SessionData}
import org.apache.kyuubi.events.KyuubiOperationEvent
import org.apache.kyuubi.ha.client.ServiceNodeInfo
import org.apache.kyuubi.operation.KyuubiOperation
import org.apache.kyuubi.session.KyuubiSession

object ApiUtils extends Logging {

  def sessionData(session: KyuubiSession): SessionData = {
    val sessionEvent = session.getSessionEvent
    new SessionData(
      session.handle.identifier.toString,
      session.user,
      session.ipAddress,
      session.conf.asJava,
      session.createTime,
      session.lastAccessTime - session.createTime,
      session.getNoOperationTime,
      sessionEvent.flatMap(_.exception).map(Utils.prettyPrint).getOrElse(""),
      session.sessionType.toString,
      session.connectionUrl,
      sessionEvent.map(_.engineId).getOrElse(""))
  }

  def operationData(operation: KyuubiOperation): OperationData = {
    val opEvent = KyuubiOperationEvent(operation)
    new OperationData(
      opEvent.statementId,
      opEvent.statement,
      opEvent.state,
      opEvent.createTime,
      opEvent.startTime,
      opEvent.completeTime,
      opEvent.exception.map(Utils.prettyPrint).getOrElse(""),
      opEvent.sessionId,
      opEvent.sessionUser,
      opEvent.sessionType,
      operation.getSession.asInstanceOf[KyuubiSession].connectionUrl,
      operation.metrics.asJava)
  }

  def serverData(nodeInfo: ServiceNodeInfo): ServerData = {
    new ServerData(
      nodeInfo.nodeName,
      nodeInfo.namespace,
      nodeInfo.instance,
      nodeInfo.host,
      nodeInfo.port,
      nodeInfo.attributes.asJava,
      "Running")
  }

  def logAndRefineErrorMsg(errorMsg: String, throwable: Throwable): String = {
    error(errorMsg, throwable)
    s"$errorMsg: ${Utils.prettyPrint(throwable)}"
  }
}
