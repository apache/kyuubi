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
import org.apache.kyuubi.client.api.v1.dto
import org.apache.kyuubi.client.api.v1.dto.{KyuubiServerEvent, OperationData, OperationProgress, ServerData, SessionData}
import org.apache.kyuubi.events.KyuubiServerInfoEvent
import org.apache.kyuubi.ha.client.ServiceNodeInfo
import org.apache.kyuubi.operation.KyuubiOperation
import org.apache.kyuubi.session.KyuubiSession

object ApiUtils extends Logging {
  def sessionEvent(session: KyuubiSession): dto.KyuubiSessionEvent = {
    session.getSessionEvent.map(event =>
      dto.KyuubiSessionEvent.builder()
        .sessionId(event.sessionId)
        .clientVersion(event.clientVersion)
        .sessionType(event.sessionType)
        .sessionName(event.sessionName)
        .user(event.user)
        .clientIp(event.clientIP)
        .serverIp(event.serverIP)
        .conf(event.conf.asJava)
        .remoteSessionId(event.remoteSessionId)
        .engineId(event.engineId)
        .engineName(event.engineName)
        .engineUrl(event.engineUrl)
        .eventTime(event.eventTime)
        .openedTime(event.openedTime)
        .startTime(event.startTime)
        .endTime(event.endTime)
        .totalOperations(event.totalOperations)
        .exception(event.exception.orNull)
        .build()).orNull
  }

  def sessionData(session: KyuubiSession): SessionData = {
    val sessionEvent = session.getSessionEvent
    new SessionData(
      session.handle.identifier.toString,
      sessionEvent.map(_.remoteSessionId).getOrElse(""),
      session.user,
      session.ipAddress,
      session.conf.asJava,
      session.createTime,
      session.lastAccessTime - session.createTime,
      session.getNoOperationTime,
      sessionEvent.flatMap(_.exception).map(Utils.prettyPrint).getOrElse(""),
      session.sessionType.toString,
      session.connectionUrl,
      sessionEvent.map(_.engineId).getOrElse(""),
      sessionEvent.map(_.engineName).getOrElse(""),
      sessionEvent.map(_.engineUrl).getOrElse(""),
      session.name.getOrElse(""),
      sessionEvent.map(_.totalOperations).getOrElse(0): Int)
  }

  private def operationProgress(operation: KyuubiOperation): OperationProgress = {
    Option(operation.getOperationJobProgress).map { jobProgress =>
      new OperationProgress(
        jobProgress.getHeaderNames,
        jobProgress.getRows,
        jobProgress.getProgressedPercentage,
        jobProgress.getStatus.toString,
        jobProgress.getFooterSummary,
        jobProgress.getStartTime)
    }.orNull
  }

  def operationEvent(operation: KyuubiOperation): dto.KyuubiOperationEvent = {
    val opEvent = operation.getOperationEvent
    dto.KyuubiOperationEvent.builder()
      .statementId(opEvent.statementId)
      .remoteId(opEvent.remoteId)
      .statement(opEvent.statement)
      .shouldRunAsync(opEvent.shouldRunAsync)
      .state(opEvent.state)
      .eventTime(opEvent.eventTime)
      .createTime(opEvent.createTime)
      .startTime(opEvent.startTime)
      .completeTime(opEvent.completeTime)
      .exception(opEvent.exception.orNull)
      .sessionId(opEvent.sessionId)
      .sessionUser(opEvent.sessionUser)
      .sessionType(opEvent.sessionType)
      .kyuubiInstance(opEvent.kyuubiInstance)
      .metrics(opEvent.metrics.asJava)
      .progress(operationProgress(operation))
      .build()
  }

  def operationData(operation: KyuubiOperation): OperationData = {
    val opEvent = operation.getOperationEvent
    new OperationData(
      opEvent.statementId,
      opEvent.remoteId,
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
      operation.metrics.asJava,
      operationProgress(operation))
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

  def serverEvent(serverEvent: KyuubiServerInfoEvent): KyuubiServerEvent = {
    if (serverEvent == null) return new KyuubiServerEvent()
    new KyuubiServerEvent(
      serverEvent.serverName,
      serverEvent.startTime,
      serverEvent.eventTime,
      serverEvent.state,
      serverEvent.serverIP,
      serverEvent.serverConf.asJava,
      serverEvent.serverEnv.asJava)
  }

  def logAndRefineErrorMsg(errorMsg: String, throwable: Throwable): String = {
    error(errorMsg, throwable)
    s"$errorMsg: ${Utils.prettyPrint(throwable)}"
  }
}
