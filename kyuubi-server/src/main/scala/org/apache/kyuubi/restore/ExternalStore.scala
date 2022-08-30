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
package org.apache.kyuubi.restore

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.operation.{KyuubiOperation, OperationHandle}
import org.apache.kyuubi.restore.kvstore.KVStore
import org.apache.kyuubi.restore.operation.{RestoredExecuteStatement, RestoredKyuubiOperation}
import org.apache.kyuubi.restore.session.RestoredKyuubiSessionImpl
import org.apache.kyuubi.server.KyuubiServer
import org.apache.kyuubi.session.{KyuubiSession, KyuubiSessionImpl, KyuubiSessionManager, SessionHandle}

abstract class ExternalStore[T, E <: StoreEntity](store: KVStore) {

  def get(key: String)(implicit tag: ClassTag[E]): Option[T] = {
    val value = store.get[E](key, tag.runtimeClass.asInstanceOf[Class[E]]).map(restore(key, _))
    if (value.isDefined) {
      markHoldServer(key)
    }
    value
  }

  def set(key: String, value: T): Unit = {
    store.set(key, entity(value))
    markHoldServer(key)
  }

  def remove(key: String): Unit = {
    store.remove(key)
    store.remove(getHoldKey(key))
  }

  protected def entity(value: T): E

  protected def restore(key: String, e: E): T

  private lazy val serverId: String =
    KyuubiServer.kyuubiServer.frontendServices.head.connectionUrl

  def getHoldKey(key: String): String = s"${key}__${serverId}__HANDLER_SERVER__"

  def isHold(key: String): Boolean = {
    val handler = store.get[String](getHoldKey(key), classOf[String])
    handler.isDefined
  }

  private def markHoldServer(key: String): Unit = {
    store.set(getHoldKey(key), "")
  }
}

class OperationExternalStore(store: KVStore, sessionManager: KyuubiSessionManager)
  extends ExternalStore[KyuubiOperation, OperationStoreEntity](store) {
  override protected def entity(operation: KyuubiOperation): OperationStoreEntity = {
    OperationStoreEntity(
      operation.getHandle.identifier.toString,
      operation.getSession.handle.identifier.toString,
      Option(operation.remoteOpHandle())
        .map(OperationHandle(_).identifier.toString).getOrElse(""),
      operation.shouldRunAsync,
      operation.getClass.getSimpleName)
  }

  override protected def restore(key: String, entity: OperationStoreEntity): KyuubiOperation = {
    val operationHandle = OperationHandle(key)
    if (sessionManager.operationManager.getOperationOption(operationHandle).isEmpty) {
      this.synchronized {
        if (sessionManager.operationManager.getOperationOption(operationHandle).isEmpty) {
          val sessionHandle = SessionHandle.fromUUID(entity.sessionHandle)
          val session = sessionManager.getSession(sessionHandle)
          val operation = entity.operationType match {
            case "ExecuteStatement" =>
              new RestoredExecuteStatement(
                session,
                entity.operationHandle,
                entity.remoteOperationHandle,
                entity.shouldRunAsync)
            case _ =>
              new RestoredKyuubiOperation(
                session,
                entity.operationHandle,
                entity.remoteOperationHandle)
          }
          operation.reopen()
          sessionManager.operationManager.addOperation(operation)
        }
      }
    }
    sessionManager.operationManager.getOperationOption(operationHandle)
      .get.asInstanceOf[KyuubiOperation]
  }
}

class SessionExternalStore(store: KVStore, sessionManager: KyuubiSessionManager)
  extends ExternalStore[KyuubiSession, SessionStoreEntity](store) {
  override protected def entity(session: KyuubiSession): SessionStoreEntity = {
    SessionStoreEntity(
      session.handle.identifier.toString,
      session.asInstanceOf[KyuubiSessionImpl].engine.getEngineSpace(),
      session.asInstanceOf[KyuubiSessionImpl].engineSessionHandle.identifier.toString,
      session.protocol.getValue,
      session.user,
      session.password,
      session.ipAddress,
      session.conf.asJava)
  }

  override protected def restore(key: String, entity: SessionStoreEntity): KyuubiSession = {
    val sessionHandle = SessionHandle.fromUUID(key)
    if (sessionManager.getSessionOption(sessionHandle).isEmpty) {
      this.synchronized {
        if (sessionManager.getSessionOption(sessionHandle).isEmpty) {
          val session = new RestoredKyuubiSessionImpl(
            TProtocolVersion.findByValue(entity.protocolVersion),
            entity.user,
            entity.password,
            entity.ipAddress,
            entity.conf.asScala.toMap,
            sessionManager,
            sessionManager.getConf.getUserDefaults(entity.user),
            entity.engineSpace,
            entity.engineSessionHandle,
            entity.sessionHandle)
          session.reopen()
          sessionManager.setSession(sessionHandle, session)
        }
      }
    }
    sessionManager.getSessionOption(sessionHandle).get.asInstanceOf[KyuubiSession]
  }
}
