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

package org.apache.kyuubi.session

import java.util.UUID

import org.apache.kyuubi.cli.Handle
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TSessionHandle

case class SessionHandle(identifier: UUID) {

  def toTSessionHandle: TSessionHandle = {
    val tSessionHandle = new TSessionHandle
    tSessionHandle.setSessionId(Handle.toTHandleIdentifier(identifier))
    tSessionHandle
  }

  override def toString: String = s"SessionHandle [$identifier]"
}

object SessionHandle {
  def apply(tHandle: TSessionHandle): SessionHandle = {
    apply(Handle.fromTHandleIdentifier(tHandle.getSessionId))
  }

  def apply(): SessionHandle = new SessionHandle(UUID.randomUUID())

  def fromUUID(uuid: String): SessionHandle = new SessionHandle(UUID.fromString(uuid))
}
