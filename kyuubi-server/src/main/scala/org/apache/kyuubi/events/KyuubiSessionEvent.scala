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

package org.apache.kyuubi.events

import org.apache.kyuubi.Utils
import org.apache.kyuubi.server.KyuubiServer
import org.apache.kyuubi.session.KyuubiSessionImpl

/**
 * @param sessionId server session id
 * @param user session user
 * @param clientIp client ip address
 * @param serverIp kyuubi server ip address, it is useful if has multi-instance Kyuubi Server
 * @param historyTag a history tag that can be used to relate the history session (e.g. app name)
 * @param startTime session create time
 * @param state session state, see [[SessionState]]
 * @param stateTime session state update time
 * @param totalOperations how many queries and meta calls
 */
case class KyuubiSessionEvent(
    sessionId: String,
    user: String,
    clientIp: String,
    serverIp: String,
    historyTag: String,
    conf: Map[String, String],
    startTime: Long,
    var state: String,
    var stateTime: Long = -1L,
    var totalOperations: Int = 0) extends KyuubiServerEvent {
  override def partitions: Seq[(String, String)] =
    ("day", Utils.getDateFromTimestamp(startTime)) :: Nil
}

object KyuubiSessionEvent {
  def apply(session: KyuubiSessionImpl): KyuubiSessionEvent = {
    assert(KyuubiServer.kyuubiServer.isDefined)
    val serverAddr = KyuubiServer.kyuubiServer.get.connectionUrl.split(":").head
    KyuubiSessionEvent(
      session.handle.identifier.toString,
      session.user,
      session.ipAddress,
      serverAddr,
      session.historyTag,
      session.sessionConf.getAll,
      session.createTime,
      session.sessionState.toString,
      session.sessionStateTime)
  }
}
