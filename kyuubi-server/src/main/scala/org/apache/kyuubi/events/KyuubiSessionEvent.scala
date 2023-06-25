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
import org.apache.kyuubi.session.KyuubiSession

/**
 * @param sessionId server session id
 * @param clientVersion client version
 * @param sessionType the session type
 * @param sessionName if user not specify it, we use empty string instead
 * @param user session user
 * @param clientIP client ip address
 * @param serverIP A unique Kyuubi server id, e.g. kyuubi server ip address and port,
 *                 it is useful if has multi-instance Kyuubi Server
 * @param conf session config
 * @param eventTime the time when the event made
 * @param startTime session create time
 * @param remoteSessionId remote engine session id
 * @param engineId engine id. For engine on yarn, it is applicationId.
 * @param openedTime session opened time
 * @param endTime session end time
 * @param totalOperations how many queries and meta calls
 * @param exception the session exception, such as the exception that occur when opening session
 */
case class KyuubiSessionEvent(
    sessionId: String,
    clientVersion: Int,
    sessionType: String,
    sessionName: String,
    user: String,
    clientIP: String,
    serverIP: String,
    conf: Map[String, String],
    eventTime: Long,
    startTime: Long,
    var remoteSessionId: String = "",
    var engineId: String = "",
    var openedTime: Long = -1L,
    var endTime: Long = -1L,
    var totalOperations: Int = 0,
    var exception: Option[Throwable] = None) extends KyuubiEvent {
  override def partitions: Seq[(String, String)] =
    ("day", Utils.getDateFromTimestamp(startTime)) :: Nil
}

object KyuubiSessionEvent {
  def apply(session: KyuubiSession): KyuubiSessionEvent = {
    KyuubiSessionEvent(
      session.handle.identifier.toString,
      session.protocol.getValue,
      session.sessionType.toString,
      session.name.getOrElse(""),
      session.user,
      session.ipAddress,
      session.connectionUrl,
      session.conf,
      System.currentTimeMillis(),
      session.createTime)
  }
}
