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

package org.apache.kyuubi.engine

import org.apache.curator.utils.ZKPaths

import org.apache.kyuubi.engine.ShareLevel.{CONNECTION, ShareLevel}
import org.apache.kyuubi.session.SessionHandle

case class SQLEngineAppName(
    sharedLevel: ShareLevel, user: String, sessionId: String) {
  override def toString: String = s"kyuubi_${sharedLevel}_${user}_$sessionId"

  def getZkNamespace(prefix: String): String = {
    sharedLevel match {
      case CONNECTION => ZKPaths.makePath(s"${prefix}_$sharedLevel", user, sessionId)
      case _ => ZKPaths.makePath(s"${prefix}_$sharedLevel", user)
    }
  }

  def getZkLockPath(prefix: String): String = {
    assert(sharedLevel != CONNECTION)
    sharedLevel match {
      case _ => ZKPaths.makePath(s"${prefix}_$sharedLevel", "lock", user)
    }
  }
}

private[kyuubi] object SQLEngineAppName {
  def apply(sharedLevel: ShareLevel, user: String, handle: SessionHandle): SQLEngineAppName = {
    SQLEngineAppName(sharedLevel, user, handle.identifier.toString)
  }
}
