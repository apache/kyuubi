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

/**
 * The default engine name
 *
 * @param sharedLevel
 * @param user
 * @param sessionId
 */
case class EngineName(
    sharedLevel: ShareLevel,
    user: String,
    sessionId: String,
    subDomain: Option[String]) {

  val defaultEngineName: String = sharedLevel match {
    case CONNECTION => s"kyuubi_${sharedLevel}_${user}_$sessionId"
    case _ => subDomain match {
      case Some(domain) => s"kyuubi_${sharedLevel}_${user}_${domain}_$sessionId"
      case _ => s"kyuubi_${sharedLevel}_${user}_$sessionId"
    }
  }

  def getEngineSpace(prefix: String): String = {
    sharedLevel match {
      case CONNECTION => ZKPaths.makePath(s"${prefix}_$sharedLevel", user, sessionId)
      case _ => subDomain match {
        case Some(domain) => ZKPaths.makePath(s"${prefix}_$sharedLevel", user, domain)
        case None => ZKPaths.makePath(s"${prefix}_$sharedLevel", user)
      }
    }
  }

  def getZkLockPath(prefix: String): String = {
    assert(sharedLevel != CONNECTION)
    subDomain match {
      case Some(domain) => ZKPaths.makePath(s"${prefix}_$sharedLevel", "lock", user, domain)
      case None => ZKPaths.makePath(s"${prefix}_$sharedLevel", "lock", user)
    }
  }
}

private[kyuubi] object EngineName {
  def apply(
    sharedLevel: ShareLevel,
    user: String,
    handle: SessionHandle,
    subDomain: Option[String]): EngineName = {
    new EngineName(sharedLevel, user, handle.identifier.toString, subDomain)
  }
}
