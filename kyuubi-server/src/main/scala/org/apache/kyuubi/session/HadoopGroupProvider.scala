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

import java.util.{Map => JMap}

import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.Logging
import org.apache.kyuubi.plugin.GroupProvider

/**
 * Hadoop based group provider, see more information at
 * https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/GroupsMapping.html
 */
class HadoopGroupProvider extends GroupProvider with Logging {
  override def primaryGroup(user: String, sessionConf: JMap[String, String]): String =
    groups(user, sessionConf).head

  override def groups(user: String, sessionConf: JMap[String, String]): Array[String] =
    UserGroupInformation.createRemoteUser(user).getGroupNames match {
      case Array() =>
        warn(s"There is no group for $user, use the client user name as group directly")
        Array(user)
      case groups => groups
    }
}
