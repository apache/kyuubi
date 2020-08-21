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

package org.apache.kyuubi.ha.client

import org.apache.curator.framework.api.ACLProvider
import org.apache.hadoop.security.UserGroupInformation
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.data.ACL

case class ZooKeeperACLProvider() extends ACLProvider {

  /**
   * Return the ACL list to use by default.
   *
   * @return default ACL list
   */
  override def getDefaultAcl: java.util.List[ACL] = {
    val nodeAcls = new java.util.ArrayList[ACL]
    if (UserGroupInformation.isSecurityEnabled) {
      // Read all to the world
      nodeAcls.addAll(ZooDefs.Ids.READ_ACL_UNSAFE)
      // Create/Delete/Write/Admin to the authenticated user
      nodeAcls.addAll(ZooDefs.Ids.CREATOR_ALL_ACL)
    } else {
      // ACLs for znodes on a non-kerberized cluster
      // Create/Read/Delete/Write/Admin to the world
      nodeAcls.addAll(ZooDefs.Ids.OPEN_ACL_UNSAFE)
    }
    nodeAcls
  }

  override def getAclForPath(path: String): java.util.List[ACL] = getDefaultAcl
}
