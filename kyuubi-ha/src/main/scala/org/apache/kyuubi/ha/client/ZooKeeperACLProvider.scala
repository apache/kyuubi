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

import java.util

import org.apache.curator.framework.api.ACLProvider
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.data.ACL

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf

class ZooKeeperACLProvider(conf: KyuubiConf) extends ACLProvider {

  /**
   * ACLs for znodes on a non-kerberized cluster
   * Create/Read/Delete/Write/Admin to the world
   */
  private lazy val OPEN_ACL_UNSAFE = new util.ArrayList[ACL](ZooDefs.Ids.OPEN_ACL_UNSAFE)

  /**
   * Create/Delete/Write/Admin to the authenticated user
   */
  private lazy val AUTH_ACL: util.ArrayList[ACL] = {
    // Read all to the world
    val acls = new util.ArrayList[ACL](ZooDefs.Ids.READ_ACL_UNSAFE)
    // Create/Delete/Write/Admin to the authenticated user
    acls.addAll(ZooDefs.Ids.CREATOR_ALL_ACL)
    acls
  }

  /**
   * Return the ACL list to use by default.
   *
   * @return default ACL list
   */
  override lazy val getDefaultAcl: java.util.List[ACL] = {
    if (conf.get(HighAvailabilityConf.HA_ZK_ACL_ENABLED) &&
      conf.get(HighAvailabilityConf.HA_ZK_ENGINE_REF_ID).isEmpty) {
      AUTH_ACL
    } else if (conf.get(HighAvailabilityConf.HA_ZK_ACL_ENGINE_ENABLED) &&
      conf.get(HighAvailabilityConf.HA_ZK_ENGINE_REF_ID).nonEmpty) {
      AUTH_ACL
    } else {
      OPEN_ACL_UNSAFE
    }
  }

  override def getAclForPath(path: String): java.util.List[ACL] = {
    /**
     * The EngineRef of the server uses InterProcessSemaphoreMutex,
     * which will create a znode with acl,
     * causing the engine to be unable to write to the znode (KeeperErrorCode = NoAuth)
     */
    if (conf.get(HighAvailabilityConf.HA_ZK_ACL_ENABLED) &&
      !conf.get(HighAvailabilityConf.HA_ZK_ACL_ENGINE_ENABLED) &&
      conf.get(HighAvailabilityConf.HA_ZK_ENGINE_REF_ID).isEmpty &&
      path != conf.get(HighAvailabilityConf.HA_ZK_NAMESPACE)) {
      OPEN_ACL_UNSAFE
    } else {
      getDefaultAcl
    }
  }
}
