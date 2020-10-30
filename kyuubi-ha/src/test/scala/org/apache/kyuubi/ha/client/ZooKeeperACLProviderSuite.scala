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

import org.apache.zookeeper.ZooDefs

import org.apache.kyuubi.KerberizedTestHelper

class ZooKeeperACLProviderSuite extends KerberizedTestHelper {

  test("acl for zookeeper") {
    val provider = new ZooKeeperACLProvider()
    val acl = provider.getDefaultAcl
    assert(acl.size() === 1)
    assert(acl === ZooDefs.Ids.OPEN_ACL_UNSAFE)

    tryWithSecurityEnabled {
      val acl1 = new ZooKeeperACLProvider().getDefaultAcl
      assert(acl1.size() === 2)
      val expected = ZooDefs.Ids.READ_ACL_UNSAFE
      expected.addAll(ZooDefs.Ids.CREATOR_ALL_ACL)
      assert(acl1 === expected)
    }
  }
}
