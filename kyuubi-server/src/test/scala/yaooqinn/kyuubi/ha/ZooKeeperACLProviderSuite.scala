/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.ha

import org.apache.spark.SparkFunSuite
import org.apache.zookeeper.ZooDefs
import org.scalatest.Matchers

import yaooqinn.kyuubi.SecuredFunSuite

class ZooKeeperACLProviderSuite extends SparkFunSuite with Matchers with SecuredFunSuite {

  test("") {
    val provider = new ZooKeeperACLProvider()
    provider.getDefaultAcl should be(ZooDefs.Ids.OPEN_ACL_UNSAFE)
    provider.getAclForPath("") should be(ZooDefs.Ids.OPEN_ACL_UNSAFE)

    tryWithSecurityEnabled {
      assert(provider.getDefaultAcl.containsAll(ZooDefs.Ids.READ_ACL_UNSAFE))
      assert(provider.getDefaultAcl.containsAll(ZooDefs.Ids.CREATOR_ALL_ACL))
    }
  }

}
