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

import java.io.{File, IOException}

import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.spark.{KyuubiConf, KyuubiSparkUtil, SparkConf, SparkFunSuite}
import org.apache.zookeeper.ZooDefs
import org.scalatest.Matchers

import yaooqinn.kyuubi.SecuredFunSuite

class HighAvailableServiceSuite extends SparkFunSuite
  with Matchers
  with SecuredFunSuite
  with ZookeeperFunSuite {

  test("ACL Provider") {
    val aclProvider = HighAvailableService.aclProvider
    aclProvider.getDefaultAcl should be(ZooDefs.Ids.OPEN_ACL_UNSAFE)
    aclProvider.getAclForPath("") should be(ZooDefs.Ids.OPEN_ACL_UNSAFE)

    tryWithSecurityEnabled {
      assert(aclProvider.getDefaultAcl.containsAll(ZooDefs.Ids.READ_ACL_UNSAFE))
      assert(aclProvider.getDefaultAcl.containsAll(ZooDefs.Ids.CREATOR_ALL_ACL))
    }
  }

  test("New Zookeeper Client") {
    val client = HighAvailableService.newZookeeperClient(conf)
    client.getState should be(CuratorFrameworkState.STARTED)
  }

  test("Get Quorum Servers") {
    val conf = new SparkConf(true)
    val e = intercept[IllegalArgumentException](HighAvailableService.getQuorumServers(conf))
    e.getMessage should startWith(KyuubiConf.HA_ZOOKEEPER_QUORUM.key)
    val host1 = "127.0.0.1:1234"
    val host2 = "127.0.0.1"
    conf.set(KyuubiConf.HA_ZOOKEEPER_QUORUM.key, host1)
    HighAvailableService.getQuorumServers(conf) should be(host1)
    conf.set(KyuubiConf.HA_ZOOKEEPER_QUORUM.key, host1 + "," + host2)
    HighAvailableService.getQuorumServers(conf) should be(host1 + "," + host2 + ":2181")
    val port = "2180"
    conf.set(KyuubiConf.HA_ZOOKEEPER_CLIENT_PORT.key, port)
    HighAvailableService.getQuorumServers(conf) should be(host1 + "," + host2 + ":" + port)
  }

  test("set up zookeeper auth") {
    tryWithSecurityEnabled {
      val keytab = File.createTempFile("user", "keytab")
      val principal = KyuubiSparkUtil.getCurrentUserName + "/localhost@" + "yaooqinn"

      conf.set(KyuubiSparkUtil.KEYTAB, keytab.getCanonicalPath)
      conf.set(KyuubiSparkUtil.PRINCIPAL, principal)

      HighAvailableService.setUpZooKeeperAuth(conf)

      conf.set(KyuubiSparkUtil.KEYTAB, keytab.getName)

      val e = intercept[IOException](HighAvailableService.setUpZooKeeperAuth(conf))

      e.getMessage should startWith(KyuubiSparkUtil.KEYTAB)
    }
  }
}