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

import org.apache.kyuubi.KyuubiFunSuite

trait DiscoveryClientSuite extends KyuubiFunSuite {

  test("parse host and port from instance string") {
    val host = "127.0.0.1"
    val port = 10009
    val instance1 = s"$host:$port"
    val (host1, port1) = DiscoveryClient.parseInstanceHostPort(instance1)
    assert(host === host1)
    assert(port === port1)

    val instance2 = s"hive.server2.thrift.sasl.qop=auth;hive.server2.thrift.bind.host=$host;" +
      s"hive.server2.transport.mode=binary;hive.server2.authentication=KERBEROS;" +
      s"hive.server2.thrift.port=$port;" +
      s"hive.server2.authentication.kerberos.principal=test/_HOST@apache.org"
    val (host2, port2) = DiscoveryClient.parseInstanceHostPort(instance2)
    assert(host === host2)
    assert(port === port2)
  }
}
