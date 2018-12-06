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

package yaooqinn.kyuubi.session

import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.{KyuubiConf, SparkFunSuite}
import org.apache.spark.sql.SparkSession

import yaooqinn.kyuubi.SecuredFunSuite
import yaooqinn.kyuubi.server.KyuubiServer

class KyuubiSessionSecuredSuite extends SparkFunSuite with SecuredFunSuite {

  var server: KyuubiServer = _
  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    System.setProperty(KyuubiConf.FRONTEND_BIND_PORT.key, "0")
    System.setProperty("spark.master", "local")
    System.setProperty("spark.hadoop.yarn.resourcemanager.principal", "yarn/_HOST@KENT.KYUUBI.COM")

    server = KyuubiServer.startKyuubiServer()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    System.clearProperty(KyuubiConf.FRONTEND_BIND_PORT.key)
    System.clearProperty("spark.master")
    System.clearProperty("spark.hadoop.yarn.resourcemanager.principal")
    if (server != null) server.stop()
    super.afterAll()
  }


  test("secured ugi test") {
    val be = server.beService
    val sessionMgr = be.getSessionManager
    val operationMgr = sessionMgr.getOperationMgr
    val user = "Kent"
    val passwd = ""
    val ip = ""
    val imper = true
    val proto = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8
    tryWithSecurityEnabled {
      val session =
        new KyuubiSession(proto, user, passwd, server.getConf, ip, imper, sessionMgr, operationMgr)
      assert(session.ugi.getShortUserName === user)
    }
  }
}
