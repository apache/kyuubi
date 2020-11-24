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

package org.apache.kyuubi.operation

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.KyuubiServer

class KyuubiOperationSuite extends JDBCTests {

  private val conf = KyuubiConf()
    .set(KyuubiConf.FRONTEND_BIND_PORT, 0)
    .set(KyuubiConf.ENGINE_CHECK_INTERVAL, 4000L)
    .set(KyuubiConf.ENGINE_IDLE_TIMEOUT, 10000L)

  private val server: KyuubiServer = KyuubiServer.startServer(conf)

  override def afterAll(): Unit = {
    server.stop()
    super.afterAll()
  }

  override protected def jdbcUrl: String = s"jdbc:hive2://${server.connectionUrl}/;"
}
