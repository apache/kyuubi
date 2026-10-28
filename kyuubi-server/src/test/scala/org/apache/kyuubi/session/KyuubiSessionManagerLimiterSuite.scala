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

import scala.collection.JavaConverters._

import org.apache.kyuubi.{KyuubiFunSuite, Utils, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.TClientTestUtils
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TOpenSessionReq

class KyuubiSessionManagerLimiterSuite extends WithKyuubiServer with KyuubiFunSuite {

  private val perUserLimit = 2
  private val restrictKey = "spark.sql.extensions"

  override protected val conf: KyuubiConf = KyuubiConf()
    .set(KyuubiConf.SERVER_LIMIT_CONNECTIONS_PER_USER, perUserLimit)
    .set(KyuubiConf.SESSION_CONF_RESTRICT_LIST, Set(restrictKey))

  test("connection limit count is given back when the session fails before it is registered") {
    // A session carrying a restricted key fails while it is created, before it is put into
    // handleToSession, so closeSession never runs for it. Unless the count openSession took is
    // given back there, every attempt keeps one, and the attempts past the limit are turned away
    // by the limiter rather than by the restrict list - the user stays locked out until the
    // server restarts.
    (1 to perUserLimit + 2).foreach { attempt =>
      TClientTestUtils.withThriftClient(server.frontendServices.head) { client =>
        val req = new TOpenSessionReq()
        req.setUsername(Utils.currentUser)
        req.setPassword("anonymous")
        req.setConfiguration(Map(restrictKey -> "org.apache.kyuubi.NoopExtensions").asJava)
        val errorMessage = client.OpenSession(req).getStatus.getErrorMessage
        assert(
          errorMessage.contains(s"$restrictKey is a restrict key"),
          s"attempt $attempt: $errorMessage")
        assert(
          !errorMessage.contains("Connection limit per user reached"),
          s"attempt $attempt: the connection limit count was not given back")
      }
    }
  }
}
