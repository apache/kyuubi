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

import org.apache.kyuubi.{Utils, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.spark.SparkBatchProcessBuilder
import org.apache.kyuubi.server.api.v1.{BatchesResource, BatchRequest}

class KyuubiBatchSessionSuite extends WithKyuubiServer {

  override protected val conf: KyuubiConf = KyuubiConf()

  private def batchSession(
      batchRequest: BatchRequest,
      user: String,
      password: String = "anonymous",
      ipAddress: String = Utils.findLocalInetAddress.getHostAddress,
      conf: Map[String, String] = Map.empty): KyuubiBatchSessionImpl = {
    new KyuubiBatchSessionImpl(
      BatchesResource.REST_BATCH_PROTOCOL,
      user,
      password,
      ipAddress,
      conf,
      server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager],
      KyuubiConf(false).getUserDefaults(user),
      batchRequest)
  }

  test("propagate session conf") {
    val req = BatchRequest("spark", "a", "b", "c", Map("k1" -> "v1", "k2" -> "v2"))
    val session = batchSession(req, "x")
    assert(session.optimizedConf.get("k1").contains("v1"))
    assert(session.optimizedConf.get("k2").contains("v2"))

    val builder = session.batchJobSubmissionOp.builder.asInstanceOf[SparkBatchProcessBuilder]
    assert(builder.conf.getOption("k1").contains("v1"))
    assert(builder.conf.getOption("k2").contains("v2"))
  }
}
