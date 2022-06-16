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
package org.apache.kyuubi.ctl.cmd.create

import java.util.{ArrayList, HashMap}

import org.apache.kyuubi.client.BatchRestApi
import org.apache.kyuubi.client.api.v1.dto.{Batch, BatchRequest}
import org.apache.kyuubi.ctl.CliConfig
import org.apache.kyuubi.ctl.RestClientFactory.withKyuubiRestClient
import org.apache.kyuubi.ctl.cmd.Command
import org.apache.kyuubi.ctl.util.{CtlUtils, Render, Validator}

class CreateBatchCommand(cliConfig: CliConfig) extends Command[Batch](cliConfig) {

  def validate(): Unit = {
    Validator.validateFilename(normalizedCliConfig)
  }

  def doRun(): Batch = {
    val map = CtlUtils.loadYamlAsMap(normalizedCliConfig)

    withKyuubiRestClient(normalizedCliConfig, map, conf) { kyuubiRestClient =>
      val batchRestApi: BatchRestApi = new BatchRestApi(kyuubiRestClient)

      val request = map.get("request").asInstanceOf[HashMap[String, Object]]
      val batchRequest = new BatchRequest(
        map.get("batchType").asInstanceOf[String],
        request.get("resource").asInstanceOf[String],
        request.get("className").asInstanceOf[String],
        request.get("name").asInstanceOf[String],
        request.get("configs").asInstanceOf[HashMap[String, String]],
        request.get("args").asInstanceOf[ArrayList[String]])

      batchRestApi.createBatch(batchRequest)
    }
  }

  def render(batch: Batch): Unit = {
    info(Render.renderBatchInfo(batch))
  }
}
