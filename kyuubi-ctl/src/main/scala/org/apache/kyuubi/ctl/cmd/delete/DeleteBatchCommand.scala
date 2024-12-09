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
package org.apache.kyuubi.ctl.cmd.delete

import org.apache.kyuubi.client.BatchRestApi
import org.apache.kyuubi.client.api.v1.dto.Batch
import org.apache.kyuubi.client.util.{BatchUtils, JsonUtils}
import org.apache.kyuubi.ctl.ControlCliException
import org.apache.kyuubi.ctl.RestClientFactory.withKyuubiRestClient
import org.apache.kyuubi.ctl.cmd.Command
import org.apache.kyuubi.ctl.opt.CliConfig

class DeleteBatchCommand(cliConfig: CliConfig) extends Command[Batch](cliConfig) {
  def validate(): Unit = {
    if (normalizedCliConfig.batchOpts.batchId == null) {
      fail("Must specify batchId for delete batch command.")
    }
  }

  def doRun(): Batch = {
    withKyuubiRestClient(normalizedCliConfig, null, conf) { kyuubiRestClient =>
      val batchRestApi: BatchRestApi = new BatchRestApi(kyuubiRestClient)
      val batchId = normalizedCliConfig.batchOpts.batchId

      val result = batchRestApi.deleteBatch(batchId)

      info(JsonUtils.toPrettyJson(result))

      if (!result.isSuccess) {
        val batch = batchRestApi.getBatchById(batchId)
        if (!BatchUtils.isTerminalState(batch.getState)) {
          error(s"Failed to delete batch ${batch.getId}, its current state is ${batch.getState}")
          throw ControlCliException(1)
        } else {
          warn(s"Batch ${batch.getId} is already in terminal state ${batch.getState}.")
        }
      }

      null
    }
  }

  def render(batch: Batch): Unit = {}
}
