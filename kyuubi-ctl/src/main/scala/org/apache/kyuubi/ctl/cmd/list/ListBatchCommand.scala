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
package org.apache.kyuubi.ctl.cmd.list

import org.apache.kyuubi.client.BatchRestApi
import org.apache.kyuubi.client.api.v1.dto.GetBatchesResponse
import org.apache.kyuubi.ctl.RestClientFactory.withKyuubiRestClient
import org.apache.kyuubi.ctl.cmd.Command
import org.apache.kyuubi.ctl.opt.CliConfig
import org.apache.kyuubi.ctl.util.Render

class ListBatchCommand(cliConfig: CliConfig) extends Command[GetBatchesResponse](cliConfig) {

  def validate(): Unit = {
    if (normalizedCliConfig.batchOpts.createTime < 0) {
      fail(s"Invalid createTime, negative milliseconds are not supported.")
    }
    if (normalizedCliConfig.batchOpts.endTime < 0) {
      fail(s"Invalid endTime, negative milliseconds are not supported.")
    }
    if (normalizedCliConfig.batchOpts.endTime != 0
      && normalizedCliConfig.batchOpts.createTime > normalizedCliConfig.batchOpts.endTime) {
      fail(s"Invalid createTime/endTime, createTime should be less or equal to endTime.")
    }
  }

  def doRun(): GetBatchesResponse = {
    withKyuubiRestClient(normalizedCliConfig, null, conf) { kyuubiRestClient =>
      val batchRestApi: BatchRestApi = new BatchRestApi(kyuubiRestClient)
      val batchOpts = normalizedCliConfig.batchOpts
      batchRestApi.listBatches(
        batchOpts.batchType,
        batchOpts.batchUser,
        batchOpts.batchState,
        batchOpts.batchName,
        batchOpts.createTime,
        batchOpts.endTime,
        if (batchOpts.from < 0) 0 else batchOpts.from,
        batchOpts.size,
        batchOpts.desc)
    }
  }

  def render(batchListInfo: GetBatchesResponse): Unit = {
    info(Render.renderBatchListInfo(batchListInfo))
  }
}
