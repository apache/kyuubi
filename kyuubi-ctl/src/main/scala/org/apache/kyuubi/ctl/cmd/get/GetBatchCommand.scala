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
package org.apache.kyuubi.ctl.cmd.get

import org.apache.kyuubi.client.{BatchRestApi, KyuubiRestClient}
import org.apache.kyuubi.client.api.v1.dto.Batch
import org.apache.kyuubi.client.util.JsonUtil
import org.apache.kyuubi.ctl.{CliConfig, ClientFactory}
import org.apache.kyuubi.ctl.cmd.Command

class GetBatchCommand(cliConfig: CliConfig) extends Command(cliConfig) {

  override def validateArguments(): Unit = {
    if (cliArgs.batchOpts.batchId == null) {
      fail("Must specify batchId for get batch command.")
    }
  }

  override def run(): Unit = {
    val kyuubiRestClient: KyuubiRestClient = ClientFactory.getKyuubiRestClient(cliArgs, null)
    val batchRestApi: BatchRestApi = new BatchRestApi(kyuubiRestClient)

    val batch: Batch = batchRestApi.getBatchById(cliArgs.batchOpts.batchId)
    info(JsonUtil.toJson(batch))

    kyuubiRestClient.close()
  }

}
