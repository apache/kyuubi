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
import org.apache.kyuubi.client.util.JsonUtil
import org.apache.kyuubi.ctl.CliConfig
import org.apache.kyuubi.ctl.RestClientFactory.withKyuubiRestClient
import org.apache.kyuubi.ctl.cmd.Command

class DeleteBatchCommand(cliConfig: CliConfig) extends Command(cliConfig) {

  var result: String = null

  def validateArguments(): Unit = {}

  override def run(): Unit = {
    withKyuubiRestClient(cliArgs, null, conf) { kyuubiRestClient =>
      val batchRestApi: BatchRestApi = new BatchRestApi(kyuubiRestClient)

      val result = batchRestApi.deleteBatch(
        cliArgs.batchOpts.batchId,
        cliArgs.batchOpts.hs2ProxyUser)
      info(JsonUtil.toJson(result))
    }
  }

}
