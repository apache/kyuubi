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
package org.apache.kyuubi.ctl.cmd.log

import scala.collection.JavaConverters._

import org.apache.kyuubi.client.BatchRestApi
import org.apache.kyuubi.client.api.v1.dto.{Batch, OperationLog}
import org.apache.kyuubi.ctl.CliConfig
import org.apache.kyuubi.ctl.RestClientFactory.withKyuubiRestClient
import org.apache.kyuubi.ctl.cmd.Command

class LogBatchCommand(cliConfig: CliConfig) extends Command(cliConfig) {

  override def validateArguments(): Unit = {
    if (cliArgs.batchOpts.batchId == null) {
      fail("Must specify batchId for log batch command.")
    }
  }

  override def run(): Unit = {
    withKyuubiRestClient(cliArgs, null, conf) { kyuubiRestClient =>
      val batchRestApi: BatchRestApi = new BatchRestApi(kyuubiRestClient)

      var log: OperationLog = batchRestApi.getBatchLocalLog(
        cliArgs.batchOpts.batchId,
        cliArgs.batchOpts.from,
        cliArgs.batchOpts.size)
      log.getLogRowSet.asScala.foreach(x => info(x))

      var done = false
      val batchId = cliArgs.batchOpts.batchId
      var batch: Batch = null
      if (cliArgs.logOpts.forward) {
        while (!done) {
          log = batchRestApi.getBatchLocalLog(
            batchId,
            -1,
            cliArgs.batchOpts.size)
          log.getLogRowSet.asScala.foreach(x => info(x))

          batch = batchRestApi.getBatchById(batchId)
          if (log.getLogRowSet.size() == 0 && batch.getState() != "RUNNING") {
            done = true
          }
        }
      }
    }
  }

}
