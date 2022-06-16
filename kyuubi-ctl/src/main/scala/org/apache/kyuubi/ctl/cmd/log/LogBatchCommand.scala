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

import java.util.{Map => JMap}

import scala.collection.JavaConverters._

import org.apache.kyuubi.client.BatchRestApi
import org.apache.kyuubi.client.api.v1.dto.{Batch, OperationLog}
import org.apache.kyuubi.client.exception.KyuubiRestException
import org.apache.kyuubi.ctl.CliConfig
import org.apache.kyuubi.ctl.RestClientFactory.withKyuubiRestClient
import org.apache.kyuubi.ctl.cmd.BatchCommand
import org.apache.kyuubi.ctl.util.BatchUtil

class LogBatchCommand(cliConfig: CliConfig, restConfigMap: JMap[String, Object] = null)
  extends BatchCommand(cliConfig) {

  def validate(): Unit = {
    if (normalizedCliConfig.batchOpts.batchId == null) {
      fail("Must specify batchId for log batch command.")
    }
  }

  override def doRunAndReturnBatchReport(): Batch = {
    withKyuubiRestClient(normalizedCliConfig, restConfigMap, conf) { kyuubiRestClient =>
      val batchRestApi: BatchRestApi = new BatchRestApi(kyuubiRestClient)
      val batchId = normalizedCliConfig.batchOpts.batchId
      var from = math.max(normalizedCliConfig.batchOpts.from, 0)
      val size = normalizedCliConfig.batchOpts.size

      var log: OperationLog = null
      var done = false
      var batch: Batch = null

      while (!done) {
        try {
          log = batchRestApi.getBatchLocalLog(
            batchId,
            from,
            size)
          from += log.getLogRowSet.size
          log.getLogRowSet.asScala.foreach(x => info(x))
          if (!normalizedCliConfig.logOpts.forward) {
            done = true
          }
        } catch {
          case e: KyuubiRestException =>
            error(s"Error fetching batch logs: ${e.getMessage}")
        }

        if (log == null || log.getLogRowSet.size() == 0) {
          batch = batchRestApi.getBatchById(batchId)
          if (BatchUtil.isTerminalState(batch.getState)) {
            done = true
          }
        }

        if (!done) {
          Thread.sleep(DEFAULT_LOG_QUERY_INTERVAL)
        }
      }

      batch
    }
  }
}
