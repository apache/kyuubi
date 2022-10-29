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
import org.apache.kyuubi.client.util.BatchUtils
import org.apache.kyuubi.ctl.CtlConf._
import org.apache.kyuubi.ctl.RestClientFactory.{withKyuubiInstanceRestClient, withKyuubiRestClient}
import org.apache.kyuubi.ctl.cmd.Command
import org.apache.kyuubi.ctl.opt.CliConfig
import org.apache.kyuubi.ctl.util.Render

class LogBatchCommand(
    cliConfig: CliConfig,
    batch: Option[Batch] = None,
    restConfigMap: JMap[String, Object] = null)
  extends Command[Batch](cliConfig) {

  def validate(): Unit = {
    if (normalizedCliConfig.batchOpts.batchId == null) {
      fail("Must specify batchId for log batch command.")
    }
  }

  def doRun(): Batch = {
    withKyuubiRestClient(normalizedCliConfig, restConfigMap, conf) { kyuubiRestClient =>
      val batchRestApi: BatchRestApi = new BatchRestApi(kyuubiRestClient)
      val batchId = normalizedCliConfig.batchOpts.batchId
      val size = normalizedCliConfig.batchOpts.size

      @volatile
      var log: OperationLog = null
      @volatile
      var done = false
      var batch = this.batch.getOrElse(batchRestApi.getBatchById(batchId))
      val kyuubiInstance = batch.getKyuubiInstance

      withKyuubiInstanceRestClient(kyuubiRestClient, kyuubiInstance) { kyuubiInstanceRestClient =>
        val kyuubiInstanceBatchRestApi: BatchRestApi = new BatchRestApi(kyuubiInstanceRestClient)

        val logThread = new Thread("batch-log-thread") {
          override def run(): Unit = {
            var from = math.max(normalizedCliConfig.batchOpts.from, 0)
            // if it has been done and the last fetched log is empty, break the log loop
            while (!(done && log != null && log.getRowCount == 0)) {
              try {
                log = kyuubiInstanceBatchRestApi.getBatchLocalLog(
                  batchId,
                  from,
                  size)
                from += log.getLogRowSet.size
                log.getLogRowSet.asScala.foreach(x => info(x))
              } catch {
                case e: Exception => error(s"Error fetching batch logs: ${e.getMessage}")
              }

              // if it has been done and the last fetched logs is non empty, do not wait a interval
              if (!done || log.getRowCount > 0) {
                Thread.sleep(conf.get(CTL_BATCH_LOG_QUERY_INTERVAL))
              }
            }
          }
        }
        logThread.setDaemon(true)
        logThread.start()

        while (!done) {
          try {
            val (latestBatch, shouldFinishLog) =
              checkStatus(kyuubiInstanceBatchRestApi, batchId, log)
            batch = latestBatch
            done = shouldFinishLog
          } catch {
            case e: Exception =>
              val (latestBatch, shouldFinishLog) = checkStatus(batchRestApi, batchId, log)
              batch = latestBatch
              done = shouldFinishLog
              if (done) {
                error(s"Error checking batch state: ${e.getMessage}")
              }
          }

          if (!done) {
            Thread.sleep(conf.get(CTL_BATCH_LOG_QUERY_INTERVAL))
          }
        }

        // if the batch state is not failed, interrupt the log thread
        if (!BatchUtils.isTerminalState(batch.getState) || BatchUtils.isFinishedState(
            batch.getState)) {
          logThread.interrupt()
        }

        logThread.join(conf.get(CTL_BATCH_LOG_THREAD_TIMEOUT))
      }
      batch
    }
  }

  def render(batch: Batch): Unit = {
    if (normalizedCliConfig.logOpts.forward) {
      info(Render.renderBatchInfo(batch))
    }
  }

  private def checkStatus(
      batchRestApi: BatchRestApi,
      batchId: String,
      log: OperationLog): (Batch, Boolean) = {
    var batch: Batch = null

    if (!normalizedCliConfig.logOpts.forward) {
      return (batch, true)
    }

    if (normalizedCliConfig.batchOpts.waitCompletion) {
      if (log == null || log.getLogRowSet.size == 0) {
        batch = batchRestApi.getBatchById(batchId)
        if (BatchUtils.isTerminalState(batch.getState)) {
          return (batch, true)
        }
      }
    } else {
      batch = batchRestApi.getBatchById(batchId)
      if (!BatchUtils.isPendingState(batch.getState)) {
        return (batch, true)
      }
    }

    (batch, false)
  }
}
