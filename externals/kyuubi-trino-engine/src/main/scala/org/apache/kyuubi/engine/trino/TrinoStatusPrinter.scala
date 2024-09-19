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

package org.apache.kyuubi.engine.trino

import java.util.OptionalDouble
import java.util.concurrent.TimeUnit._

import io.airlift.units.DataSize
import io.airlift.units.Duration
import io.trino.client.StatementClient

import org.apache.kyuubi.engine.trino.ProgressFormatUtils._
import org.apache.kyuubi.operation.log.OperationLog

/**
 * Copy from trino-cli
 *   1. use OperationLog instead PrintStream to out status info
 */
object TrinoStatusPrinter {

  def printStatusInfo(
      client: StatementClient,
      operationLog: OperationLog,
      debug: Boolean = false,
      lastStats: OptionalDouble = null): OptionalDouble = {
    val out = new TrinoConsoleProgressBar(operationLog)
    val results =
      if (client.isRunning) {
        client.currentStatusInfo()
      } else {
        client.finalStatusInfo()
      }

    val stats = results.getStats

    if (lastStats != null &&
      stats.getProgressPercentage.equals(lastStats)) {
      return lastStats
    }

    val wallTime = Duration.succinctDuration(stats.getElapsedTimeMillis(), MILLISECONDS)

    val nodes = stats.getNodes
    if ((nodes == 0) || (stats.getTotalSplits == 0)) {
      return stats.getProgressPercentage
    }

    // Query 12, FINISHED, 1 node
    val querySummary = s"Query ${results.getId}, ${stats.getState}," +
      s" ${nodes.formatted("%,d")} ${pluralize("node", nodes)}"
    out.printLine(querySummary)

    if (debug) {
      out.printLine(results.getInfoUri.toString)
    }

    // Splits: 1000 total, 842 done (84.20%)
    val splitsSummary = s"Splits: ${stats.getTotalSplits.formatted("%,d")} total," +
      s" ${stats.getCompletedSplits.formatted("%,d")}" +
      s" done (${stats.getProgressPercentage.orElse(0.0).formatted("%.2f")}%)"
    out.printLine(splitsSummary)

    if (debug) {
      // CPU Time: 565.2s total,   26K rows/s, 3.85MB/s
      val cpuTime = new Duration(stats.getCpuTimeMillis(), MILLISECONDS)
      val cpuTimeSummary = s"CPU Time: ${cpuTime.getValue(SECONDS).formatted("%.1f")}s total, " +
        s"${formatCountRate(
            stats.getProcessedRows(),
            cpuTime,
            false).formatted("%5s")} rows/s, " +
        s"${formatDataRate(
            DataSize.ofBytes(stats.getProcessedBytes),
            cpuTime,
            true).formatted("%8s")}, " +
        s"${percentage(
            stats.getCpuTimeMillis(),
            stats.getWallTimeMillis()).formatted("%d")}% active"
      out.printLine(cpuTimeSummary)

      val parallelism = cpuTime.getValue(MILLISECONDS) / wallTime.getValue(MILLISECONDS)

      // Per Node: 3.5 parallelism, 83.3K rows/s, 0.7 MB/s
      val perNodeSummary = s"Per Node: ${(parallelism / nodes).formatted("%.1f")} parallelism, " +
        s"${formatCountRate(
            stats.getProcessedRows().toDouble / nodes,
            wallTime,
            false).formatted("%5s")} rows/s, " +
        s"${formatDataRate(
            DataSize.ofBytes(stats.getProcessedBytes() / nodes),
            wallTime,
            true).formatted("%8s")}"
      out.reprintLine(perNodeSummary)

      // Parallelism: 5.3
      out.printLine(s"Parallelism: ${parallelism.formatted("%.1f")}")

      // Peak Memory: 1.97GB
      out.reprintLine("Peak Memory: " + formatDataSize(
        DataSize.ofBytes(stats.getPeakMemoryBytes()),
        true))

      // Spilled Data: 20GB
      if (stats.getSpilledBytes() > 0) {
        out.reprintLine("Spilled: " + formatDataSize(
          DataSize.ofBytes(stats.getSpilledBytes()),
          true))
      }
    }

    // 0:32 [2.12GB, 15M rows] [67MB/s, 463K rows/s]
    val statsLine = s"${formatFinalTime(wallTime)} " +
      s"[${formatCount(stats.getProcessedRows())} rows, " +
      s"${formatDataSize(DataSize.ofBytes(stats.getProcessedBytes()), true)}] " +
      s"[${formatCountRate(stats.getProcessedRows(), wallTime, false)} rows/s, " +
      s"${formatDataRate(DataSize.ofBytes(stats.getProcessedBytes()), wallTime, true)}]"
    out.printLine(statsLine)
    stats.getProgressPercentage
  }

  def percentage(count: Double, total: Double): Int = {
    if (total == 0) 0 else Math.min(100, (count * 100.0) / total).toInt
  }

}
