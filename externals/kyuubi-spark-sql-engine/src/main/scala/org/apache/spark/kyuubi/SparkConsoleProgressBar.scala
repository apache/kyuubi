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

package org.apache.spark.kyuubi

import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.{Locale, Timer, TimerTask}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.kyuubi.Logging
import org.apache.kyuubi.operation.Operation

class SparkConsoleProgressBar(
    operation: Operation,
    liveStages: ConcurrentHashMap[SparkStageAttempt, SparkStageInfo],
    updatePeriodMSec: Long,
    timeFormat: String)
  extends Logging {
  // Carriage return
  private val CR = '\r'
  // Delay to show up a progress bar, in milliseconds
  private val firstDelayMSec = 500L

  // The width of terminal
  private val TerminalWidth = sys.env.getOrElse("COLUMNS", "80").toInt

  private var lastUpdateTime = 0L
  private var lastProgressBar = ""

  val dtFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(timeFormat)
    .withLocale(Locale.getDefault).withZone(ZoneId.systemDefault)

  // Schedule a refresh thread to run periodically
  private val timer = new Timer(
    "refresh progress for " +
      operation.getHandle.identifier.toString,
    true)
  timer.schedule(
    new TimerTask {
      override def run(): Unit = {
        refresh()
      }
    },
    firstDelayMSec,
    updatePeriodMSec)

  /**
   * Try to refresh the progress bar in every cycle
   */
  private def refresh(): Unit = {
    val now = System.currentTimeMillis()
    val stages = liveStages.values.asScala.toList.sortBy(_.stageId)
    if (stages.nonEmpty) {
      show(now, stages.take(3)) // display at most 3 stages in same time
    }
  }

  /**
   * Show progress bar in console. The progress bar is displayed in the next line
   * after your last output, keeps overwriting itself to hold in one line. The logging will follow
   * the progress bar, then progress bar will be showed in next line without overwrite logs.
   */
  private def show(now: Long, stages: Seq[SparkStageInfo]): Unit = {
    val width = TerminalWidth / stages.size
    val bar = stages.map { s =>
      val total = s.numTasks
      val header = s"[Stage ${s.stageId}:"
      val tailer = s"(${s.numCompleteTasks} + ${s.numActiveTasks}) / $total]"
      val w = width - header.length - tailer.length
      val bar =
        if (w > 0) {
          val percent = w * s.numCompleteTasks.get / total
          (0 until w).map { i =>
            if (i < percent) "=" else if (i == percent) ">" else " "
          }.mkString("")
        } else {
          ""
        }
      header + bar + tailer
    }.mkString("")

    // only refresh if it's changed OR after 1 minute (or the ssh connection will be closed
    // after idle some time)
    if (bar != lastProgressBar || now - lastUpdateTime > 60 * 1000L) {
      operation.getOperationLog.foreach(log => {
        log.write(dtFormatter.format(Instant.ofEpochMilli(now)) + ' ' + bar + CR)
      })
      lastUpdateTime = now
    }
    lastProgressBar = bar
  }

  /**
   * Clear the progress bar if showed.
   */
  private def clear(): Unit = {
    if (lastProgressBar.nonEmpty) {
      operation.getOperationLog.foreach(log => {
        log.write(" " * TerminalWidth + CR)
      })
      lastProgressBar = ""
    }
  }

  /**
   * Mark all the stages as finished, clear the progress bar if showed
   */
  def finish(): Unit = synchronized {
    clear()
    timer.cancel()
  }
}
