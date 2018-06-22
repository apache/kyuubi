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

package yaooqinn.kyuubi.operation

import java.io.CharArrayWriter

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.ql.session.OperationLog
import org.apache.log4j._
import org.apache.log4j.spi.{Filter, LoggingEvent}

import yaooqinn.kyuubi.Logging

class LogDivertAppender extends WriterAppender with Logging {

  private var operationManager: OperationManager = _

  private class NameFilter(
       var operationManager: OperationManager) extends Filter {
    override def decide(ev: LoggingEvent): Int = {
      val log = operationManager.getOperationLog
      if (log == null) return Filter.DENY
      val currentLoggingMode = log.getOpLoggingLevel
      // If logging is disabled, deny everything.
      if (currentLoggingMode == OperationLog.LoggingLevel.NONE) return Filter.DENY
      Filter.NEUTRAL
    }
  }

  /** This is where the log message will go to */
  private val writer = new CharArrayWriter

  private def initLayout(): Unit = {
    // There should be a ConsoleAppender. Copy its Layout.
    var layout: Layout = null
    Logger.getRootLogger.getAllAppenders.asScala.foreach { ap =>
      if (ap.isInstanceOf[ConsoleAppender]) layout = ap.asInstanceOf[Appender].getLayout
    }
    this.layout =
      Option(layout).getOrElse(new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n"))
  }

  def this(operationManager: OperationManager) {
    this()
    initLayout()
    setWriter(writer)
    setName("SparkLogDivertAppender")
    this.operationManager = operationManager
    addFilter(new NameFilter(operationManager))
  }

  /**
   * Overrides WriterAppender.subAppend(), which does the real logging. No need
   * to worry about concurrency since log4j calls this synchronously.
   */
  override protected def subAppend(event: LoggingEvent): Unit = {
    super.subAppend(event)
    // That should've gone into our writer. Notify the LogContext.
    val logOutput = writer.toString
    writer.reset()
    val log = operationManager.getOperationLog
    if (log == null) {
      debug(" ---+++=== Dropped log event from thread " + event.getThreadName)
      return
    }
    log.writeOperationLog(logOutput)
  }
}
