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

package org.apache.kyuubi.operation.log

import java.io.CharArrayWriter

import scala.collection.JavaConverters._

import org.apache.log4j._
import org.apache.log4j.spi.{Filter, LoggingEvent}

class LogDivertAppender extends WriterAppender {

  private final val writer = new CharArrayWriter

  private final val lo = Logger.getRootLogger
    .getAllAppenders.asScala
    .find(_.isInstanceOf[ConsoleAppender])
    .map(_.asInstanceOf[Appender].getLayout)
    .getOrElse(new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n"))

  setName("KyuubiSparkSQLEngineLogDivertAppender")
  setWriter(writer)
  setLayout(lo)

  addFilter(new Filter {
    override def decide(loggingEvent: LoggingEvent): Int = {
      if (OperationLog.getCurrentOperationLog == null) Filter.DENY else Filter.NEUTRAL
    }
  })

  /**
   * Overrides WriterAppender.subAppend(), which does the real logging. No need
   * to worry about concurrency since log4j calls this synchronously.
   */
  override protected def subAppend(event: LoggingEvent): Unit = {
    super.subAppend(event)
    // That should've gone into our writer. Notify the LogContext.
    val logOutput = writer.toString
    writer.reset()
    val log = OperationLog.getCurrentOperationLog
    if (log != null) log.write(logOutput)
  }
}

object LogDivertAppender {
  def initialize(): Unit = {
    org.apache.log4j.Logger.getRootLogger.addAppender(new LogDivertAppender())
  }
}
