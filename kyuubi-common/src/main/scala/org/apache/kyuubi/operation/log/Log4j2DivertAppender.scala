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
import java.util.concurrent.locks.ReadWriteLock

import scala.collection.JavaConverters._

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.{Filter, LogEvent, StringLayout}
import org.apache.logging.log4j.core.appender.{AbstractWriterAppender, ConsoleAppender, RollingFileAppender, WriterManager}
import org.apache.logging.log4j.core.filter.AbstractFilter
import org.apache.logging.log4j.core.layout.PatternLayout

import org.apache.kyuubi.util.reflect.ReflectUtils._

class Log4j2DivertAppender(
    name: String,
    layout: StringLayout,
    filter: Filter,
    ignoreExceptions: Boolean,
    immediateFlush: Boolean,
    writer: CharArrayWriter)
  extends AbstractWriterAppender[WriterManager](
    name,
    layout,
    filter,
    ignoreExceptions,
    immediateFlush,
    null,
    new WriterManager(writer, name, layout, true)) {
  def this() = this(
    "KyuubiEngineLogDivertAppender",
    Log4j2DivertAppender.initLayout(),
    null,
    false,
    true,
    new CharArrayWriter())

  addFilter(new AbstractFilter() {
    override def filter(event: LogEvent): Filter.Result = {
      if (OperationLog.getCurrentOperationLog.isDefined) {
        Filter.Result.NEUTRAL
      } else {
        Filter.Result.DENY
      }
    }
  })

  private val writeLock =
    getField[ReadWriteLock]((classOf[AbstractWriterAppender[_]], this), "readWriteLock").writeLock

  /**
   * Overrides AbstractWriterAppender.append(), which does the real logging. No need
   * to worry about concurrency since log4j calls this synchronously.
   */
  override def append(event: LogEvent): Unit = {
    super.append(event)
    writeLock.lock()
    try {
      // That should've gone into our writer. Notify the LogContext.
      val logOutput = writer.toString
      writer.reset()
      OperationLog.getCurrentOperationLog.foreach(_.write(logOutput))
    } finally {
      writeLock.unlock()
    }
  }
}

object Log4j2DivertAppender {
  def initLayout(): StringLayout = {
    LogManager.getRootLogger.asInstanceOf[org.apache.logging.log4j.core.Logger]
      .getAppenders.values().asScala
      .find(ap =>
        (ap.isInstanceOf[ConsoleAppender] || ap.isInstanceOf[RollingFileAppender]) &&
          ap.getLayout.isInstanceOf[StringLayout])
      .map(_.getLayout.asInstanceOf[StringLayout])
      .getOrElse(PatternLayout.newBuilder().withPattern(
        "%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n%ex").build())
  }

  def initialize(): Unit = {
    val ap = new Log4j2DivertAppender()
    org.apache.logging.log4j.LogManager.getRootLogger
      .asInstanceOf[org.apache.logging.log4j.core.Logger].addAppender(ap)
    ap.start()
  }
}
