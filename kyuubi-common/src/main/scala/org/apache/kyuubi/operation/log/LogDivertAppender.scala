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

import org.apache.logging.log4j.{Level, LogManager, Marker}
import org.apache.logging.log4j.core.{Filter, LogEvent, Logger, StringLayout}
import org.apache.logging.log4j.core.Filter.Result
import org.apache.logging.log4j.core.LifeCycle.State
import org.apache.logging.log4j.core.appender.{AbstractWriterAppender, ConsoleAppender, WriterManager}
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.message.Message

class LogDivertAppender(
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
    Property.EMPTY_ARRAY,
    new WriterManager(writer, name, layout, true)) {
  def this() = this(
    "KyuubiEngineLogDivertAppender",
    LogDivertAppender.initLayout(),
    null,
    false,
    true,
    LogDivertAppender.writer)

  addFilter(new NameFilter())

  /**
   * Overrides AbstractWriterAppender.append(), which does the real logging. No need
   * to worry about concurrency since log4j calls this synchronously.
   */
  override def append(event: LogEvent): Unit = {
    super.append(event)
    // That should've gone into our writer. Notify the LogContext.
    val logOutput = writer.toString
    writer.reset()
    val log = OperationLog.getCurrentOperationLog
    if (log != null) log.write(logOutput)
  }

  class NameFilter extends Filter {
    private var state: State = State.INITIALIZED

    override def getOnMismatch: Result = Result.NEUTRAL

    override def getOnMatch: Result = Result.NEUTRAL

    // scalastyle:off
    override def filter(
        logger: Logger,
        level: Level,
        marker: Marker,
        msg: String,
        objects: java.lang.Object*): Result = Result.NEUTRAL

    override def filter(logger: Logger, level: Level, marker: Marker, s: String, o: Any): Result =
      Result.NEUTRAL

    override def filter(
        logger: Logger,
        level: Level,
        marker: Marker,
        s: String,
        o: Any,
        o1: Any): Result = Result.NEUTRAL

    override def filter(
        logger: Logger,
        level: Level,
        marker: Marker,
        s: String,
        o: Any,
        o1: Any,
        o2: Any): Result = Result.NEUTRAL

    override def filter(
        logger: Logger,
        level: Level,
        marker: Marker,
        s: String,
        o: Any,
        o1: Any,
        o2: Any,
        o3: Any): Result = Result.NEUTRAL

    override def filter(
        logger: Logger,
        level: Level,
        marker: Marker,
        s: String,
        o: Any,
        o1: Any,
        o2: Any,
        o3: Any,
        o4: Any): Result = Result.NEUTRAL

    override def filter(
        logger: Logger,
        level: Level,
        marker: Marker,
        s: String,
        o: Any,
        o1: Any,
        o2: Any,
        o3: Any,
        o4: Any,
        o5: Any): Result = Result.NEUTRAL

    override def filter(
        logger: Logger,
        level: Level,
        marker: Marker,
        s: String,
        o: Any,
        o1: Any,
        o2: Any,
        o3: Any,
        o4: Any,
        o5: Any,
        o6: Any): Result = Result.NEUTRAL

    override def filter(
        logger: Logger,
        level: Level,
        marker: Marker,
        s: String,
        o: Any,
        o1: Any,
        o2: Any,
        o3: Any,
        o4: Any,
        o5: Any,
        o6: Any,
        o7: Any): Result = Result.NEUTRAL

    override def filter(
        logger: Logger,
        level: Level,
        marker: Marker,
        s: String,
        o: Any,
        o1: Any,
        o2: Any,
        o3: Any,
        o4: Any,
        o5: Any,
        o6: Any,
        o7: Any,
        o8: Any): Result = Result.NEUTRAL

    override def filter(
        logger: Logger,
        level: Level,
        marker: Marker,
        s: String,
        o: Any,
        o1: Any,
        o2: Any,
        o3: Any,
        o4: Any,
        o5: Any,
        o6: Any,
        o7: Any,
        o8: Any,
        o9: Any): Result = Result.NEUTRAL

    override def filter(
        logger: Logger,
        level: Level,
        marker: Marker,
        o: Any,
        throwable: Throwable): Result = Result.NEUTRAL

    override def filter(
        logger: Logger,
        level: Level,
        marker: Marker,
        message: Message,
        throwable: Throwable): Result = Result.NEUTRAL
    // scalastyle:on

    override def filter(event: LogEvent): Result = {
      if (OperationLog.getCurrentOperationLog == null) {
        Filter.Result.DENY
      } else {
        Filter.Result.NEUTRAL
      }
    }

    override def getState: State = state

    override def initialize(): Unit = {
      state = State.INITIALIZED
    }

    override def start(): Unit = {
      state = State.STARTED
    }

    override def stop(): Unit = {
      state = State.STOPPED
    }

    override def isStarted: Boolean = state eq State.STARTED

    override def isStopped: Boolean = state eq State.STOPPED
  }
}

object LogDivertAppender {
  val writer = new CharArrayWriter()

  def initLayout(): StringLayout = {
    LogManager.getRootLogger.asInstanceOf[org.apache.logging.log4j.core.Logger]
      .getAppenders.values().asScala
      .find(ap => ap.isInstanceOf[ConsoleAppender] && ap.getLayout.isInstanceOf[StringLayout])
      .map(_.getLayout.asInstanceOf[StringLayout])
      .getOrElse(PatternLayout.newBuilder().withPattern(
        "%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n").build())
  }

  def initialize(): Unit = {
    val ap = new LogDivertAppender()
    org.apache.logging.log4j.LogManager.getRootLogger()
      .asInstanceOf[org.apache.logging.log4j.core.Logger].addAppender(ap)
    ap.start()
  }
}
