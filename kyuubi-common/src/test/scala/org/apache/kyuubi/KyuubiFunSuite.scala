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

package org.apache.kyuubi

import scala.collection.mutable.ArrayBuffer

// scalastyle:off
import org.apache.log4j.{Appender, AppenderSkeleton, Level, Logger}
import org.apache.log4j.spi.LoggingEvent
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite, Outcome}
import org.scalatest.concurrent.Eventually

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.internal.Tests.IS_TESTING

trait KyuubiFunSuite extends FunSuite
  with BeforeAndAfterAll
  with BeforeAndAfterEach
  with Eventually
  with ThreadAudit
  with Logging {
  // scalastyle:on
  System.setProperty(KyuubiConf.FRONTEND_BIND_HOST.key, "127.0.0.1")
  override def beforeAll(): Unit = {
    System.setProperty(IS_TESTING.key, "true")
    doThreadPostAudit()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    doThreadPostAudit()
  }

  final override def withFixture(test: NoArgTest): Outcome = {
    val testName = test.text
    val suiteName = this.getClass.getName
    val shortSuiteName = suiteName.replaceAll("org\\.apache\\.kyuubi", "o\\.a\\.k")
    try {
      info(s"\n\n===== TEST OUTPUT FOR $shortSuiteName: '$testName' =====\n")
      test()
    } finally {
      info(s"\n\n===== FINISHED $shortSuiteName: '$testName' =====\n")
    }
  }

  /**
   * Adds a log appender and optionally sets a log level to the root logger or the logger with
   * the specified name, then executes the specified function, and in the end removes the log
   * appender and restores the log level if necessary.
   */
  protected def withLogAppender(
      appender: Appender,
      loggerName: Option[String] = None,
      level: Option[Level] = None)(
    f: => Unit): Unit = {
    val logger = loggerName.map(Logger.getLogger).getOrElse(Logger.getRootLogger)
    val restoreLevel = logger.getLevel
    logger.addAppender(appender)
    if (level.isDefined) {
      logger.setLevel(level.get)
    }
    try f finally {
      logger.removeAppender(appender)
      if (level.isDefined) {
        logger.setLevel(restoreLevel)
      }
    }
  }

  class LogAppender(msg: String = "", maxEvents: Int = 1000) extends AppenderSkeleton {
    val loggingEvents = new ArrayBuffer[LoggingEvent]()

    override def append(loggingEvent: LoggingEvent): Unit = {
      if (loggingEvents.size >= maxEvents) {
        val loggingInfo = if (msg == "") "." else s" while logging $msg."
        throw new IllegalStateException(
          s"Number of events reached the limit of $maxEvents$loggingInfo")
      }
      loggingEvents.append(loggingEvent)
    }
    override def close(): Unit = {}
    override def requiresLayout(): Boolean = false
  }
}
