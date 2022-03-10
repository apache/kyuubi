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
import org.apache.logging.log4j._
import org.apache.logging.log4j.core.{LogEvent, Logger, LoggerContext}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Outcome}
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.bridge.SLF4JBridgeHandler

import org.apache.kyuubi.config.internal.Tests.IS_TESTING

trait KyuubiFunSuite extends AnyFunSuite
  with BeforeAndAfterAll
  with BeforeAndAfterEach
  with Eventually
  with ThreadAudit
  with Logging {

  // Redirect jul to sl4j
  SLF4JBridgeHandler.removeHandlersForRootLogger()
  SLF4JBridgeHandler.install()

  // scalastyle:on
  override def beforeAll(): Unit = {
    System.setProperty(IS_TESTING.key, "true")
    doThreadPreAudit()
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
  final def withLogAppender(
      appender: AbstractAppender,
      loggerNames: Seq[String] = Seq.empty,
      level: Option[Level] = None)(
      f: => Unit): Unit = {
    val loggers =
      if (loggerNames.nonEmpty) {
        loggerNames.map(LogManager.getLogger)
      } else {
        Seq(LogManager.getRootLogger)
      }
    val restoreLevels = loggers.map(_.getLevel)
    loggers.foreach { l =>
      val logger = l.asInstanceOf[Logger]
      logger.addAppender(appender)
      appender.start()
      if (level.isDefined) {
        logger.setLevel(level.get)
        logger.get().setLevel(level.get)
        LogManager.getContext(false).asInstanceOf[LoggerContext].updateLoggers()
      }
    }
    try f
    finally {
      loggers.foreach(_.asInstanceOf[Logger].removeAppender(appender))
      appender.stop()
      if (level.isDefined) {
        loggers.zipWithIndex.foreach { case (logger, i) =>
          logger.asInstanceOf[Logger].setLevel(restoreLevels(i))
          logger.asInstanceOf[Logger].get().setLevel(restoreLevels(i))
        }
      }
    }
  }

  class LogAppender(msg: String = "", maxEvents: Int = 1000)
    extends AbstractAppender("logAppender", null, null, true, Property.EMPTY_ARRAY) {
    private val _loggingEvents = new ArrayBuffer[LogEvent]()
    private var _threshold: Level = Level.INFO

    override def append(loggingEvent: LogEvent): Unit = loggingEvent.synchronized {
      val copyEvent = loggingEvent.toImmutable
      if (copyEvent.getLevel.isMoreSpecificThan(_threshold)) {
        _loggingEvents.synchronized {
          if (_loggingEvents.size >= maxEvents) {
            val loggingInfo = if (msg == "") "." else s" while logging $msg."
            throw new IllegalStateException(
              s"Number of events reached the limit of $maxEvents$loggingInfo")
          }
          _loggingEvents.append(copyEvent)
        }
      }
    }

    def setThreshold(threshold: Level): Unit = {
      _threshold = threshold
    }

    def loggingEvents: ArrayBuffer[LogEvent] = _loggingEvents.synchronized {
      _loggingEvents.filterNot(_ == null)
    }
  }

  final def withSystemProperty(properties: Map[String, String])(f: => Unit): Unit = {
    val originValues = properties.map {
      case (key, value) =>
        val originValue = System.getProperty(value)
        setSystemProperty(key, value)
        (key, originValue)
    }
    try {
      f
    } finally {
      originValues.foreach {
        case (key, originValue) =>
          setSystemProperty(key, originValue)
      }
    }
  }

  final def withSystemProperty(key: String, value: String)(f: => Unit): Unit = {
    val originValue = System.getProperty(key)
    setSystemProperty(key, value)
    try {
      f
    } finally {
      setSystemProperty(key, originValue)
    }
  }

  final def setSystemProperty(key: String, value: String): Unit = {
    if (value == null) {
      System.clearProperty(key)
    } else {
      System.setProperty(key, value)
    }
  }
}
