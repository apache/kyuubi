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

import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import org.slf4j.{Logger, LoggerFactory}
import org.slf4j.impl.StaticLoggerBinder

/**
 * Simple version of logging adopted from Apache Spark.
 */
trait Logging {

  @transient private var log_ : Logger = _

  // Method to get the logger name for this object
  protected def loggerName: String = {
    // Ignore anon$'s of super class and trailing $'s in the class names for Scala objects
    this.getClass.getName.split('$')(0)
  }

  // Method to get or create the logger for this object
  protected def logger: Logger = {
    if (log_ == null) {
      initializeLoggerIfNecessary(false)
      log_ = LoggerFactory.getLogger(loggerName)
    }
    log_
  }

  def debug(message: => Any): Unit = {
    if (logger.isDebugEnabled) {
      logger.debug(message.toString)
    }
  }

  def info(message: => Any): Unit = {
    if (logger.isInfoEnabled) {
      logger.info(message.toString)
    }
  }

  def warn(message: => Any): Unit = {
    if (logger.isWarnEnabled) {
      logger.warn(message.toString)
    }
  }

  def warn(message: => Any, t: Throwable): Unit = {
    if (logger.isWarnEnabled) {
      logger.warn(message.toString, t)
    }
  }

  def error(message: => Any, t: Throwable): Unit = {
    if (logger.isErrorEnabled) {
      logger.error(message.toString, t)
    }
  }

  def error(message: => Any): Unit = {
    if (logger.isErrorEnabled) {
      logger.error(message.toString)
    }
  }

  protected def initializeLoggerIfNecessary(isInterpreter: Boolean): Unit = {
    if (!Logging.initialized) {
      Logging.initLock.synchronized {
        if (!Logging.initialized) {
          initializeLogging(isInterpreter)
        }
      }
    }
  }

  private def initializeLogging(isInterpreter: Boolean): Unit = {
    if (Logging.isLog4j12) {
      val log4j12Initialized = LogManager.getRootLogger.getAllAppenders.hasMoreElements
      // scalastyle:off println
      if (!log4j12Initialized) {
        Logging.useDefault = true
        val defaultLogProps = "log4j-defaults.properties"
        Option(Thread.currentThread().getContextClassLoader.getResource(defaultLogProps)) match {
          case Some(url) =>
            PropertyConfigurator.configure(url)
          case None =>
            System.err.println(s"Missing $defaultLogProps")
        }
      }

      val rootLogger = LogManager.getRootLogger
      if (Logging.defaultRootLevel == null) {
        Logging.defaultRootLevel = rootLogger.getLevel
      }

      if (isInterpreter) {
        // set kyuubi ctl log level, default ERROR
        val ctlLogger = LogManager.getLogger(loggerName)
        val ctlLevel = Option(ctlLogger.getLevel()).getOrElse(Level.ERROR)
        rootLogger.setLevel(ctlLevel)
      }
      // scalastyle:on println
    }
    Logging.initialized = true

    // Force a call into slf4j to initialize it. Avoids this happening from multiple threads
    // and triggering this: http://mailman.qos.ch/pipermail/slf4j-dev/2010-April/002956.html
    logger
  }
}

object Logging {
  @volatile private var useDefault = false
  @volatile private var defaultRootLevel: Level = _
  @volatile private var initialized = false
  val initLock = new Object()
  private def isLog4j12: Boolean = {
    // This distinguishes the log4j 1.2 binding, currently
    // org.slf4j.impl.Log4jLoggerFactory, from the log4j 2.0 binding, currently
    // org.apache.logging.slf4j.Log4jLoggerFactory
    val binderClass = StaticLoggerBinder.getSingleton.getLoggerFactoryClassStr
    "org.slf4j.impl.Log4jLoggerFactory".equals(binderClass)
  }
}
