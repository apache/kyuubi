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

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.{Logger => Log4jLogger, LoggerContext}
import org.apache.logging.log4j.core.config.DefaultConfiguration
import org.slf4j.{Logger, LoggerFactory}
import org.slf4j.bridge.SLF4JBridgeHandler

import org.apache.kyuubi.util.reflect.ReflectUtils

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

  def debug(message: => Any, t: Throwable): Unit = {
    if (logger.isDebugEnabled) {
      logger.debug(message.toString, t)
    }
  }

  def info(message: => Any): Unit = {
    if (logger.isInfoEnabled) {
      logger.info(message.toString)
    }
  }

  def info(message: => Any, t: Throwable): Unit = {
    if (logger.isInfoEnabled) {
      logger.info(message.toString, t)
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
          Logging.initializeLogging(isInterpreter, loggerName, logger)
        }
      }
    }
  }
}

object Logging {
  @volatile private var useDefault = false
  @volatile private var defaultRootLevel: String = _
  @volatile private var initialized = false
  val initLock = new Object()

  private[kyuubi] def isLog4j12: Boolean = {
    // This distinguishes the log4j 1.2 binding, currently
    // org.slf4j.impl.Log4jLoggerFactory, from the log4j 2.0 binding, currently
    // org.apache.logging.slf4j.Log4jLoggerFactory
    val binderClass = LoggerFactory.getILoggerFactory.getClass.getName
    "org.slf4j.impl.Log4jLoggerFactory".equals(
      binderClass) || "org.slf4j.impl.Reload4jLoggerFactory".equals(binderClass)
  }

  private[kyuubi] def isLog4j2: Boolean = {
    // This distinguishes the log4j 1.2 binding, currently
    // org.slf4j.impl.Log4jLoggerFactory, from the log4j 2.0 binding, currently
    // org.apache.logging.slf4j.Log4jLoggerFactory
    "org.apache.logging.slf4j.Log4jLoggerFactory"
      .equals(LoggerFactory.getILoggerFactory.getClass.getName)
  }

  /**
   * Return true if log4j2 is initialized by default configuration which has one
   * appender with error level. See `org.apache.logging.log4j.core.config.DefaultConfiguration`.
   */
  private def isLog4j2DefaultConfigured(): Boolean = {
    val rootLogger = LogManager.getRootLogger.asInstanceOf[Log4jLogger]
    // If Log4j 2 is used but is initialized by default configuration,
    // load a default properties file
    // (see org.apache.logging.log4j.core.config.DefaultConfiguration)
    rootLogger.getAppenders.isEmpty ||
    (rootLogger.getAppenders.size() == 1 &&
      rootLogger.getLevel == Level.ERROR &&
      LogManager.getContext.asInstanceOf[LoggerContext]
        .getConfiguration.isInstanceOf[DefaultConfiguration])
  }

  private def initializeLogging(
      isInterpreter: Boolean,
      loggerName: String,
      logger: => Logger): Unit = {
    if (ReflectUtils.isClassLoadable("org.slf4j.bridge.SLF4JBridgeHandler")) {
      // Handles configuring the JUL -> SLF4J bridge
      SLF4JBridgeHandler.removeHandlersForRootLogger()
      SLF4JBridgeHandler.install()
    }

    if (Logging.isLog4j2) {
      // scalastyle:off println
      if (Logging.isLog4j2DefaultConfigured()) {
        Logging.useDefault = true
        val defaultLogProps = "log4j2-defaults.xml"
        Option(Thread.currentThread().getContextClassLoader.getResource(defaultLogProps)) match {
          case Some(url) =>
            val context = LogManager.getContext(false).asInstanceOf[LoggerContext]
            context.setConfigLocation(url.toURI)
          case None =>
            System.err.println(s"Missing $defaultLogProps")
        }
      }

      val rootLogger = LogManager.getRootLogger
        .asInstanceOf[org.apache.logging.log4j.core.Logger]
      if (Logging.defaultRootLevel == null) {
        Logging.defaultRootLevel = rootLogger.getLevel.toString
      }

      if (isInterpreter) {
        // set kyuubi ctl log level, default ERROR
        val ctlLogger = LogManager.getLogger(loggerName)
          .asInstanceOf[org.apache.logging.log4j.core.Logger]
        val ctlLevel = Option(ctlLogger.getLevel).getOrElse(Level.ERROR)
        rootLogger.setLevel(ctlLevel)
      }
      // scalastyle:on println
    } else if (Logging.isLog4j12) {
      val log4j12Initialized =
        org.apache.log4j.LogManager.getRootLogger.getAllAppenders.hasMoreElements
      // scalastyle:off println
      if (!log4j12Initialized) {
        Logging.useDefault = true
        val defaultLogProps = "log4j-defaults.properties"
        Option(Thread.currentThread().getContextClassLoader.getResource(defaultLogProps)) match {
          case Some(url) =>
            org.apache.log4j.PropertyConfigurator.configure(url)

          case None =>
            System.err.println(s"Missing $defaultLogProps")
        }

        val rootLogger = org.apache.log4j.LogManager.getRootLogger
        if (Logging.defaultRootLevel == null) {
          Logging.defaultRootLevel = rootLogger.getLevel.toString
        }

        if (isInterpreter) {
          // set kyuubi ctl log level, default ERROR
          val ctlLogger = org.apache.log4j.LogManager.getLogger(loggerName)
          val ctlLevel = Option(ctlLogger.getLevel).getOrElse(org.apache.log4j.Level.ERROR)
          rootLogger.setLevel(ctlLevel)
        }
        // scalastyle:on println
      }
    }
    Logging.initialized = true

    // Force a call into slf4j to initialize it. Avoids this happening from multiple threads
    // and triggering this: http://mailman.qos.ch/pipermail/slf4j-dev/2010-April/002956.html
    logger
  }
}
