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

package org.apache.kyuubi.ctl.cli

import org.apache.kyuubi.Logging
import org.apache.kyuubi.ctl.{ControlCliException, KyuubiOEffectSetup}
import org.apache.kyuubi.ctl.util.CommandLineUtils

/**
 * Main gateway of launching a Kyuubi Ctl action.
 */
private[kyuubi] class ControlCli extends Logging {

  def doAction(args: Array[String]): Unit = {
    // Initialize logging if it hasn't been done yet.
    // Set log level ERROR
    initializeLoggerIfNecessary(true)

    val ctlArgs = parseArguments(args)

    // when parse failed, exit
    if (ctlArgs.cliConfig == null) {
      sys.exit(1)
    }

    val verbose = ctlArgs.cliConfig.commonOpts.verbose
    if (verbose) {
      super.info(ctlArgs.toString)
    }

    ctlArgs.command.run()
  }

  protected def parseArguments(args: Array[String]): ControlCliArguments = {
    new ControlCliArguments(args)
  }

}

object ControlCli extends CommandLineUtils with Logging {
  override def main(args: Array[String]): Unit = {
    val ctl = new ControlCli() { self =>
      override protected def parseArguments(args: Array[String]): ControlCliArguments = {
        new ControlCliArguments(args) {
          override def info(msg: => Any): Unit = self.info(msg)
          override def warn(msg: => Any): Unit = self.warn(msg)
          override def error(msg: => Any): Unit = self.error(msg)

          override private[kyuubi] lazy val effectSetup = new KyuubiOEffectSetup {
            override def displayToOut(msg: String): Unit = self.info(msg)
            override def displayToErr(msg: String): Unit = self.error(msg)
            override def reportWarning(msg: String): Unit = self.warn(msg)
            override def reportError(msg: String): Unit = self.error(msg)
          }
        }
      }

      override def info(msg: => Any): Unit = printMessage(msg)
      override def warn(msg: => Any): Unit = printMessage(s"Warning: $msg")
      override def error(msg: => Any): Unit = printMessage(s"Error: $msg")

      override def doAction(args: Array[String]): Unit = {
        try {
          super.doAction(args)
          exitFn(0)
        } catch {
          case e: ControlCliException => exitFn(e.exitCode)
        }
      }
    }

    ctl.doAction(args)
  }

}
