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

package org.apache.kyuubi.ctl

import scopt.OParser

import org.apache.kyuubi.Logging
import org.apache.kyuubi.ctl.cmd._

class ControlCliArguments(args: Seq[String], env: Map[String, String] = sys.env)
  extends ControlCliArgumentsParser with Logging {

  var cliArgs: CliConfig = null

  var command: Command = null

  // Set parameters from command line arguments
  parse(args)

  lazy val cliParser = parser()

  override def parser(): OParser[Unit, CliConfig] = {
    val builder = OParser.builder[CliConfig]
    CommandLine.getOptionParser(builder)
  }

  private[kyuubi] lazy val effectSetup = new KyuubiOEffectSetup

  override def parse(args: Seq[String]): Unit = {
    OParser.runParser(cliParser, args, CliConfig()) match {
      case (result, effects) =>
        OParser.runEffects(effects, effectSetup)
        result match {
          case Some(arguments) =>
            command = getCommand(arguments)
            command.preProcess()
            cliArgs = command.cliArgs
          case _ =>
          // arguments are bad, exit
        }
    }
  }

  private def getCommand(cliArgs: CliConfig): Command = {
    cliArgs.action match {
      case ServiceControlAction.CREATE => new CreateCommand(cliArgs)
      case ServiceControlAction.GET => new GetCommand(cliArgs)
      case ServiceControlAction.DELETE => new DeleteCommand(cliArgs)
      case ServiceControlAction.LIST => new ListCommand(cliArgs)
      case _ => null
    }
  }

  override def toString: String = {
    cliArgs.service match {
      case ServiceControlObject.SERVER =>
        s"""Parsed arguments:
           |  action                  ${cliArgs.action}
           |  service                 ${cliArgs.service}
           |  zkQuorum                ${cliArgs.commonOpts.zkQuorum}
           |  namespace               ${cliArgs.commonOpts.namespace}
           |  host                    ${cliArgs.commonOpts.host}
           |  port                    ${cliArgs.commonOpts.port}
           |  version                 ${cliArgs.commonOpts.version}
           |  verbose                 ${cliArgs.commonOpts.verbose}
        """.stripMargin
      case ServiceControlObject.ENGINE =>
        s"""Parsed arguments:
           |  action                  ${cliArgs.action}
           |  service                 ${cliArgs.service}
           |  zkQuorum                ${cliArgs.commonOpts.zkQuorum}
           |  namespace               ${cliArgs.commonOpts.namespace}
           |  user                    ${cliArgs.engineOpts.user}
           |  host                    ${cliArgs.commonOpts.host}
           |  port                    ${cliArgs.commonOpts.port}
           |  version                 ${cliArgs.commonOpts.version}
           |  verbose                 ${cliArgs.commonOpts.verbose}
        """.stripMargin
      case _ => ""
    }
  }
}
