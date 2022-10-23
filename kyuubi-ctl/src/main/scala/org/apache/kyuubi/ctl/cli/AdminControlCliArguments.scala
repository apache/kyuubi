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

import scopt.OParser

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.ctl.cmd.Command
import org.apache.kyuubi.ctl.cmd.delete.AdminDeleteEngineCommand
import org.apache.kyuubi.ctl.cmd.list.AdminListEngineCommand
import org.apache.kyuubi.ctl.cmd.refresh.RefreshConfigCommand
import org.apache.kyuubi.ctl.opt.{AdminCommandLine, CliConfig, ControlAction, ControlObject}

class AdminControlCliArguments(args: Seq[String], env: Map[String, String] = sys.env)
  extends ControlCliArguments(args, env) {
  override def parser(): OParser[Unit, CliConfig] = {
    val builder = OParser.builder[CliConfig]
    AdminCommandLine.getAdminCtlOptionParser(builder)
  }

  override protected def getCommand(cliConfig: CliConfig): Command[_] = {
    cliConfig.action match {
      case ControlAction.LIST => cliConfig.resource match {
          case ControlObject.ENGINE => new AdminListEngineCommand(cliConfig)
          case _ => throw new KyuubiException(s"Invalid resource: ${cliConfig.resource}")
        }
      case ControlAction.DELETE => cliConfig.resource match {
          case ControlObject.ENGINE => new AdminDeleteEngineCommand(cliConfig)
          case _ => throw new KyuubiException(s"Invalid resource: ${cliConfig.resource}")
        }
      case ControlAction.REFRESH => cliConfig.resource match {
          case ControlObject.CONFIG => new RefreshConfigCommand(cliConfig)
          case _ => throw new KyuubiException(s"Invalid resource: ${cliConfig.resource}")
        }
      case _ => throw new KyuubiException(s"Invalid operation: ${cliConfig.action}")
    }
  }

  override def toString: String = {
    cliConfig.resource match {
      case ControlObject.ENGINE =>
        s"""Parsed arguments:
           |  action                  ${cliConfig.action}
           |  resource                ${cliConfig.resource}
           |  type                    ${cliConfig.engineOpts.engineType}
           |  sharelevel              ${cliConfig.engineOpts.engineShareLevel}
           |  sharesubdomain          ${cliConfig.engineOpts.engineSubdomain}
        """.stripMargin
      case ControlObject.CONFIG =>
        s"""Parsed arguments:
           |  action                  ${cliConfig.action}
           |  resource                ${cliConfig.resource}
           |  configType              ${cliConfig.adminConfigOpts.configType}
        """.stripMargin
      case _ => ""
    }
  }
}
