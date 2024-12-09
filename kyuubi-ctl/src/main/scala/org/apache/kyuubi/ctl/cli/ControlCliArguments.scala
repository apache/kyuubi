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

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.ctl.{opt, KyuubiOEffectSetup}
import org.apache.kyuubi.ctl.cmd._
import org.apache.kyuubi.ctl.cmd.create.{CreateBatchCommand, CreateServerCommand}
import org.apache.kyuubi.ctl.cmd.delete.{DeleteBatchCommand, DeleteEngineCommand, DeleteServerCommand}
import org.apache.kyuubi.ctl.cmd.get.{GetBatchCommand, GetEngineCommand, GetServerCommand}
import org.apache.kyuubi.ctl.cmd.list.{ListBatchCommand, ListEngineCommand, ListServerCommand, ListSessionCommand}
import org.apache.kyuubi.ctl.cmd.log.LogBatchCommand
import org.apache.kyuubi.ctl.cmd.submit.SubmitBatchCommand
import org.apache.kyuubi.ctl.opt.{CliConfig, CommandLine, ControlAction, ControlObject}

class ControlCliArguments(args: Seq[String], env: Map[String, String] = sys.env)
  extends ControlCliArgumentsParser with Logging {

  var cliConfig: CliConfig = _

  var command: Command[_] = _

  // Set parameters from command line arguments
  parse(args)

  lazy val cliParser = parser()

  override def parser(): OParser[Unit, CliConfig] = {
    val builder = OParser.builder[CliConfig]
    CommandLine.getCtlOptionParser(builder)
  }

  private[kyuubi] lazy val effectSetup = new KyuubiOEffectSetup

  override def parse(args: Seq[String]): Unit = {
    OParser.runParser(cliParser, args, opt.CliConfig()) match {
      case (result, effects) =>
        OParser.runEffects(effects, effectSetup)
        result match {
          case Some(arguments) =>
            command = getCommand(arguments)
            command.validate()
            cliConfig = command.normalizedCliConfig
          case _ =>
          // arguments are bad, exit
        }
    }
  }

  protected def getCommand(cliConfig: CliConfig): Command[_] = {
    cliConfig.action match {
      case ControlAction.CREATE => cliConfig.resource match {
          case ControlObject.BATCH => new CreateBatchCommand(cliConfig)
          case ControlObject.SERVER => new CreateServerCommand(cliConfig)
          case _ => throw new KyuubiException(s"Invalid resource: ${cliConfig.resource}")
        }
      case ControlAction.GET => cliConfig.resource match {
          case ControlObject.BATCH => new GetBatchCommand(cliConfig)
          case ControlObject.ENGINE => new GetEngineCommand(cliConfig)
          case ControlObject.SERVER => new GetServerCommand(cliConfig)
          case _ => throw new KyuubiException(s"Invalid resource: ${cliConfig.resource}")
        }
      case ControlAction.DELETE => cliConfig.resource match {
          case ControlObject.BATCH => new DeleteBatchCommand(cliConfig)
          case ControlObject.ENGINE => new DeleteEngineCommand(cliConfig)
          case ControlObject.SERVER => new DeleteServerCommand(cliConfig)
          case _ => throw new KyuubiException(s"Invalid resource: ${cliConfig.resource}")
        }
      case ControlAction.LIST => cliConfig.resource match {
          case ControlObject.BATCH => new ListBatchCommand(cliConfig)
          case ControlObject.ENGINE => new ListEngineCommand(cliConfig)
          case ControlObject.SERVER => new ListServerCommand(cliConfig)
          case ControlObject.SESSION => new ListSessionCommand(cliConfig)
          case _ => throw new KyuubiException(s"Invalid resource: ${cliConfig.resource}")
        }
      case ControlAction.LOG => cliConfig.resource match {
          case ControlObject.BATCH => new LogBatchCommand(cliConfig)
          case _ => throw new KyuubiException(s"Invalid resource: ${cliConfig.resource}")
        }
      case ControlAction.SUBMIT => cliConfig.resource match {
          case ControlObject.BATCH => new SubmitBatchCommand(cliConfig)
          case _ => throw new KyuubiException(s"Invalid resource: ${cliConfig.resource}")
        }
      case _ => throw new KyuubiException(s"Invalid operation: ${cliConfig.action}")
    }
  }

  override def toString: String = {
    cliConfig.resource match {
      case ControlObject.BATCH =>
        s"""Parsed arguments:
           |  action                  ${cliConfig.action}
           |  resource                ${cliConfig.resource}
           |  batchId                 ${cliConfig.batchOpts.batchId}
           |  batchType               ${cliConfig.batchOpts.batchType}
           |  batchUser               ${cliConfig.batchOpts.batchUser}
           |  batchState              ${cliConfig.batchOpts.batchState}
           |  batchName               ${cliConfig.batchOpts.batchName}
           |  createTime              ${cliConfig.batchOpts.createTime}
           |  endTime                 ${cliConfig.batchOpts.endTime}
           |  from                    ${cliConfig.batchOpts.from}
           |  size                    ${cliConfig.batchOpts.size}
           |  desc                    ${cliConfig.batchOpts.desc}
        """.stripMargin
      case ControlObject.SERVER =>
        s"""Parsed arguments:
           |  action                  ${cliConfig.action}
           |  resource                ${cliConfig.resource}
           |  zkQuorum                ${cliConfig.zkOpts.zkQuorum}
           |  namespace               ${cliConfig.zkOpts.namespace}
           |  host                    ${cliConfig.zkOpts.host}
           |  port                    ${cliConfig.zkOpts.port}
           |  version                 ${cliConfig.zkOpts.version}
           |  verbose                 ${cliConfig.commonOpts.verbose}
        """.stripMargin
      case ControlObject.ENGINE =>
        s"""Parsed arguments:
           |  action                  ${cliConfig.action}
           |  resource                ${cliConfig.resource}
           |  zkQuorum                ${cliConfig.zkOpts.zkQuorum}
           |  namespace               ${cliConfig.zkOpts.namespace}
           |  user                    ${cliConfig.engineOpts.user}
           |  host                    ${cliConfig.zkOpts.host}
           |  port                    ${cliConfig.zkOpts.port}
           |  version                 ${cliConfig.zkOpts.version}
           |  verbose                 ${cliConfig.commonOpts.verbose}
        """.stripMargin
      case _ => ""
    }
  }
}
