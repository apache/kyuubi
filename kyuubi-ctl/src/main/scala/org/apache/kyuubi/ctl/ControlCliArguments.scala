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

import scala.reflect.runtime.{universe => ru}

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
    val operationName = cliArgs.action.toString.toLowerCase
    val resourceName = cliArgs.resource.toString.toLowerCase

    val packageName = operationName
    val commandName = s"${operationName.capitalize}${resourceName.capitalize}"
    val className = s"org.apache.kyuubi.ctl.cmd.${packageName}.${commandName}Command"

    val mirror: ru.Mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol: ru.ClassSymbol = mirror.staticClass(className)

    val consMethodSymbol = classSymbol.primaryConstructor.asMethod

    val classMirror = mirror.reflectClass(classSymbol)
    val consMethodMirror = classMirror.reflectConstructor(consMethodSymbol)

    val result = consMethodMirror.apply(cliArgs).asInstanceOf[Command]
    result
  }

  override def toString: String = {
    cliArgs.resource match {
      case ControlObject.BATCH =>
        s"""Parsed arguments:
           |  action                  ${cliArgs.action}
           |  resource                ${cliArgs.resource}
           |  batchId                 ${cliArgs.batchOpts.batchId}
           |  batchType               ${cliArgs.batchOpts.batchType}
           |  batchUser               ${cliArgs.batchOpts.batchUser}
           |  batchState              ${cliArgs.batchOpts.batchState}
           |  createTime              ${cliArgs.batchOpts.createTime}
           |  endTime                 ${cliArgs.batchOpts.endTime}
           |  from                    ${cliArgs.batchOpts.from}
           |  size                    ${cliArgs.batchOpts.size}
        """.stripMargin
      case ControlObject.SERVER =>
        s"""Parsed arguments:
           |  action                  ${cliArgs.action}
           |  resource                ${cliArgs.resource}
           |  zkQuorum                ${cliArgs.commonOpts.zkQuorum}
           |  namespace               ${cliArgs.commonOpts.namespace}
           |  host                    ${cliArgs.commonOpts.host}
           |  port                    ${cliArgs.commonOpts.port}
           |  version                 ${cliArgs.commonOpts.version}
           |  verbose                 ${cliArgs.commonOpts.verbose}
        """.stripMargin
      case ControlObject.ENGINE =>
        s"""Parsed arguments:
           |  action                  ${cliArgs.action}
           |  resource                ${cliArgs.resource}
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
