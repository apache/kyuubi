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

import java.util.{NoSuchElementException, StringJoiner}

import scala.collection.JavaConverters._

import com.beust.jcommander.{DefaultUsageFormatter, JCommander, MissingCommandException, ParameterException}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.ctl.commands.common._
import org.apache.kyuubi.ctl.commands.common.ServiceType.ServiceType
import org.apache.kyuubi.ctl.commands.config.ConfigCommandGroup
import org.apache.kyuubi.ctl.commands.engine._
import org.apache.kyuubi.ctl.commands.server._

class KyuubiCli extends Logging {

  private lazy val commands = Map(
    ServiceType.engine -> new EngineCommandGroup,
    ServiceType.server -> new ServerCommandGroup,
    ServiceType.config -> new ConfigCommandGroup
  )

  def printAllCommandUsage(): Unit = {
    // scalastyle:off println
    val sj = new StringJoiner("|", "<", ">")
    ServiceType.values.foreach(v => sj.add(String.valueOf(v)))
    println(s"Usage: kyuubi-ctl ${sj.toString} [actions] [options] \n")
    var prefixIndent = 0
    for (st <- commands.keys) {
      val prefix = "  " + String.valueOf(st)
      if (prefix.length > prefixIndent) prefixIndent = prefix.length
    }

    for ((st, group) <- commands) {
      val prefix = "  " + String.valueOf(st)
      val info = prefix +
        DefaultUsageFormatter.s(prefixIndent - prefix.length) + "  " +
        group.desc()
      println(info)
    }

    print(System.lineSeparator())
    // scalastyle:off println
  }

  def printUsageOfJcommander(service: String, action: Option[String],
                             jcommander: JCommander): Unit = {
    // scalastyle:off println
    val act = if (action.isEmpty) "[actions]" else action
    println(s"Usage: kyuubi-ctl ${service} ${act} [options]")
    if (action.isEmpty) {
      for (cmd <- jcommander.getCommands.values().asScala) {
        cmd.setColumnSize(100)
        cmd.setUsageFormatter(new UnixStyleUsage(cmd))
        cmd.usage()
      }
    } else {
      jcommander.setUsageFormatter(new UnixStyleUsage(jcommander))
      jcommander.usage()
    }
    // scalastyle:off println
  }

  def run(service: ServiceType, args: Array[String]): Unit = {

    val serviceCommand = commands.getOrElse(service, throw new RuntimeException).cmd()

    try {
      serviceCommand.parse(args: _*)
    } catch {
      case t: MissingCommandException =>
        printUsageOfJcommander(String.valueOf(service), Option.empty, serviceCommand)
        return;
      case t: ParameterException =>
        val act = t.getJCommander.getParsedCommand
        val commander = t.getJCommander.getCommands.get(act)
        error(s"Parameter error for command: ${act}")
        printUsageOfJcommander(String.valueOf(service), Option(act), commander)
        return;
    }

    val parsedCommand = serviceCommand.getCommands.get(serviceCommand.getParsedCommand)
    parsedCommand.getObjects.asScala.find(_.isInstanceOf[Command]).foreach(cmd => {
      cmd.asInstanceOf[Command].run(parsedCommand)
    })

  }

}

object KyuubiCli extends Logging {

  def main(args: Array[String]): Unit = {
    val shell = new KyuubiCli

    if (args == null || args(0).equals("-h") || args(0).equals("--help") || args.length == 1) {
      shell.printAllCommandUsage()
    } else {
      try {
        shell.run(ServiceType.withName(args(0)), args.drop(1))
      } catch {
        case _: NoSuchElementException =>
          error(s"unknown service type: args(0)")
          shell.printAllCommandUsage()
      }
    }

  }

}

