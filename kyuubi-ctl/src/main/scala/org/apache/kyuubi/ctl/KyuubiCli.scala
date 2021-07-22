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

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap

import com.beust.jcommander.JCommander

import org.apache.kyuubi.Logging
import org.apache.kyuubi.ctl.ServiceType.ServiceType
import org.apache.kyuubi.ctl.commands.common.{AbstractCommand, UnixStyleUsage}
import org.apache.kyuubi.ctl.commands.engine._
import org.apache.kyuubi.ctl.commands.server._

private[ctl] object ServiceType extends Enumeration {
  type ServiceType = Value
  val SERVER, ENGINE = Value
}

class KyuubiCli extends Logging {

  private lazy val commands = initCommands()

  def initCommands(): HashMap[ServiceType, JCommander] = {
    // init engine commands
    val engineCommand = JCommander
      .newBuilder()
      .addCommand(new GetEngineCommand)
      .addCommand(new DeleteEngineCommand)
      .addCommand(new ListEngineCommand)
      .build()

    // init server commands
    val serverCommand = JCommander
      .newBuilder()
      .addCommand(new CreateServerCommand)
      .addCommand(new GetServerCommand)
      .addCommand(new DeleteServerCommand)
      .addCommand(new ListServerCommand)
      .build()

    // register commands
    HashMap(
      ServiceType.ENGINE -> engineCommand,
      ServiceType.SERVER -> serverCommand
    )
  }

  def usage(): Unit = {
    // scalastyle:off println
    println("Usage: kyuubi-ctl <server|engine> <create|get|delete|list> [options]")
    for ((service, commander) <- commands) {
      println(s"[ $service ]")
      for (cmd <- commander.getCommands.values().asScala) {
        cmd.setColumnSize(100)
        cmd.setUsageFormatter(new UnixStyleUsage(cmd))
        cmd.usage()
      }
      print(System.lineSeparator())
    }
    // scalastyle:off println
  }

  def run(service: ServiceType, args: Array[String]): Unit = {

    val serviceCommand = commands.getOrElse(service, throw new RuntimeException)

    serviceCommand.parse(args: _*)
    val parsedCommand = serviceCommand.getCommands.get(serviceCommand.getParsedCommand)
    parsedCommand.getObjects.asScala.find(_.isInstanceOf[AbstractCommand]).foreach(cmd => {
      cmd.asInstanceOf[AbstractCommand].run(parsedCommand)
    })

  }

}

object KyuubiCli extends Logging {

  def main(args: Array[String]): Unit = {

    val shell = new KyuubiCli
    if (args(0).equals("--help")) {
      shell.usage()
    } else {
      shell.run(ServiceType.withName(args(0).toUpperCase()), args.drop(1))
    }

  }

}

