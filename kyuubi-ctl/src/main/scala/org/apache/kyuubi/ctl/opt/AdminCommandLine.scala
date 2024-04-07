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

package org.apache.kyuubi.ctl.opt

import scopt.{OParser, OParserBuilder}

import org.apache.kyuubi.KYUUBI_VERSION
import org.apache.kyuubi.ctl.cmd.refresh.RefreshConfigCommandConfigType._

object AdminCommandLine extends CommonCommandLine {

  def getAdminCtlOptionParser(builder: OParserBuilder[CliConfig]): OParser[Unit, CliConfig] = {
    import builder._
    OParser.sequence(
      programName("kyuubi-admin"),
      head("kyuubi", KYUUBI_VERSION),
      common(builder),
      list(builder),
      delete(builder),
      refresh(builder),
      checkConfig(f => {
        if (f.action == null) {
          failure("Must specify action command: [list|delete|refresh].")
        } else {
          success
        }
      }),
      note(""),
      help('h', "help").text("Show help message and exit."))
  }

  private def delete(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    OParser.sequence(
      note(""),
      cmd("delete")
        .text("\tDelete resources.")
        .action((_, c) => c.copy(action = ControlAction.DELETE))
        .children(
          engineCmd(builder).text("\tDelete the specified engine node for user.")))

  }

  private def list(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    OParser.sequence(
      note(""),
      cmd("list")
        .text("\tList information about resources.")
        .action((_, c) => c.copy(action = ControlAction.LIST))
        .children(
          engineCmd(builder).text("\tList all the engine nodes for a user"),
          serverCmd(builder).text("\tList all the server nodes")))

  }

  private def refresh(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    OParser.sequence(
      note(""),
      cmd("refresh")
        .text("\tRefresh the resource.")
        .action((_, c) => c.copy(action = ControlAction.REFRESH))
        .children(
          refreshConfigCmd(builder).text("\tRefresh the config with specified type.")))
  }

  private def engineCmd(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    cmd("engine").action((_, c) => c.copy(resource = ControlObject.ENGINE))
      .children(
        opt[String]("engine-type").abbr("et")
          .action((v, c) => c.copy(engineOpts = c.engineOpts.copy(engineType = v)))
          .text("The engine type this engine belong to."),
        opt[String]("engine-subdomain").abbr("es")
          .action((v, c) => c.copy(engineOpts = c.engineOpts.copy(engineSubdomain = v)))
          .text("The engine subdomain this engine belong to."),
        opt[String]("engine-share-level").abbr("esl")
          .action((v, c) => c.copy(engineOpts = c.engineOpts.copy(engineShareLevel = v)))
          .text("The engine share level this engine belong to."))
  }

  private def serverCmd(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    cmd("server").action((_, c) => c.copy(resource = ControlObject.SERVER))
  }

  private def refreshConfigCmd(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    cmd("config").action((_, c) => c.copy(resource = ControlObject.CONFIG))
      .children(
        arg[String]("<configType>")
          .optional()
          .action((v, c) => c.copy(adminConfigOpts = c.adminConfigOpts.copy(configType = v)))
          .text("The valid config type can be one of the following: " +
            s"$HADOOP_CONF, $USER_DEFAULTS_CONF, $KUBERNETES_CONF, " +
            s"$UNLIMITED_USERS, $DENY_USERS, $DENY_IPS."))
  }
}
