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

import scopt.{OParser, OParserBuilder}

import org.apache.kyuubi.KYUUBI_VERSION

object CommandLine {

  def getOptionParser(builder: OParserBuilder[CliConfig]): OParser[Unit, CliConfig] = {
    import builder._
    OParser.sequence(
      programName("kyuubi-ctl"),
      head("kyuubi", KYUUBI_VERSION),
      common(builder),
      create(builder),
      get(builder),
      delete(builder),
      list(builder),
      checkConfig(f => {
        if (f.action == null) failure("Must specify action command: [create|get|delete|list].")
        else success
      }),
      note(""),
      help('h', "help").text("Show help message and exit."))
  }

  private def common(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    OParser.sequence(
      opt[String]("zk-quorum").abbr("zk")
        .action((v, c) => c.copy(commonOpts = c.commonOpts.copy(zkQuorum = v)))
        .text("The connection string for the zookeeper ensemble," +
          " using zk quorum manually."),
      opt[String]('n', "namespace")
        .action((v, c) => c.copy(commonOpts = c.commonOpts.copy(namespace = v)))
        .text("The namespace, using kyuubi-defaults/conf if absent."),
      opt[String]('s', "host")
        .action((v, c) => c.copy(commonOpts = c.commonOpts.copy(host = v)))
        .text("Hostname or IP address of a service."),
      opt[String]('p', "port")
        .action((v, c) => c.copy(commonOpts = c.commonOpts.copy(port = v)))
        .text("Listening port of a service."),
      opt[String]('v', "version")
        .action((v, c) => c.copy(commonOpts = c.commonOpts.copy(version = v)))
        .text("Using the compiled KYUUBI_VERSION default," +
          " change it if the active service is running in another."),
      opt[Unit]('b', "verbose")
        .action((_, c) => c.copy(commonOpts = c.commonOpts.copy(verbose = true)))
        .text("Print additional debug output."))
  }

  private def create(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    OParser.sequence(
      note(""),
      cmd("create")
        .action((_, c) => c.copy(action = ServiceControlAction.CREATE))
        .children(
          serverCmd(builder).text("\tExpose Kyuubi server instance to another domain.")))
  }

  private def get(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    OParser.sequence(
      note(""),
      cmd("get")
        .text("\tGet the service/engine node info, host and port needed.")
        .action((_, c) => c.copy(action = ServiceControlAction.GET))
        .children(
          serverCmd(builder).text("\tGet Kyuubi server info of domain"),
          engineCmd(builder).text("\tGet Kyuubi engine info belong to a user.")))

  }

  private def delete(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    OParser.sequence(
      note(""),
      cmd("delete")
        .text("\tDelete the specified service/engine node, host and port needed.")
        .action((_, c) => c.copy(action = ServiceControlAction.DELETE))
        .children(
          serverCmd(builder).text("\tDelete the specified service node for a domain"),
          engineCmd(builder).text("\tDelete the specified engine node for user.")))

  }

  private def list(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    OParser.sequence(
      note(""),
      cmd("list")
        .text("\tList all the service/engine nodes for a particular domain.")
        .action((_, c) => c.copy(action = ServiceControlAction.LIST))
        .children(
          serverCmd(builder).text("\tList all the service nodes for a particular domain"),
          engineCmd(builder).text("\tList all the engine nodes for a user")))

  }

  private def serverCmd(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    cmd("server").action((_, c) => c.copy(service = ServiceControlObject.SERVER))
  }

  private def engineCmd(builder: OParserBuilder[CliConfig]): OParser[_, CliConfig] = {
    import builder._
    cmd("engine").action((_, c) => c.copy(service = ServiceControlObject.ENGINE))
      .children(
        opt[String]('u', "user")
          .action((v, c) => c.copy(engineOpts = c.engineOpts.copy(user = v)))
          .text("The user name this engine belong to."),
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

}
