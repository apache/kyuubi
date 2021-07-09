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

import java.net.InetAddress

import scopt.OParser

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf._

class ServiceControlCliArguments(args: Seq[String], env: Map[String, String] = sys.env)
  extends ServiceControlCliArgumentsParser with Logging {

  var cliArgs: CliArguments = null

  val conf = KyuubiConf().loadFileDefaults()

  // Set parameters from command line arguments
  parse(args)

  lazy val cliParser = parser()

  override def parser(): OParser[Unit, CliArguments] = {
    val builder = OParser.builder[CliArguments]
    import builder._

    // Options after action and service
    val ops = OParser.sequence(
      opt[String]("zk-quorum").abbr("zk")
        .action((v, c) => c.copy(zkQuorum = v))
        .text("The connection string for the zookeeper ensemble," +
          " using zk quorum manually."),
      opt[String]('n', "namespace")
        .action((v, c) => c.copy(namespace = v))
        .text("The namespace, using kyuubi-defaults/conf if absent."),
      opt[String]('s', "host")
        .action((v, c) => c.copy(host = v))
        .text("Hostname or IP address of a service."),
      opt[String]('p', "port")
        .action((v, c) => c.copy(port = v))
        .text("Listening port of a service."),
      opt[String]('v', "version")
        .action((v, c) => c.copy(version = v))
        .text("Using the compiled KYUUBI_VERSION default," +
          " change it if the active service is running in another."),
      opt[Unit]('b', "verbose")
        .action((_, c) => c.copy(verbose = true))
        .text("Print additional debug output."))

    // for engine service only
    val userOps = opt[String]('u', "user")
      .action((v, c) => c.copy(user = v))
      .text("The user name this engine belong to.")

    val serverCmd =
      cmd("server").action((_, c) => c.copy(service = ServiceControlObject.SERVER))
    val engineCmd =
      cmd("engine").action((_, c) => c.copy(service = ServiceControlObject.ENGINE))

    val CtlParser = {
      OParser.sequence(
        programName("kyuubi-ctl"),
        head("kyuubi", KYUUBI_VERSION),
        ops,
        note(""),
        cmd("create")
          .action((_, c) => c.copy(action = ServiceControlAction.CREATE))
          .children(
            serverCmd.text("\tExpose Kyuubi server instance to another domain.")),
        note(""),
        cmd("get")
          .action((_, c) => c.copy(action = ServiceControlAction.GET))
          .text("\tGet the service/engine node info, host and port needed.")
          .children(
            serverCmd.text("\tGet Kyuubi server info of domain"),
            engineCmd
              .children(userOps)
              .text("\tGet Kyuubi engine info belong to a user.")),
        note(""),
        cmd("delete")
          .action((_, c) => c.copy(action = ServiceControlAction.DELETE))
          .text("\tDelete the specified service/engine node, host and port needed.")
          .children(
            serverCmd.text("\tDelete the specified service node for a domain"),
            engineCmd
              .children(userOps)
              .text("\tDelete the specified engine node for user.")),
        note(""),
        cmd("list")
          .action((_, c) => c.copy(action = ServiceControlAction.LIST))
          .text("\tList all the service/engine nodes for a particular domain.")
          .children(
            serverCmd.text("\tList all the service nodes for a particular domain"),
            engineCmd
              .children(userOps)
              .text("\tList all the engine nodes for a user")),
        checkConfig(f => {
          if (f.action == null)  failure("Must specify action command: [create|get|delete|list].")
          else success
        }),
        note(""),
        help('h', "help").text("Show help message and exit.")
      )
    }
    CtlParser
  }

  private[kyuubi] lazy val effectSetup = new KyuubiOEffectSetup

  override def parse(args: Seq[String]): Unit = {
    OParser.runParser(cliParser, args, CliArguments()) match {
      case (result, effects) =>
        OParser.runEffects(effects, effectSetup)
        result match {
          case Some(arguments) =>
            // Use default property value if not set
            cliArgs = useDefaultPropertyValueIfMissing(arguments).copy()

            // Validate arguments
            validateArguments()
          case _ =>
            // arguments are bad, do nothing
        }
    }
  }

  private def useDefaultPropertyValueIfMissing(value: CliArguments): CliArguments = {
    var arguments: CliArguments = value.copy()
    if (value.zkQuorum == null) {
      conf.getOption(HA_ZK_QUORUM.key).foreach { v =>
        if (arguments.verbose) {
          super.info(s"Zookeeper quorum is not specified, use value from default conf:$v")
        }
        arguments = arguments.copy(zkQuorum = v)
      }
    }

    // for create action, it only expose Kyuubi service instance to another domain,
    // so we do not use namespace from default conf
    if (arguments.action != ServiceControlAction.CREATE && arguments.namespace == null) {
      arguments = arguments.copy(namespace = conf.get(HA_ZK_NAMESPACE))
      if (arguments.verbose) {
        super.info(s"Zookeeper namespace is not specified, use value from default conf:" +
          s"${arguments.namespace}")
      }
    }

    if (arguments.version == null) {
      if (arguments.verbose) {
        super.info(s"version is not specified, use built-in KYUUBI_VERSION:$KYUUBI_VERSION")
      }
      arguments = arguments.copy(version = KYUUBI_VERSION)
    }
    arguments
  }

  /** Ensure that required fields exists. Call this only once all defaults are loaded. */
  private def validateArguments(): Unit = {
    cliArgs.action match {
      case ServiceControlAction.CREATE => validateCreateActionArguments()
      case ServiceControlAction.GET => validateGetDeleteActionArguments()
      case ServiceControlAction.DELETE => validateGetDeleteActionArguments()
      case ServiceControlAction.LIST => validateListActionArguments()
      case _ => // do nothing
    }
  }

  private def validateCreateActionArguments(): Unit = {
    if (cliArgs.service != ServiceControlObject.SERVER) {
      fail("Only support expose Kyuubi server instance to another domain")
    }
    validateZkArguments()

    val defaultNamespace = conf.getOption(HA_ZK_NAMESPACE.key)
    if (defaultNamespace.isEmpty || defaultNamespace.get.equals(cliArgs.namespace)) {
      fail(
        s"""
           |Only support expose Kyuubi server instance to another domain, but the default
           |namespace is [$defaultNamespace] and specified namespace is [${cliArgs.namespace}]
        """.stripMargin)
    }
  }

  private def validateGetDeleteActionArguments(): Unit = {
    validateZkArguments()
    validateHostAndPort()
    validateUser()
    mergeArgsIntoKyuubiConf()
  }

  private def validateListActionArguments(): Unit = {
    validateZkArguments()
    cliArgs.service match {
      case ServiceControlObject.ENGINE => validateUser()
      case _ =>
    }
    mergeArgsIntoKyuubiConf()
  }

  private def mergeArgsIntoKyuubiConf(): Unit = {
    conf.set(HA_ZK_QUORUM.key, cliArgs.zkQuorum)
    conf.set(HA_ZK_NAMESPACE.key, cliArgs.namespace)
  }

  private def validateZkArguments(): Unit = {
    if (cliArgs.zkQuorum == null) {
      fail("Zookeeper quorum is not specified and no default value to load")
    }
    if (cliArgs.namespace == null) {
      if (cliArgs.action == ServiceControlAction.CREATE) {
        fail("Zookeeper namespace is not specified")
      } else {
        fail("Zookeeper namespace is not specified and no default value to load")
      }
    }
  }

  private def validateHostAndPort(): Unit = {
    if (cliArgs.host == null) {
      fail("Must specify host for service")
    }
    if (cliArgs.port == null) {
      fail("Must specify port for service")
    }

    try {
      cliArgs = cliArgs.copy(host = InetAddress.getByName(cliArgs.host).getCanonicalHostName)
    } catch {
      case _: Exception =>
        fail(s"Unknown host: ${cliArgs.host}")
    }

    try {
      if (cliArgs.port.toInt <= 0 ) {
        fail(s"Specified port should be a positive number")
      }
    } catch {
      case _: NumberFormatException =>
        fail(s"Specified port is not a valid integer number: ${cliArgs.port}")
    }
  }

  private def validateUser(): Unit = {
    if (cliArgs.service == ServiceControlObject.ENGINE && cliArgs.user == null) {
      fail("Must specify user name for engine, please use -u or --user.")
    }
  }

  override def toString: String = {
    cliArgs.service match {
      case ServiceControlObject.SERVER =>
        s"""Parsed arguments:
          |  action                  ${cliArgs.action}
          |  service                 ${cliArgs.service}
          |  zkQuorum                ${cliArgs.zkQuorum}
          |  namespace               ${cliArgs.namespace}
          |  host                    ${cliArgs.host}
          |  port                    ${cliArgs.port}
          |  version                 ${cliArgs.version}
          |  verbose                 ${cliArgs.verbose}
        """.stripMargin
      case ServiceControlObject.ENGINE =>
        s"""Parsed arguments:
           |  action                  ${cliArgs.action}
           |  service                 ${cliArgs.service}
           |  zkQuorum                ${cliArgs.zkQuorum}
           |  namespace               ${cliArgs.namespace}
           |  user                    ${cliArgs.user}
           |  host                    ${cliArgs.host}
           |  port                    ${cliArgs.port}
           |  version                 ${cliArgs.version}
           |  verbose                 ${cliArgs.verbose}
        """.stripMargin
      case _ => ""
    }
  }

  private def fail(msg: String): Unit = throw new KyuubiException(msg)
}
