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

import scopt.{DefaultOEffectSetup, OParser}

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf._

class ServiceControlCliArguments(args: Seq[String], env: Map[String, String] = sys.env)
  extends ServiceControlCliArgumentsParser with Logging {

  var cliArgs: CliArguments = null

  val conf = KyuubiConf().loadFileDefaults()

  // Set parameters from command line arguments
  parse(args)

  override def parser(): OParser[Unit, CliArguments] = {
    val builder = OParser.builder[CliArguments]
    import builder._

    // Options after action and service
    val serverOps = Array(
      opt[String]("zk-quorum").abbr("zk")
        .action((v, c) => c.copy(zkQuorum = v)),
      opt[String]('n', "namespace")
        .action((v, c) => c.copy(namespace = v)),
      opt[String]('s', "host")
        .action((v, c) => c.copy(host = v)),
      opt[String]('p', "port")
        .action((v, c) => c.copy(port = v)),
      opt[String]('v', "version")
        .action((v, c) => c.copy(version = v)),
      opt[Unit]('b', "verbose")
        .action((_, c) => c.copy(verbose = true)),
      opt[Unit]('h', "help")
        .action((_, c) => c.copy(action = ServiceControlAction.HELP)))

    // for engine service only
    val engineOps = serverOps :+
      opt[String]('u', "user")
        .action((v, c) => c.copy(user = v))

    val services = Array(
      cmd("server").action((_, c) => c.copy(service = ServiceControlObject.SERVER))
        .children(serverOps: _*),
      cmd("engine").action((_, c) => c.copy(service = ServiceControlObject.ENGINE))
        .children(engineOps: _*))

    val CtlParser = {
      OParser.sequence(
        programName("kyuubi-ctl"),
        head("kyuubi", "4.x"),
        cmd("create")
          .action((_, c) => c.copy(action = ServiceControlAction.CREATE))
          .children(services: _*)
          ++cmd("get")
          .action((_, c) => c.copy(action = ServiceControlAction.GET))
          .children(services: _*)
          ++cmd("delete")
          .action((_, c) => c.copy(action = ServiceControlAction.DELETE))
          .children(services: _*)
          ++cmd("list")
          .action((_, c) => c.copy(action = ServiceControlAction.LIST))
          .required()
          .children(services: _*),
        // Use custom help string instead scopt help() function
        opt[Unit]('h', "help")
          .action((_, c) => c.copy(action = ServiceControlAction.HELP))
      )
    }
    CtlParser
  }

  override def parse(args: Seq[String]): Unit = {
    OParser.runParser(parser(), args, CliArguments()) match {
      case (result, effects) =>
        OParser.runEffects(effects, new DefaultOEffectSetup {
          // Noting to display, use printUsageAndExit instead
          override def displayToOut(msg: String): Unit = Unit
          override def displayToErr(msg: String): Unit = Unit
          override def reportError(msg: String): Unit = info(msg)
          override def reportWarning(msg: String): Unit = warn(msg)

          // ignore terminate
          override def terminate(exitState: Either[String, Unit]): Unit = ()
        })
        result match {
          case Some(arguments) =>
            // Use default property value if not set
            cliArgs = useDefaultPropertyValueIfMissing(arguments).copy()

            // Validate arguments
            validateArguments()
          case _ =>
            // arguments are bad, print usage
            printUsageAndExit(-1)
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
      case ServiceControlAction.HELP =>
      case _ => printUsageAndExit(-1)
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

  private[ctl] def printUsageAndExit(exitCode: Int, unknownParam: Any = null): Unit = {
    if (unknownParam != null) {
      info(unknownParam)
    }
    val command =
      s"""
        |Kyuubi Ver $KYUUBI_VERSION.
        |Usage: kyuubi-ctl [create|get|delete|list]  [server|engine] --zk-quorum ...
        |--namespace ... --user ... --host ... --port ... --version""".stripMargin
    info(command)

    info(
      s"""
         |Command:
         |  - create                    Expose a service to a namespace on the zookeeper cluster of
         |                              zk quorum manually.
         |  - get                       Get the service node info.
         |  - delete                    Delete the specified serviceNode.
         |  - list                      List all the service nodes for a particular domain.
         |
         |Service:
         |  - server                    Default.
         |  - engine
         |
         |Options:
         |  -zk,--zk-quorum <value>     The connection string for the zookeeper ensemble, using
         |                              kyuubi-defaults/conf if absent.
         |  -n,--namespace <value>      The namespace, using kyuubi-defaults/conf if absent.
         |  -s,--host <value>           Hostname or IP address of a service.
         |  -p,--port <value>           Listening port of a service.
         |  -v,--version <value>        Using the compiled KYUUBI_VERSION default, change it if the
         |                              active service is running in another.
         |  -h,--help                   Show this help message and exit.
         |  -b,--verbose                Print additional debug output.
         |
         | Engine service only:
         |  -u,--user <value>           The user name this engine belong to.
      """.stripMargin
    )

    throw new ServiceControlCliException(exitCode)
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
