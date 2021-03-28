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

import java.util.{List => JList}

import scala.collection.JavaConverters._

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiException, Logging, Utils}
import org.apache.kyuubi.ctl.KyuubiCtlAction._
import org.apache.kyuubi.ctl.KyuubiCtlActionService._
import org.apache.kyuubi.ha.HighAvailabilityConf._

class KyuubiCtlArguments(args: Seq[String], env: Map[String, String] = sys.env)
  extends KyuubiCtlArgumentsParser with Logging {
  var action: KyuubiCtlAction = null
  var service: KyuubiCtlActionService = null
  var zkAddress: String = null
  var nameSpace: String = null
  var user: String = null
  var host: String = null
  var port: String = null
  var version: String = null
  var verbose: Boolean = false

  /** Default properties present in the currently defined default file. */
  lazy val defaultKyuubiProperties: Map[String, String] = {
    val maybeConfigFile = Utils.getDefaultPropertiesFile(env)
    if (verbose) {
      info(s"Using properties file: $maybeConfigFile")
    }
    val defaultProperties = Utils.getPropertiesFromFile(maybeConfigFile)
    if (verbose) {
      defaultProperties.foreach { case (k, v) =>
        info(s"Adding default property: $k=$v")
      }
    }
    defaultProperties
  }

  // Set parameters from command line arguments
  parse(args.asJava)

  // Use default property value if not set
  useDefaultPropertyValueIfMissing()

  validateArguments()

  if (verbose) {
    info(toString)
  }

  private def useDefaultPropertyValueIfMissing(): Unit = {
    if (zkAddress == null) {
      zkAddress = defaultKyuubiProperties.getOrElse(HA_ZK_QUORUM.key, null)
    }
    if (nameSpace == null) {
      nameSpace = defaultKyuubiProperties.getOrElse(HA_ZK_NAMESPACE.key, null)
    }
    if (version == null) {
      version = KYUUBI_VERSION
    }
  }
  /** Ensure that required fields exists. Call this only once all defaults are loaded. */
  private def validateArguments(): Unit = {
    action match {
      case KyuubiCtlAction.CREATE => validateCreateGetDeleteArguments()
      case KyuubiCtlAction.GET => validateCreateGetDeleteArguments()
      case KyuubiCtlAction.DELETE => validateCreateGetDeleteArguments()
      case KyuubiCtlAction.LIST => validateListArguments()
      case KyuubiCtlAction.HELP =>
    }
  }

  private def validateCreateGetDeleteArguments(): Unit = {
    if (zkAddress == null) {
      fail("Zookeeper address is not specified and no default value to load")
    }
    if (nameSpace == null) {
      fail("Zookeeper namespace is not specified and no default value to load")
    }
    if (version == null) {
      fail("Kyuubi version is not specified and could not found KYUUBI_VERSION in " +
        "kyuubi-build-info")
    }
    if (host == null) {
      fail("Must specify host for service")
    }
    if (port == null) {
      fail("Must specify port for service")
    }
    if (service == KyuubiCtlActionService.ENGINE && user == null) {
      fail("Must specify user name for engine")
    }
  }

  private def validateListArguments(): Unit = {
    if (zkAddress == null) {
      fail("Zookeeper address is not specified and no default value to load")
    }
    if (nameSpace == null) {
      fail("Zookeeper namespace is not specified and no default value to load")
    }
  }

  private def printUsageAndExit(exitCode: Int, unknownParam: Any = null): Unit = {
    if (unknownParam != null) {
      info("Unknown/unsupported param " + unknownParam)
    }
    val command = sys.env.getOrElse("_KYUUBI_CMD_USAGE",
      s"""
        |Kyuubi Ver $KYUUBI_VERSION.
        |Usage: kyuubi-ctl <create|get|delete|list>  <server|engine> --zkAddress ...
        |--namespace ... --user ... --host ... --port ... --version""".stripMargin)
    info(command)

    info(
      s"""
         |Command:
         |  - create                    expose a service to a namespace on the zookeeper cluster of
         |                              zkAddress manually
         |  - get                       get the service node info
         |  - delete                    delete the specified serviceNode
         |  - list                      list all the service nodes for a particular domain
         |
         |Service:
         |  - server                    default
         |  - engine
         |
         |Arguments:
         |  --zkAddress, -zk            one of the zk ensemble address, using kyuubi-defaults/conf
         |                              if absent
         |  --namespace, -ns            the namespace, using kyuubi-defaults/conf if absent
         |  --host, -h                  hostname or IP address of a service
         |  --port, -p                  listening port of a service
         |  --version, -V               using the compiled KYUUBI_VERSION default, change it if the
         |                              active service is running in another
         |  --user, -u                  for engine service only, the user name this engine belong to
         |  --help, -I                  Show this help message and exit.
         |  --verbose, -v               Print additional debug output.
      """.stripMargin
    )

    throw new KyuubiCtlException(exitCode)
  }

  override protected def parseActionAndService(args: JList[String]): Int = {
    if (args.isEmpty) {
      printUsageAndExit(-1)
    }

    args.get(0) match {
      case CREATE =>
        action = KyuubiCtlAction.CREATE
      case GET =>
        action = KyuubiCtlAction.GET
      case DELETE =>
        action = KyuubiCtlAction.DELETE
      case LIST =>
        action = KyuubiCtlAction.LIST
      case HELP =>
        action = KyuubiCtlAction.HELP
      case _ =>
        printUsageAndExit(-1, args.get(0))
    }

    if (args.size() == 1) {
      1
    } else {
      args.get(1) match {
        case SERVER =>
          service = KyuubiCtlActionService.SERVER
          2
        case ENGINE =>
          service = KyuubiCtlActionService.ENGINE
          2
        case _ =>
          service = KyuubiCtlActionService.SERVER
          1
      }
    }
  }

  override def toString: String = {
    s"""Parsed arguments:
       |  action                  $action
       |  service                 $service
       |  zkAddress               $zkAddress
       |  namespace               $nameSpace
       |  user                    $user
       |  host                    $host
       |  port                    $port
       |  version                 $version
       |  verbose                 $verbose
    """.stripMargin
  }

  /** Fill in values by parsing user options. */
  override protected def handle(opt: String, value: String): Boolean = {
    opt match {
      case ZK_ADDRESS =>
        zkAddress = value

      case NAMESPACE =>
        nameSpace = value

      case USER =>
        user = value

      case HOST =>
        host = value

      case PORT =>
        port = value

      case VERSION =>
        version = value

      case HELP =>
        action = KyuubiCtlAction.HELP

      case VERBOSE =>
        verbose = true

      case _ =>
        fail(s"Unexpected argument '$opt'.")
    }
    action != KyuubiCtlAction.HELP
  }

  override protected def handleUnknown(opt: String): Boolean = {
    printUsageAndExit(-1, opt)
    false
  }

  private def fail(msg: String): Unit = throw new KyuubiException(msg)
}
